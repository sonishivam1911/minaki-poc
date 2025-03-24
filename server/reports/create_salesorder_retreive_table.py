import asyncio
import pandas as pd
from utils.zakya_api import fetch_records_from_zakya, fetch_object_for_each_id
from utils.postgres_connector import crud
from config.logger import logger
from core.helper_zakya import extract_record_list

def fetch_all_salesorder_and_mapping_records_from_database(config):
    """
    Retrieve existing mappings and all sales orders, identify orders needing processing.
    
    Args:
        config (dict): Dictionary with API credentials and settings
        
    Returns:
        tuple: (new_orders_df, existing_mappings_df)
    """
    try:
        # Step 1: Get existing mappings from database
        logger.debug("Fetching existing mappings from database")
        try:
            existing_mappings_df = crud.read_table('salesorder_line_item_mapping')
            logger.debug(f"Found {len(existing_mappings_df)} existing sales order mappings")
        except Exception as db_error:
            logger.debug(f"Error reading existing mappings: {str(db_error)}")
            existing_mappings_df = pd.DataFrame()
        
        # Get list of salesorder IDs that already have mappings
        if not existing_mappings_df.empty and 'salesorder_id' in existing_mappings_df.columns:
            existing_salesorder_ids = existing_mappings_df['salesorder_id'].unique().tolist()
            logger.debug(f"Found {len(existing_salesorder_ids)} unique existing salesorder IDs")
        else:
            existing_salesorder_ids = []
            logger.debug("No existing salesorder IDs found")
        
        # Step 2: Fetch all sales orders from Zakya
        logger.debug("Fetching all sales orders from Zakya API")
        try:
            sales_orders_data = fetch_records_from_zakya(
                config['api_domain'],
                config['access_token'],
                config['organization_id'],
                '/salesorders'
            )
            
            all_orders = extract_record_list(sales_orders_data, "salesorders")
            all_orders_df = pd.DataFrame(all_orders)
            logger.debug(f"Found {len(all_orders_df)} total sales orders in Zakya")
            
        except Exception as api_error:
            logger.debug(f"Error fetching sales orders from API: {str(api_error)}")
            raise
        
        # Step 3: Identify sales orders that need mapping
        if all_orders_df.empty:
            logger.debug("No sales orders found in Zakya")
            return pd.DataFrame(), existing_mappings_df
            
        if not existing_salesorder_ids:
            logger.debug("All sales orders need processing (no existing mappings)")
            return all_orders_df, existing_mappings_df
            
        new_orders_df = all_orders_df[~all_orders_df['salesorder_id'].isin(existing_salesorder_ids)]
        logger.debug(f"Found {len(new_orders_df)} sales orders that need mapping")
        
        return new_orders_df, existing_mappings_df
        
    except Exception as e:
        logger.debug(f"Error in fetch_all_salesorder_and_mapping_records_from_database: {str(e)}")
        return pd.DataFrame(), pd.DataFrame()

async def fetch_missing_salesorder_details(new_orders_df, config):
    """
    Retrieve detailed information for each sales order requiring mapping.
    
    Args:
        new_orders_df (DataFrame): DataFrame of orders needing details
        config (dict): Dictionary with API credentials and settings
        
    Returns:
        tuple: (line_item_mapping_data, invoice_mapping_data)
    """
    try:
        # Convert to list of dictionaries for processing
        new_orders_records = new_orders_df.to_dict('records')
        
        # Set batch size for processing
        batch_size = config.get('batch_size', 3)
        
        # Create a semaphore to limit concurrency
        semaphore = asyncio.Semaphore(batch_size)
        
        # Create containers for results
        line_item_mapping_data = []
        invoice_mapping_data = []
        
        # Process orders in batches
        total_batches = (len(new_orders_records) + batch_size - 1) // batch_size
        
        for i in range(0, len(new_orders_records), batch_size):
            batch = new_orders_records[i:i+batch_size]
            
            batch_num = i // batch_size + 1
            logger.debug(f"Processing batch {batch_num} of {total_batches} ({len(batch)} orders)")
            
            # Create tasks for each sales order in the batch
            tasks = []
            for order in batch:
                order_id = order.get('salesorder_id')
                if not order_id:
                    logger.debug("Skipping order with missing ID")
                    continue
                    
                # Define coroutine to fetch with semaphore
                async def fetch_with_semaphore(order_id):
                    async with semaphore:
                        logger.debug(f"Fetching details for order {order_id}")
                        try:
                            return await asyncio.to_thread(
                                fetch_object_for_each_id,
                                config['api_domain'],
                                config['access_token'],
                                config['organization_id'],
                                f'salesorders/{order_id}'
                            )
                        except Exception as e:
                            logger.debug(f"Error fetching order {order_id}: {str(e)}")
                            return None
                
                tasks.append((order_id, asyncio.create_task(fetch_with_semaphore(order_id))))
            
            # Wait for all tasks in this batch to complete
            for order_id, task in tasks:
                try:
                    details = await task
                    if not details:
                        logger.debug(f"No details returned for order {order_id}")
                        continue
                    
                    # Process the main sales order data
                    order_data = {}
                    if 'sales_order' in details:
                        order_data = details['sales_order']
                    elif 'salesorder' in details:
                        order_data = details['salesorder']
                    else:
                        logger.debug(f"Unexpected response format for order {order_id}")
                        continue
                    
                    # Extract line items
                    line_items = []
                    if 'line_items' in order_data:
                        line_items = order_data['line_items']
                    else:
                        logger.debug(f"No line items found in order {order_id}")
                        continue
                    
                    # Extract invoice data if present
                    if 'invoices' in order_data:
                        inv_mappings = extract_invoice_mapping_data(order_data, order_id)
                        invoice_mapping_data.extend(inv_mappings)
                    
                    # Extract financial details
                    financial_details = extract_financial_details(order_data)
                    
                    # Extract custom fields (store as-is)
                    custom_fields = handle_custom_fields(order_data)
                    
                    # Process each line item
                    for line_item in line_items:
                        mapping_record = {
                            # Sales order fields
                            'salesorder_id': order_id,
                            'salesorder_number': order_data.get('salesorder_number', ''),
                            'date': order_data.get('date', ''),
                            'reference_number': order_data.get('reference_number', ''),
                            'customer_id': order_data.get('customer_id', ''),
                            
                            # Line item fields
                            'line_item_id': line_item.get('line_item_id', ''),
                            'item_id': line_item.get('item_id', ''),
                            'sku': line_item.get('sku', ''),
                            'vendor_code': line_item.get('vendor_code', ''),
                            'name': line_item.get('name', ''),
                            
                            # Quantity fields
                            'quantity': line_item.get('quantity', 0),
                            'quantity_invoiced': line_item.get('quantity_invoiced', 0),
                            'quantity_packed': line_item.get('quantity_packed', 0),
                            'quantity_shipped': line_item.get('quantity_shipped', 0),
                            'quantity_picked': line_item.get('quantity_picked', 0),
                            'quantity_backordered': line_item.get('quantity_backordered', 0),
                            'quantity_dropshipped': line_item.get('quantity_dropshipped', 0),
                            'quantity_cancelled': line_item.get('quantity_cancelled', 0),
                            'quantity_delivered': line_item.get('quantity_delivered', 0),
                            'quantity_invoiced_cancelled': line_item.get('quantity_invoiced_cancelled', 0),
                            'quantity_returned': line_item.get('quantity_returned', 0),
                            
                            # Price fields
                            'rate': line_item.get('rate', 0),
                            'bcy_rate': line_item.get('bcy_rate', 0),
                            
                            # Tax fields
                            'tax_id': line_item.get('tax_id', ''),
                            'tax_name': line_item.get('tax_name', ''),
                            'tax_amount': line_item.get('tax_amount', 0),
                            'tax_percentage': line_item.get('tax_percentage', 0),
                            'tax_specific_type': line_item.get('tax_specific_type', ''),
                            'hsn_or_sac': line_item.get('hsn_or_sac', ''),
                            
                            # Financial fields from order level
                            'discount_amount': financial_details.get('discount_amount', 0),
                            'adjustment': financial_details.get('adjustment', 0),
                            'sub_total': financial_details.get('sub_total', 0),
                            'bcy_sub_total': financial_details.get('bcy_sub_total', 0),
                            'sub_total_inclusive_of_tax': financial_details.get('sub_total_inclusive_of_tax', 0),
                            'sub_total_exclusive_of_discount': financial_details.get('sub_total_exclusive_of_discount', 0),
                            'discount_total': financial_details.get('discount_total', 0),
                            'bcy_discount_total': financial_details.get('bcy_discount_total', 0),
                            'discount_percent': financial_details.get('discount_percent', 0),
                            'tax_total': financial_details.get('tax_total', 0),
                            'bcy_tax_total': financial_details.get('bcy_tax_total', 0),
                            'total': financial_details.get('total', 0),
                            'bcy_total': financial_details.get('bcy_total', 0),
                            
                            # Custom fields as JSON array
                            'custom_fields': custom_fields
                        }
                        line_item_mapping_data.append(mapping_record)
                        
                except Exception as e:
                    logger.debug(f"Error processing order {order_id}: {str(e)}")
                    continue
        
        logger.debug(f"Completed processing {len(new_orders_records)} orders")
        logger.debug(f"Generated {len(line_item_mapping_data)} line item mappings")
        logger.debug(f"Generated {len(invoice_mapping_data)} invoice mappings")
        
        return line_item_mapping_data, invoice_mapping_data
        
    except Exception as e:
        logger.debug(f"Error in fetch_missing_salesorder_details: {str(e)}")
        return [], []

def extract_invoice_mapping_data(sales_order_details, order_id):
    """
    Extract invoice information from sales order details.
    
    Args:
        sales_order_details (dict): Detailed sales order response
        order_id (str): ID of the sales order
        
    Returns:
        list: List of invoice mapping records
    """
    invoice_mapping_data = []
    
    # Check if invoices array exists
    if 'invoices' not in sales_order_details:
        logger.debug(f"No invoices found for order {order_id}")
        return invoice_mapping_data
        
    invoices = sales_order_details['invoices']
    logger.debug(f"Found {len(invoices)} invoices for order {order_id}")
    
    # Extract mapping data for each invoice
    for invoice in invoices:
        try:
            mapping_record = {
                'salesorder_id': order_id,
                'salesorder_number': sales_order_details.get('salesorder_number', ''),
                'invoice_id': invoice.get('invoice_id', ''),
                'invoice_number': invoice.get('invoice_number', ''),
                'status': invoice.get('status', ''),
                'date': invoice.get('date', ''),
                'due_date': invoice.get('due_date', ''),
                'total': invoice.get('total', 0),
                'balance': invoice.get('balance', 0)
            }
            invoice_mapping_data.append(mapping_record)
        except Exception as e:
            logger.debug(f"Error extracting invoice data: {str(e)}")
            continue
            
    return invoice_mapping_data

def handle_custom_fields(sales_order_details):
    """
    Process custom fields from sales order.
    
    Args:
        sales_order_details (dict): Sales order details from API
        
    Returns:
        list: Custom fields array (unmodified)
    """
    if 'custom_fields_hashed' not in sales_order_details:
        return None
        
    custom_fields = sales_order_details['custom_fields_hashed']
    
    # Return the array as-is
    return custom_fields

def extract_financial_details(sales_order_details):
    """
    Extract financial information from sales order.
    
    Args:
        sales_order_details (dict): Sales order details from API
        
    Returns:
        dict: Dictionary of financial details
    """
    financial_details = {
        'discount_amount': sales_order_details.get('discount_amount', 0),
        'adjustment': sales_order_details.get('adjustment', 0),
        'sub_total': sales_order_details.get('sub_total', 0),
        'bcy_sub_total': sales_order_details.get('bcy_sub_total', 0),
        'sub_total_inclusive_of_tax': sales_order_details.get('sub_total_inclusive_of_tax', 0),
        'sub_total_exclusive_of_discount': sales_order_details.get('sub_total_exclusive_of_discount', 0),
        'discount_total': sales_order_details.get('discount_total', 0),
        'bcy_discount_total': sales_order_details.get('bcy_discount_total', 0),
        'discount_percent': sales_order_details.get('discount_percent', 0),
        'tax_total': sales_order_details.get('tax_total', 0),
        'bcy_tax_total': sales_order_details.get('bcy_tax_total', 0),
        'total': sales_order_details.get('total', 0),
        'bcy_total': sales_order_details.get('bcy_total', 0),
    }
        
    return financial_details

def save_new_mappings_to_database(line_item_mapping_data, existing_mappings_df):
    """
    Save newly created line item mapping records to database.
    
    Args:
        line_item_mapping_data (list): List of new mapping records
        existing_mappings_df (DataFrame): DataFrame of existing mappings
        
    Returns:
        DataFrame: Combined DataFrame with all mappings
    """
    if not line_item_mapping_data:
        logger.debug("No new line item mappings to save")
        return existing_mappings_df
        
    # Convert to DataFrame
    new_mappings_df = pd.DataFrame.from_records(line_item_mapping_data)
    logger.debug(f"Saving {len(new_mappings_df)} new line item mappings")
    
    # If no existing mappings, create a new table
    if existing_mappings_df.empty:
        logger.debug("Creating new line item mapping table")
        crud.create_table('salesorder_line_item_mapping', new_mappings_df)
    else:
        # Append new mappings to existing table
        logger.debug("Appending to existing line item mapping table")
        all_mappings_df = pd.concat([existing_mappings_df, new_mappings_df], ignore_index=True)
        crud.create_table('salesorder_line_item_mapping', all_mappings_df)
    
    logger.debug(f"Successfully saved {len(new_mappings_df)} line item mappings")
    
    # Return combined mappings
    return pd.concat([existing_mappings_df, new_mappings_df], ignore_index=True)

def save_invoice_mappings_to_database(invoice_mapping_data):
    """
    Save invoice-to-sales order mappings to database.
    
    Args:
        invoice_mapping_data (list): List of invoice mapping records
        
    Returns:
        DataFrame: DataFrame of all invoice mappings
    """
    if not invoice_mapping_data:
        logger.debug("No invoice mappings to save")
        return pd.DataFrame()
        
    # Convert to DataFrame
    invoice_mappings_df = pd.DataFrame.from_records(invoice_mapping_data)
    logger.debug(f"Saving {len(invoice_mappings_df)} invoice mappings")
    
    # Check if mapping table exists
    try:
        existing_invoice_mappings = crud.read_table('zakya_salesorder_invoice_mapping')
        logger.debug(f"Found {len(existing_invoice_mappings)} existing invoice mappings")
        
        # Combine with new mappings
        all_invoice_mappings = pd.concat([existing_invoice_mappings, invoice_mappings_df], ignore_index=True)
        
        # Remove duplicates based on salesorder_id and invoice_id
        all_invoice_mappings = all_invoice_mappings.drop_duplicates(
            subset=['salesorder_id', 'invoice_id'], 
            keep='last'
        )
        
        # Save combined mappings
        crud.create_table('zakya_salesorder_invoice_mapping', all_invoice_mappings)
        logger.debug(f"Saved {len(all_invoice_mappings)} total invoice mappings")
        
        return all_invoice_mappings
        
    except Exception:
        # Table doesn't exist, create new
        logger.debug("Creating new invoice mapping table")
        crud.create_table('zakya_salesorder_invoice_mapping', invoice_mappings_df)
        logger.debug(f"Created table with {len(invoice_mappings_df)} invoice mappings")
        
        return invoice_mappings_df

async def sync_salesorder_mappings(config):
    """
    Main orchestration function that runs the entire process.
    
    Args:
        config (dict): Dictionary with API credentials and settings
        
    Returns:
        tuple: (line_item_mappings_df, invoice_mappings_df)
    """
    logger.debug("Starting sales order synchronization")
    
    # Step 1: Get existing mappings and identify orders needing processing
    new_orders_df, existing_mappings_df = fetch_all_salesorder_and_mapping_records_from_database(config)
    
    # Step 2: Process new sales orders to create mappings
    if not new_orders_df.empty:
        logger.debug(f"Processing {len(new_orders_df)} new sales orders")
        
        # Fetch detailed information for new orders
        line_item_mapping_data, invoice_mapping_data = await fetch_missing_salesorder_details(
            new_orders_df, 
            config
        )
        
        # Save line item mappings
        if line_item_mapping_data:
            all_line_item_mappings = save_new_mappings_to_database(
                line_item_mapping_data,
                existing_mappings_df
            )
        else:
            all_line_item_mappings = existing_mappings_df
            
        # Save invoice mappings
        if invoice_mapping_data:
            all_invoice_mappings = save_invoice_mappings_to_database(
                invoice_mapping_data
            )
        else:
            all_invoice_mappings = pd.DataFrame()
            
        logger.debug("Completed sales order synchronization")
        return all_line_item_mappings, all_invoice_mappings
    else:
        logger.debug("No new sales orders to process")
        return existing_mappings_df, pd.DataFrame()

def sync_salesorder_mappings_sync(config):
    """
    Wrapper function to run async sync_salesorder_mappings function.
    
    Args:
        config (dict): Configuration dictionary with API credentials
        
    Returns:
        tuple: Results from async function (line_item_mappings, invoice_mappings)
    """
    # Run the async function
    return asyncio.run(sync_salesorder_mappings(config))