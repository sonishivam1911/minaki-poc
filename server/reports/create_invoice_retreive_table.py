import asyncio
import pandas as pd
from utils.zakya_api import fetch_records_from_zakya, fetch_object_for_each_id
from utils.postgres_connector import crud
from config.logger import logger
from core.helper_zakya import extract_record_list



def fetch_all_invoices_from_zakya(config):
    """
    Fetch all invoices from Zakya API.
    
    Args:
        config (dict): Dictionary with API credentials and settings
        
    Returns:
        DataFrame: DataFrame containing all invoices
    """
    try:
        # Fetch all invoices from Zakya
        logger.debug("Fetching all invoices from Zakya API")
        
        invoices_data = fetch_records_from_zakya(
            config['api_domain'],
            config['access_token'],
            config['organization_id'],
            '/invoices'
        )
        
        all_invoices = extract_record_list(invoices_data, "invoices")
        all_invoices_df = pd.DataFrame(all_invoices)
        
        logger.debug(f"Found {len(all_invoices_df)} total invoices in Zakya")
        return all_invoices_df
        
    except Exception as e:
        logger.debug(f"Error fetching invoices from API: {str(e)}")
        return pd.DataFrame()

def identify_new_invoices(all_invoices_df, config):
    """
    Identify invoices that have not been processed yet.
    
    Args:
        all_invoices_df (DataFrame): DataFrame of all invoices
        config (dict): Dictionary with configuration settings
        
    Returns:
        DataFrame: DataFrame of invoices that need processing
    """
    try:
        # Check if we already have invoices in our database
        try:
            existing_invoice_ids = []
            # existing_invoices = crud.read_table('invoice_line_item_mapping')
            # existing_invoice_ids = existing_invoices['invoice_id'].unique().tolist()
            logger.debug(f"Found {len(existing_invoice_ids)} existing invoices in database")
        except Exception:
            logger.debug("No existing invoice mapping table found")
            existing_invoice_ids = []

        # Identify new invoices
        if all_invoices_df.empty:
            logger.debug("No invoices found in Zakya")
            return pd.DataFrame()
            
        if not existing_invoice_ids:
            logger.debug("All invoices need processing (no existing mappings)")
            return all_invoices_df
            
        new_invoices_df = all_invoices_df[~all_invoices_df['invoice_id'].isin(existing_invoice_ids)]
        logger.debug(f"Found {len(new_invoices_df)} invoices that need processing")
        
        return new_invoices_df
        
    except Exception as e:
        logger.debug(f"Error in identify_new_invoices: {str(e)}")
        return pd.DataFrame()

async def fetch_invoice_details(new_invoices_df, config):
    """
    Fetch detailed information for each invoice requiring processing.
    
    Args:
        new_invoices_df (DataFrame): DataFrame of invoices needing details
        config (dict): Dictionary with API credentials and settings
        
    Returns:
        list: List of detailed invoice data
    """
    try:
        # Convert to list of dictionaries for processing
        new_invoice_records = new_invoices_df.to_dict('records')
        
        # Set batch size for processing
        batch_size = config.get('batch_size', 3)
        
        # Create a semaphore to limit concurrency
        semaphore = asyncio.Semaphore(batch_size)
        
        # Create container for results
        detailed_invoices = []
        
        # Process invoices in batches
        total_batches = (len(new_invoice_records) + batch_size - 1) // batch_size
        
        for i in range(0, len(new_invoice_records), batch_size):
            batch = new_invoice_records[i:i+batch_size]
            
            batch_num = i // batch_size + 1
            logger.debug(f"Processing batch {batch_num} of {total_batches} ({len(batch)} invoices)")
            
            # Create tasks for each invoice in the batch
            tasks = []
            for invoice in batch:
                invoice_id = invoice.get('invoice_id')
                if not invoice_id:
                    logger.debug("Skipping invoice with missing ID")
                    continue
                    
                # Define coroutine to fetch with semaphore
                async def fetch_with_semaphore(invoice_id):
                    async with semaphore:
                        logger.debug(f"Fetching details for invoice {invoice_id}")
                        try:
                            return await asyncio.to_thread(
                                fetch_object_for_each_id,
                                config['api_domain'],
                                config['access_token'],
                                config['organization_id'],
                                f'invoices/{invoice_id}'
                            )
                        except Exception as e:
                            logger.debug(f"Error fetching invoice {invoice_id}: {str(e)}")
                            return None
                
                tasks.append((invoice_id, asyncio.create_task(fetch_with_semaphore(invoice_id))))
            
            # Wait for all tasks in this batch to complete
            for invoice_id, task in tasks:
                try:
                    details = await task
                    if not details:
                        logger.debug(f"No details returned for invoice {invoice_id}")
                        continue
                    
                    # Add the invoice details to our results
                    detailed_invoices.append(details)
                    
                except Exception as e:
                    logger.debug(f"Error processing invoice {invoice_id}: {str(e)}")
                    continue
        
        logger.debug(f"Completed processing {len(new_invoice_records)} invoices")
        return detailed_invoices
        
    except Exception as e:
        logger.debug(f"Error in fetch_invoice_details: {str(e)}")
        return []

def extract_flattened_invoice_data(detailed_invoices):
    """
    Extract and flatten invoice data with line items.
    
    Args:
        detailed_invoices (list): List of detailed invoice data from API
        
    Returns:
        tuple: (invoice_mapping_data, invoice_line_item_mapping_data)
    """
    invoice_line_item_mapping_data = []
    
    for invoice_response in detailed_invoices:
        try:
            # Get the invoice data
            if 'invoice' not in invoice_response:
                logger.debug("Unexpected response format, missing invoice data")
                continue
                
            invoice_data = invoice_response['invoice']
            invoice_id = invoice_data.get('invoice_id')
            
            if not invoice_id:
                logger.debug("Invoice data missing ID, skipping")
                continue
                
            # Process line items if available
            line_items = invoice_data.get('line_items', [])
            
            if not line_items:
                logger.debug(f"No line items found for invoice {invoice_id}")
                continue
                
            # Process each line item and create flattened records
            for line_item in line_items:
                line_item_id = line_item.get('line_item_id')
                
                if not line_item_id:
                    logger.debug(f"Line item missing ID for invoice {invoice_id}, skipping")
                    continue
                    
                # Create flattened line item record with invoice data
                line_item_record = {
                    # Invoice fields
                    'invoice_id': invoice_id,
                    'invoice_number': invoice_data.get('invoice_number', ''),
                    'date': invoice_data.get('date', ''),
                    'status': invoice_data.get('status', ''),
                    'customer_id': invoice_data.get('customer_id', ''),
                    'customer_name': invoice_data.get('customer_name', ''),
                    'currency_code': invoice_data.get('currency_code', ''),
                    
                    # Line item fields
                    'line_item_id': line_item_id,
                    'item_id': line_item.get('item_id', ''),
                    'name': line_item.get('name', ''),
                    'description': line_item.get('description', ''),
                    'item_order': line_item.get('item_order', 0),
                    'quantity': line_item.get('quantity', 0),
                    'unit': line_item.get('unit', ''),
                    'rate': line_item.get('rate', 0),
                    'bcy_rate': line_item.get('bcy_rate', 0),
                    
                    # Tax fields
                    'tax_id': line_item.get('tax_id', ''),
                    'tax_name': line_item.get('tax_name', ''),
                    'tax_percentage': line_item.get('tax_percentage', 0),
                    'tax_type': line_item.get('tax_type', ''),
                    
                    # Financial fields
                    'discount': line_item.get('discount', 0),
                    'discount_amount': line_item.get('discount_amount', 0),
                    'item_total': line_item.get('item_total', 0),
                    
                    # Additional fields
                    'hsn_or_sac': line_item.get('hsn_or_sac', ''),
                    'project_id': line_item.get('project_id', ''),
                    'warehouse_id': line_item.get('warehouse_id', '')
                }
                
                # Add line item record to mapping data
                invoice_line_item_mapping_data.append(line_item_record)
                
        except Exception as e:
            logger.debug(f"Error processing invoice data: {str(e)}")
            continue
        
    logger.debug(f"Extracted {len(invoice_line_item_mapping_data)} invoice line item mappings")
    
    return invoice_line_item_mapping_data

def save_invoice_line_item_mappings_to_database(invoice_line_item_mapping_data):
    """
    Save invoice line item mapping records to database.
    
    Args:
        invoice_line_item_mapping_data (list): List of invoice line item mapping records
        
    Returns:
        DataFrame: DataFrame of all invoice line item mappings
    """
    if not invoice_line_item_mapping_data:
        logger.debug("No invoice line item mappings to save")
        return pd.DataFrame()
        
    # Convert to DataFrame
    line_item_mappings_df = pd.DataFrame.from_records(invoice_line_item_mapping_data)
    logger.debug(f"Saving {len(line_item_mappings_df)} invoice line item mappings")
    
    # Check if mapping table exists
    try:

        crud.create_table('invoice_line_item_mapping', line_item_mappings_df)
        # existing_line_item_mappings = crud.read_table('invoice_line_item_mapping')
        # logger.debug(f"Found {len(existing_line_item_mappings)} existing invoice line item mappings")
        
        # # Combine with new mappings
        # all_line_item_mappings = pd.concat([existing_line_item_mappings, line_item_mappings_df], ignore_index=True)
        
        # # Remove duplicates based on invoice_id and line_item_id
        # all_line_item_mappings = all_line_item_mappings.drop_duplicates(
        #     subset=['invoice_id', 'line_item_id'], 
        #     keep='last'
        # )
        
        # # Save combined mappings
        # crud.create_table('zakya_invoice_line_item_mapping', all_line_item_mappings)
        logger.debug(f"Saved {len(line_item_mappings_df)} total invoice line item mappings")
        
        return line_item_mappings_df
        
    except Exception:
        # Table doesn't exist, create new
        logger.debug("Creating new invoice line item mapping table")
        crud.create_table('invoice_line_item_mapping', line_item_mappings_df)
        logger.debug(f"Created table with {len(line_item_mappings_df)} invoice line item mappings")
        
        return line_item_mappings_df

# This function has been removed as we don't need to map invoices to sales orders

async def sync_invoice_mappings(config):
    """
    Main orchestration function that runs the entire process.
    
    Args:
        config (dict): Dictionary with API credentials and settings
        
    Returns:
        tuple: (invoice_mappings_df, line_item_mappings_df)
    """
    logger.debug("Starting invoice synchronization")
    
    # Step 1: Fetch all invoices from Zakya
    all_invoices_df = fetch_all_invoices_from_zakya(config)
    
    if all_invoices_df.empty:
        logger.debug("No invoices found in Zakya")
        return pd.DataFrame(), pd.DataFrame()
    
    # Step 2: Identify invoices that need processing
    new_invoices_df = identify_new_invoices(all_invoices_df, config)
    
    if new_invoices_df.empty:
        logger.debug("No new invoices to process")
        return pd.DataFrame(), pd.DataFrame()
    
    # Step 3: Fetch detailed information for new invoices
    detailed_invoices = await fetch_invoice_details(new_invoices_df, config)
    
    if not detailed_invoices:
        logger.debug("Failed to fetch invoice details")
        return pd.DataFrame(), pd.DataFrame()
    
    # Step 4: Extract and flatten invoice data
    invoice_line_item_mapping_data = extract_flattened_invoice_data(detailed_invoices)
    
    # Step 6: Save invoice line item mappings to database
    line_item_mappings_df = save_invoice_line_item_mappings_to_database(invoice_line_item_mapping_data)
    
    logger.debug("Completed invoice synchronization")
    return line_item_mappings_df

def sync_invoice_mappings_sync(config):
    """
    Wrapper function to run async sync_invoice_mappings function.
    
    Args:
        config (dict): Configuration dictionary with API credentials
        
    Returns:
        tuple: Results from async function (invoice_mappings, line_item_mappings)
    """
    # Run the async function
    return asyncio.run(sync_invoice_mappings(config))