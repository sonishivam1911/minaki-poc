import re
import streamlit as st
import asyncio
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from dotenv import load_dotenv
from utils.postgres_connector import crud
from config.logger import logger
from utils.common_filtering_database_function import find_product
from utils.zakya_api import fetch_records_from_zakya, post_record_to_zakya, fetch_object_for_each_id
from core.helper_zakya import extract_record_list
from server.invoice.route import PerniaInvoiceProcessor
# Load environment variables from .env file
load_dotenv()

def fetch_pernia_data_from_database(input):
    # Fetch all data from ppus_orders table
    pernia_data = crud.read_table('ppus_orders')
    pernia_data = pernia_data[pernia_data['Product Status'] == 'Received and QC Pass']
    logger.debug(f"Pernia data fetched is : {pernia_data.columns}")
    
    # Extract start and end dates from input
    start_date = input.get('start_date')
    end_date = input.get('end_date')
    
    if not start_date or not end_date:
        return pernia_data  # Return all data if no date range provided
    
    # Function to convert date format from '30 September, 2025' to '2025-09-30'
    def convert_date_format(date_str):

        if isinstance(date_str, str) and re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            return date_str
        
        date_object = datetime.strptime(date_str, "%B %d, %Y")
        formatted_date = date_object.strftime("%Y-%m-%d")
        # Return formatted date
        return formatted_date
    
    # Convert input dates to the standard format
    start_date_formatted = convert_date_format(start_date)
    end_date_formatted = convert_date_format(end_date)
    
    # Filter data based on PO Date
    filtered_data = []
    for _,order in pernia_data.iterrows():
        # logger.debug(f"order row is : {order}")
        po_date = order.get("PO Date")
        if po_date:
            po_date_formatted = convert_date_format(po_date)
            # logger.debug(f" po date format after formatting is : {po_date}")
            if po_date_formatted:
                # Compare dates as strings (works with YYYY-MM-DD format)
                if start_date_formatted <= po_date_formatted <= end_date_formatted:
                    filtered_data.append(order)
    
    return filtered_data

def fetch_salesorders_by_customer(config):
    """Fetch sales orders for a specific customer."""
    try:

        items_data_result = []
        for indx,row in config['pernia_orders'].iterrows():
            sku = row.get("Vendor Code"," ")
            logger.debug(f"Sku is : {sku}")
            if len(sku) > 0:
                item=find_product(sku)
                logger.debug(f"Item is : {item}")
                items_data_result.extend(item)

        mapped_pernia_products_df = pd.DataFrame.from_records(items_data_result)
        mapped_pernia_products_df = mapped_pernia_products_df[['item_id']]
        # logger.debug(f"Mapped Pernia Products Dataframe Columns and Size : {mapped_pernia_products_df.columns} and {len(mapped_pernia_products_df)}")
        # Fetch all sales orders
        sales_orders_data = fetch_records_from_zakya(
            config['base_url'],
            config['access_token'],
            config['organization_id'],
            '/salesorders'
        )

        salesorder_item_mapping_df = crud.read_table('zakya_salesorder_line_item_mapping')
        # Extract sales orders
        all_orders = extract_record_list(sales_orders_data, "salesorders")
        # Convert to DataFrame for easier filtering
        sales_orders_df = pd.DataFrame(all_orders)
        sales_orders_df = sales_orders_df[sales_orders_df['customer_id'] == config['customer_id']]
        logger.debug(f"Sales Order after filtering : {sales_orders_df}")
        sales_orders_df = pd.merge(
            left=sales_orders_df, right=salesorder_item_mapping_df,
            how='left' , on=['salesorder_id']
        )

        mapped_sales_order_with_product_df = pd.merge(
            left=sales_orders_df, right=mapped_pernia_products_df,
            how='left', on=['item_id']
        )
        sales_order_with_product_mapped_columns = ['salesorder_id','line_item_id', 'date',
                                                   'delivery_date', 'salesorder_number_x',
                                                   'item_id','item_name']

        # logger.debug(f"Mapped Sales Order & Product Mapping Dataframe Columns and Size : {mapped_sales_order_with_product_df.columns} and {len(mapped_sales_order_with_product_df)}")
        
        # Log the columns for debugging
        return mapped_sales_order_with_product_df[sales_order_with_product_mapped_columns]
        
            
    except Exception as e:
        logger.error(f"Error fetching sales orders: {str(e)}")
        return pd.DataFrame()


def process_sales_orders(order_data, customer_id, zakya_config, options=None):
    """
    Process and create sales orders for multiple items grouped by reference number.
    
    Args:
        order_data (DataFrame): DataFrame containing order data
        customer_id (str): Customer ID in Zakya
        zakya_config (dict): Zakya API connection details
        options (dict, optional): Customization options with the following possible keys:
            - ref_field: Field name for reference number (default: 'PO Number')
            - date_field: Field name for order date (default: 'PO Date')
            - delivery_date_field: Field name for delivery date (default: 'Delivery Date')
            - price_field: Field name for item price (default: 'PO Value')
            - sku_field: Field name for SKU (default: 'Vendor Code')
            - partner_sku_field: Field name for partner SKU (default: 'SKU Code')
            - description_field: Field name for item description (default: 'Designer Name')
            - quantity_value: Default quantity value (default: 1)
            - order_source: Order source description (default: 'Pernia')
    
    Returns:
        dict: Results of the operation including:
            - success (bool): Overall success status
            - created_count (int): Number of sales orders created
            - errors (list): List of errors encountered
            - details (list): Detailed results for each sales order
    """
    logger.debug(f"Starting process_sales_orders for customer_id: {customer_id}")
    
    # Set default options
    default_options = {
        'ref_field': 'PO Number',
        'date_field': 'PO Date',
        'delivery_date_field': 'Delivery Date',
        'price_field': 'PO Value',
        'sku_field': 'Vendor Code',
        'partner_sku_field': 'SKU Code',
        'description_field': 'Designer Name',
        'quantity_value': 1,
        'order_source': 'Pernia'
    }
    
    # Merge with provided options
    opts = {**default_options, **(options or {})}
    
    # Initialize results
    results = {
        'success': False,
        'created_count': 0,
        'errors': [],
        'details': []
    }
    
    try:
        # Load product mappings
        mapping_product = crud.read_table("zakya_products")
        mapping_order = crud.read_table("zakya_sales_order")

        logger.debug("Database call completed")
        
        # Group orders by reference number
        grouped_orders = order_data.groupby(opts['ref_field'])
        
        # Track created sales orders
        created_count = 0
        
        # Process each group (each reference number)
        for ref_number, group in grouped_orders:
            # Skip if reference number is missing
            if pd.isna(ref_number) or not ref_number:
                results['errors'].append("Skipping group with missing reference number")
                continue
            
            # Check if a sales order with this reference already exists
            ref_number_str = str(ref_number)
            existing_order = mapping_order[mapping_order["reference_number"] == ref_number_str]
            logger.debug(f"Existing Order : {existing_order}")
            
            if not existing_order.empty:
                logger.debug(f"Sales Order with reference number {ref_number_str} already exists.")
                results['details'].append({
                    'reference_number': ref_number_str,
                    'status': 'Skipped',
                    'reason': 'Already exists'
                })
                continue
            
            # Create line items for this reference number
            line_items = []
            
            # Process each item in this reference group
            for _, item in group.iterrows():
                sku = str(item.get(opts['sku_field'], '')).strip()
                partner_sku = str(item.get(opts['partner_sku_field'], '')).strip()
                description = str(item.get(opts['description_field'], '')).strip()
                price = float(item.get(opts['price_field'], 0))
                
                # Skip if price is invalid
                if price <= 0:
                    logger.warning(f"Skipping item with invalid price: {price}")
                    continue
                
                logger.debug(f"sku is {sku} & partner_sku : {partner_sku} & price : {price}")
                # Create line item
                line_item = {
                    "description": f"PO: {ref_number_str} and {description} - {partner_sku}",
                    "rate": price,
                    "quantity": opts['quantity_value'],
                    "item_total": price * opts['quantity_value']
                }

                logger.debug(f"line_items are {line_item}")
                
                # Try to find item_id for this SKU
                if sku:
                    filtered_products = mapping_product[mapping_product["sku"] == sku]
                    if not filtered_products.empty:
                        item_id = filtered_products["item_id"].iloc[0]
                        line_item["item_id"] = int(item_id)

                        logger.debug(f"line_item after dding item id {line_item}")
                
                line_items.append(line_item)
            
            # Skip if no valid line items
            if not line_items:
                results['errors'].append(f"No valid line items for reference {ref_number_str}")
                results['details'].append({
                    'reference_number': ref_number_str,
                    'status': 'Skipped',
                    'reason': 'No valid line items'
                })
                continue
            
            # Get dates from first item in group
            first_item = group.iloc[0]
            order_date = format_date_for_api(first_item.get(opts['date_field'], None))
            delivery_date = format_date_for_api(first_item.get(opts['delivery_date_field'], None))
            
            # If dates are missing, use current date
            if not order_date:
                order_date = datetime.now().strftime('%Y-%m-%d')
            if not delivery_date:
                delivery_date = datetime.now().strftime('%Y-%m-%d')
            
            # Create sales order payload
            salesorder_payload = {
                "customer_id": int(customer_id),
                # "salesorder_number": f"MN/SO/{ref_number}",
                "date": order_date,
                "shipment_date": delivery_date,
                "reference_number": ref_number_str,
                "line_items": line_items,
                "notes": "Created With Minkai Tool",
                "terms": "Terms and Conditions"
            }

            logger.debug(f" Sales Order payload : {salesorder_payload}")
            
            # Create the sales order
            try:
                logger.debug(f"Creating sales order for reference {ref_number_str}")
                response = post_record_to_zakya(
                    zakya_config['base_url'],
                    zakya_config['access_token'],  
                    zakya_config['organization_id'],
                    'salesorders',
                    salesorder_payload
                )
                
                # Check response
                if response and 'salesorder' in response:
                    created_count += 1
                    results['details'].append({
                        'reference_number': ref_number_str,
                        'status': 'Success',
                        'salesorder_id': response['salesorder'].get('salesorder_id'),
                        'salesorder_number': response['salesorder'].get('salesorder_number'),
                        'line_item_count': len(line_items)
                    })
                    logger.info(f"Created sales order for reference {ref_number_str}")
                else:
                    error_msg = f"Failed to create sales order for reference {ref_number_str}"
                    results['errors'].append(error_msg)
                    results['details'].append({
                        'reference_number': ref_number_str,
                        'status': 'Failed',
                        'error': str(response)
                    })
                    logger.error(f"{error_msg}: {response}")
            except Exception as e:
                error_msg = f"Error creating sales order for reference {ref_number_str}"
                results['errors'].append(f"{error_msg}: {str(e)}")
                results['details'].append({
                    'reference_number': ref_number_str,
                    'status': 'Failed',
                    'error': str(e)
                })
                logger.error(f"{error_msg}: {e}")
        
        # Update results
        results['created_count'] = created_count
        results['success'] = created_count > 0
        
        return results
    
    except Exception as e:
        logger.error(f"Error in process_sales_orders: {e}")
        results['errors'].append(f"General error: {str(e)}")
        return results

def format_date_for_api(date_input):
    """Format date input for Zakya API (handles various input formats)."""
    if not date_input or pd.isna(date_input):
        return None
    
    # If already a datetime object
    if isinstance(date_input, datetime):
        return date_input.strftime('%Y-%m-%d')
    
    # If already in correct string format
    if isinstance(date_input, str) and re.match(r'^\d{4}-\d{2}-\d{2}$', date_input):
        return date_input
    
    # Try different date formats
    date_formats = [
        "%B %d, %Y",  # January 04, 2023
        "%b %d, %Y",  # Jan 04, 2023
        "%d/%m/%Y",   # 04/01/2023
        "%m/%d/%Y",   # 01/04/2023
        "%d-%m-%Y",   # 04-01-2023
        "%Y/%m/%d"    # 2023/01/04
    ]
    
    if isinstance(date_input, str):
        for date_format in date_formats:
            try:
                date_obj = datetime.strptime(date_input, date_format)
                return date_obj.strftime('%Y-%m-%d')
            except ValueError:
                continue
    
    # If all parsing attempts fail, return None
    logger.warning(f"Could not parse date: {date_input}")
    return None



def fetch_salesorders_by_customer_service(config):
    """
    Fetch sales orders for a specific customer with enhanced inventory data.
    
    Args:
        config (dict): Configuration dictionary containing:
            - base_url, access_token, organization_id: Zakya API connection details
            - customer_id: Customer ID to fetch sales orders for
            - pernia_orders (optional): DataFrame of Pernia orders to filter by
            - include_inventory (optional): Whether to include inventory data
    
    Returns:
        DataFrame: Sales orders with enhanced data
    """
    try:
        # Fetch all sales orders
        sales_orders_data = fetch_records_from_zakya(
            config['base_url'],
            config['access_token'],
            config['organization_id'],
            '/salesorders'
        )

        salesorder_item_mapping_df = crud.read_table('zakya_salesorder_line_item_mapping')
        
        # Extract sales orders
        all_orders = extract_record_list(sales_orders_data, "salesorders")
        
        # Convert to DataFrame for easier filtering
        sales_orders_df = pd.DataFrame(all_orders)

        # logger.debug(f"Total sales orders fetched: {len(sales_orders_data)}")
        
        # Filter to only include the selected customer
        sales_orders_df = sales_orders_df[sales_orders_df['customer_id'] == config['customer_id']]
        logger.debug(f"Sales orders for customer {config['customer_id']}: {len(sales_orders_df)}")
        
        # Add date range filtering 
        if config.get('start_date') and config.get('end_date'):
            logger.debug(f"Start dat filtering applied : {config.get('start_date')}")
            start_date = config['start_date']
            end_date = config['end_date']
            
            # Convert to datetime object if it's a string
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, '%Y-%m-%d')

            if isinstance(end_date, str):
                end_date = datetime.strptime(end_date, '%Y-%m-%d')

                
            
            # Calculate the date 45 days before start_date
            date_45_days_before = start_date - timedelta(days=45)
            
            # Convert dates for comparison
            end_date_str = end_date.strftime('%Y-%m-%d')
            date_45_days_before_str = date_45_days_before.strftime('%Y-%m-%d')
            
            # Filter sales orders to only include those between 45 days before start_date and start_date
            sales_orders_df = sales_orders_df[
                (sales_orders_df['date'] >= date_45_days_before_str) & 
                (sales_orders_df['date'] <= end_date_str)
            ]


        # Join with the salesorder_item_mapping to get item details
        sales_orders_df = pd.merge(
            left=sales_orders_df, 
            right=salesorder_item_mapping_df,
            how='left', 
            on=['salesorder_id']
        )
        
        # If Pernia orders are provided, filter to only include those items
        if config.get('pernia_orders') is not None:
            # Original Pernia-specific filtering code
            items_data_result = []
            for indx, row in config['pernia_orders'].iterrows():
                sku = row.get("Vendor Code", " ")
                if len(sku) > 0:
                    item = config['product_mapping'](sku)
                    items_data_result.extend(item)
            
            mapped_pernia_products_df = pd.DataFrame.from_records(items_data_result)
            if not mapped_pernia_products_df.empty and 'item_id' in mapped_pernia_products_df.columns:
                mapped_pernia_products_df = mapped_pernia_products_df[['item_id']]
                
                # Filter to only include Pernia items
                mapped_sales_order_with_product_df = pd.merge(
                    left=sales_orders_df, 
                    right=mapped_pernia_products_df,
                    how='inner',  # Only include matches
                    on=['item_id']
                )
            else:
                mapped_sales_order_with_product_df = sales_orders_df
        else:
            # No Pernia filtering - include all items
            mapped_sales_order_with_product_df = sales_orders_df
        
        # Add invoice information - check if each sales order item has been invoiced
        if not mapped_sales_order_with_product_df.empty:
            # Load necessary data from database tables
            logger.debug("Loading data from database tables")
            
            # Load invoice mappings
            try:
                salesorder_invoice_mapping_df = crud.read_table('zakya_salesorder_invoice_mapping')
                logger.debug(f"Loaded {len(salesorder_invoice_mapping_df)} sales order-invoice mappings")
            except Exception as e:
                logger.error(f"Error loading sales order invoice mappings: {str(e)}")
                salesorder_invoice_mapping_df = pd.DataFrame()
            
            # Load sales order line item mappings
            try:
                salesorder_line_item_mapping_df = crud.read_table('salesorder_line_item_mapping')
                logger.debug(f"Loaded {len(salesorder_line_item_mapping_df)} sales order line items")
            except Exception as e:
                logger.error(f"Error loading sales order line item mappings: {str(e)}")
                salesorder_line_item_mapping_df = pd.DataFrame()
            
            # Load invoice line item mappings
            try:
                invoice_item_mapping_df = crud.read_table('zakya_invoice_line_item_mapping')
                logger.debug(f"Loaded {len(invoice_item_mapping_df)} invoice line items")
            except Exception as e:
                logger.error(f"Error loading invoice line item mappings: {str(e)}")
                invoice_item_mapping_df = pd.DataFrame()
            
            # Helper function to extract PO value from reference string
            def extract_po_from_reference(ref_string):
                if pd.isna(ref_string) or not isinstance(ref_string, str):
                    return None
                
                # Check if the reference starts with "PO: " or similar pattern
                if ref_string.strip().startswith("PO"):
                    
                    po_value = ref_string.strip(':')[-1].strip()  # Extract everything after "PO: "
                    logger.debug(f"Reference string is : {ref_string} and po value extracted is : {po_value}")
                    return po_value
                return None
            
            # Pre-process sales orders to extract PO values from reference numbers
            if 'reference_number' in mapped_sales_order_with_product_df.columns:
                mapped_sales_order_with_product_df['extracted_po'] = mapped_sales_order_with_product_df['reference_number'].apply(extract_po_from_reference)
            
            # Create a dictionary to store invoice status and mapped salesorder_id for each row
            invoice_status_dict = {}
            mapped_salesorder_dict = {}
            
            # Function to check if an item is invoiced using reference numbers and database tables
            def check_if_invoiced(row):
                row_id = row.name  # Use DataFrame index as unique identifier
                
                # Method 1: Check reference number against PO Number in pernia_orders
                if 'extracted_po' in row and not pd.isna(row['extracted_po']) and config.get('pernia_orders') is not None:
                    extracted_po = row['extracted_po']
                    
                    # Check if this extracted PO number matches any PO Number in pernia_orders
                    if 'PO Number' in config['pernia_orders'].columns:
                        matching_po = config['pernia_orders'][
                            config['pernia_orders']['PO Number'] == extracted_po
                        ]
                        
                        if not matching_po.empty:
                            # Found a matching PO Number
                            mapped_salesorder_dict[row_id] = row.get('salesorder_id', '')
                            return f"Invoiced (PO: {extracted_po})"
                
                # Method 2: Check using database tables
                if not salesorder_invoice_mapping_df.empty and not invoice_item_mapping_df.empty and 'item_id' in row and not pd.isna(row['item_id']) and 'salesorder_id' in row and not pd.isna(row['salesorder_id']):
                    sales_order_id = row.get('salesorder_id')
                    item_id = row.get('item_id')
                    
                    # Filter for invoices associated with this sales order
                    so_invoices = salesorder_invoice_mapping_df[
                        salesorder_invoice_mapping_df['salesorder_id'] == sales_order_id
                    ]
                    
                    if not so_invoices.empty:
                        # For each invoice, check if the item is included
                        for _, invoice_row in so_invoices.iterrows():
                            invoice_id = invoice_row['invoice_id']
                            invoice_number = invoice_row['invoice_number']
                            
                            # Find matching invoice items
                            matching_items = invoice_item_mapping_df[
                                (invoice_item_mapping_df['invoice_id'] == invoice_id) &
                                (invoice_item_mapping_df['item_id'] == item_id)
                            ]
                            
                            if not matching_items.empty:
                                # If there are multiple matches, sort by line_item_id and take the first
                                if len(matching_items) > 1 and 'line_item_id' in matching_items.columns:
                                    matching_items = matching_items.sort_values('line_item_id').head(1)
                                
                                mapped_salesorder_dict[row_id] = sales_order_id
                                return f"Invoiced ({invoice_number})"
                
                # No invoice found through either method
                if 'item_id' in row and not pd.isna(row['item_id']) and 'salesorder_id' in row and not pd.isna(row['salesorder_id']):
                    # Still store the salesorder_id for non-invoiced items
                    mapped_salesorder_dict[row_id] = row.get('salesorder_id', '')
                
                return "Not Invoiced"
            
            logger.debug("Starting invoice status check")
            
            # Apply the check to each row in the DataFrame
            results = []
            for idx, row in mapped_sales_order_with_product_df.iterrows():
                invoice_status = check_if_invoiced(row)
                results.append(invoice_status)
                invoice_status_dict[idx] = invoice_status
            
            # Add results back to DataFrame
            mapped_sales_order_with_product_df['Invoice Status'] = results
            
            # Add the mapped salesorder_id
            mapped_sales_order_with_product_df['Mapped Salesorder ID'] = mapped_sales_order_with_product_df.index.map(
                lambda x: mapped_salesorder_dict.get(x, '')
            )
            
            logger.debug(f"Invoice status check completed, status counts: {pd.Series(results).value_counts().to_dict()}")
                
        # Add inventory data if requested
        if config.get('include_inventory', False) and not mapped_sales_order_with_product_df.empty:
            # Get unique item IDs
            item_ids = mapped_sales_order_with_product_df['item_id'].dropna().unique().tolist()
            
            if item_ids:
                # Fetch product data
                zakya_products_df = crud.read_table('zakya_products')
                
                # Create inventory lookup dictionary
                inventory_lookup = {}
                
                for item_id in item_ids:
                    if not pd.isna(item_id):
                        product_rows = zakya_products_df[zakya_products_df['item_id'] == item_id]
                        if not product_rows.empty:
                            product_row = product_rows.iloc[0]
                            inventory_lookup[item_id] = {
                                'available_stock': product_row.get('available_stock', 0),
                                'stock_on_hand': product_row.get('stock_on_hand', 0)
                            }
                
                # Add inventory data to the DataFrame
                def add_inventory_data(row):
                    item_id = row.get('item_id')
                    if not pd.isna(item_id) and item_id in inventory_lookup:
                        row['Available Stock'] = inventory_lookup[item_id]['available_stock']
                        row['Stock on Hand'] = inventory_lookup[item_id]['stock_on_hand']
                    else:
                        row['Available Stock'] = 'N/A'
                        row['Stock on Hand'] = 'N/A'
                    return row
                
                mapped_sales_order_with_product_df = mapped_sales_order_with_product_df.apply(
                    add_inventory_data, axis=1
                )

        # Group by salesorder, item name, and date, then calculate averages for metrics
        grouped_df = mapped_sales_order_with_product_df.groupby(
            ['salesorder_number_x', 'item_name', 'date', 'item_id', 'Invoice Status', 'Mapped Salesorder ID']
        ).agg({
            'quantity_y': 'sum',  # Sum quantities for same item in same order
            'rate': 'mean',      # Average rate
            'amount': 'sum'      # Sum amounts
        }).reset_index()

        # Rename columns for clarity
        renamed_df = grouped_df.rename(columns={
            'salesorder_number_x': 'Order Number',
            'item_name': 'Item Name',
            'date': 'Order Date',
            'quantity_y': 'Total Quantity',
            'rate': 'Average Rate',
            'amount': 'Total Amount'
        })

        # Add inventory columns if they exist
        if 'Available Stock' in mapped_sales_order_with_product_df.columns:
            inventory_data = mapped_sales_order_with_product_df.groupby(
                ['item_id']
            ).agg({
                'Available Stock': 'first',
                'Stock on Hand': 'first'
            }).reset_index()
            
            renamed_df = pd.merge(
                left=renamed_df,
                right=inventory_data,
                how='left',
                on=['item_id']
            )

        return renamed_df
        
    except Exception as e:
        logger.error(f"Error fetching sales orders: {str(e)}")
        return pd.DataFrame()
    

def analyze_missing_salesorders(pernia_orders, product_mapping, sales_orders):
    """
    Analyze which Pernia products need sales orders and which are already in sales orders.
    
    Args:
        pernia_orders (DataFrame): Pernia order data
        product_mapping (dict): SKU to item_id mapping
        sales_orders (DataFrame): Existing sales orders data
    
    Returns:
        tuple: (missing_df, present_df) - Missing and present sales order items with their details
    """
    # Prepare lists to store missing and present sales order items
    missing_items = []
    present_items = []
    
    logger.debug(f"Product mapping is : {product_mapping}")
    
    # Get existing sales orders item_ids
    mapped_sales_order_items = set()
    # Dictionary to store the lowest sales order ID for each item_id
    lowest_so_mapping = {}
    
    if sales_orders is not None and not sales_orders.empty and 'item_id' in sales_orders.columns:
        # Only consider non-invoiced items as valid
        valid_orders = sales_orders[sales_orders['Invoice Status'] == 'Not Invoiced']
        if not valid_orders.empty:
            mapped_sales_order_items = set(valid_orders['item_id'].dropna().unique())
            
            # Create a mapping of item_id to the lowest sales order for each item
            for item_id in mapped_sales_order_items:
                # Filter for rows with this item_id
                item_orders = valid_orders[valid_orders['item_id'] == item_id]
                if not item_orders.empty:
                    # Get the row with the lowest salesorder_id
                    if 'Mapped Salesorder ID' in item_orders.columns:
                        # Sort by salesorder_id and take the first row
                        lowest_so = item_orders.sort_values('Mapped Salesorder ID').iloc[0]
                        lowest_so_mapping[item_id] = lowest_so.to_dict()
                        logger.debug(f"Mapping for so is : {lowest_so_mapping}")
    
    # Check each Pernia order item
    for idx, row in pernia_orders.iterrows():
        sku = row.get('Vendor Code', '').strip()

        if not sku:
            continue
        
        # Check if this SKU is mapped to a product
        is_mapped = sku in product_mapping
        item_id = product_mapping.get(sku, None)

        # Check if this item has a valid sales order
        has_sales_order = False
        if is_mapped and item_id in mapped_sales_order_items:
            has_sales_order = True

        logger.debug(f"For sku : {sku} and is mapped be : {is_mapped} and item is : {item_id} and has_sales_order flag is : {has_sales_order}")             
        
        # Convert Pernia order row to dict for both cases
        pernia_item = row.to_dict()
        pernia_item['is_mapped'] = is_mapped
        pernia_item['item_id'] = item_id
        # pernia_item['Mapped Sales Order ID'] = 
        
        if has_sales_order:
            # It's present - get the associated lowest sales order info
            so_info = lowest_so_mapping.get(item_id, {})
            
            # Merge Pernia item data with sales order data
            present_item = {**pernia_item, **so_info}
            present_items.append(present_item)
        else:
            # It's missing - add reason
            if not is_mapped:
                pernia_item['reason'] = "Not mapped in Zakya"
            elif item_id not in mapped_sales_order_items:
                # Check if it's invoiced
                if sales_orders is not None and not sales_orders.empty:
                    invoiced_orders = sales_orders[
                        (sales_orders['item_id'] == item_id) & 
                        (sales_orders['Invoice Status'] != 'Not Invoiced')
                    ]
                    if not invoiced_orders.empty:
                        pernia_item['reason'] = "Already invoiced"
                    else:
                        pernia_item['reason'] = "No sales order found"
                else:
                    pernia_item['reason'] = "No sales order found"
            
            missing_items.append(pernia_item)
    
    # Create DataFrames
    missing_df = pd.DataFrame(missing_items) if missing_items else pd.DataFrame()
    present_df = pd.DataFrame(present_items) if present_items else pd.DataFrame()
    
    logger.debug(f"Missing Df is as follows : {missing_df}")
    logger.debug(f"Present Df is as follows : {present_df}")
    
    # Add inventory data for mapped items in missing_df
    if not missing_df.empty and 'item_id' in missing_df.columns:
        missing_df = add_inventory_data(missing_df)
    
    # Add inventory data for mapped items in present_df
    if not present_df.empty and 'item_id' in present_df.columns:
        present_df = add_inventory_data(present_df)
    
    logger.debug(f"Finalized Missing Dataframe is : {missing_df}")
    logger.debug(f"Finalized Present Dataframe is : {present_df}")
    
    return missing_df, present_df

def add_inventory_data(df):
    """
    Helper function to add inventory data to mapped items in a DataFrame.
    
    Args:
        df (DataFrame): DataFrame with item_id column
    
    Returns:
        DataFrame: Updated DataFrame with inventory data
    """
    mapped_items = df[df['is_mapped'] == True]
    if not mapped_items.empty:
        item_ids = mapped_items['item_id'].dropna().unique().tolist()
        
        if item_ids:
            # Fetch product data
            zakya_products_df = crud.read_table('zakya_products')
            
            # Add inventory data
            for idx, row in df.iterrows():
                if row.get('is_mapped') and not pd.isna(row.get('item_id')):
                    item_id = row['item_id']
                    product_rows = zakya_products_df[zakya_products_df['item_id'] == item_id]
                    
                    if not product_rows.empty:
                        product_row = product_rows.iloc[0]
                        df.at[idx, 'available_stock'] = product_row.get('available_stock', 0)
                        df.at[idx, 'stock_on_hand'] = product_row.get('stock_on_hand', 0)
    
    return df

def create_missing_salesorders(missing_orders, zakya_connection, customer_id):
    """
    Create missing sales orders for Pernia items.
    
    Args:
        missing_orders (DataFrame): Missing sales order items
        zakya_connection (dict): Zakya API connection details
        customer_id (str): Customer ID in Zakya
    
    Returns:
        dict: Results of the operation
    """
    # Use the reusable process_sales_orders function with Pernia-specific settings
    pernia_options = {
        'ref_field': 'PO Number',
        'date_field': 'PO Date',
        'delivery_date_field': 'Delivery Date',
        'price_field': 'PO Value',
        'sku_field': 'Vendor Code',
        'partner_sku_field': 'SKU Code',
        'description_field': 'Designer Name',
        'quantity_value': 1,
        'order_source': 'Pernia'
    }

    logger.debug(f"Pernia options : {pernia_options}")
    
    return process_sales_orders(missing_orders, customer_id, zakya_connection, pernia_options)    



def analyze_products(pernia_orders_df):
    """
    Analyze Pernia orders to identify mapped and unmapped products.
    This function is called from the frontend to initiate product analysis.
    
    Args:
        pernia_orders_df (DataFrame): Pernia order data
    
    Returns:
        dict: Analysis results including mapped and unmapped products
    """
    try:
        # Convert any results to the appropriate format
        if not isinstance(pernia_orders_df, pd.DataFrame):
            pernia_orders_df = pd.DataFrame(pernia_orders_df)
                
        # Get connection details from session state
        zakya_connection = {
            'base_url': st.session_state['api_domain'],
            'access_token': st.session_state['access_token'],
            'organization_id': st.session_state['organization_id']
        }
        
        # Create processor instance
        processor = PerniaInvoiceProcessor(
            pernia_orders_df, 
            datetime.now(),  # Not used for analysis
            zakya_connection,
            st.session_state['selected_customer']
        )
        
        # Run preprocess step
        processor.preprocess_data_sync()
        
        # Find existing products
        product_config = asyncio.run(processor.find_existing_products())
        # logger.debug(f"Product config is : {product_config}")
        
        # Format results
        mapped_products = []
        unmapped_products = []
        product_mapping = product_config.get('existing_sku_item_id_mapping',{})
        
        # Process mapped products
        for sku in product_config.get('existing_products', []):
            # logger.debug(f"SKU is : {sku}")
            item_id = product_config['existing_sku_item_id_mapping'].get(sku, None)
            # logger.debug(f"item_id is : {item_id}")
            if item_id:
                # Get product details from mapping
                product_data = product_config['existing_products_data_dict'].get(item_id, {})
                # logger.debug(f"product_data is : {product_data}")
                # Find matching row in Pernia orders
                matching_rows = pernia_orders_df[pernia_orders_df['Vendor Code'] == sku]
                po_data = {}
                
                if not matching_rows.empty:
                    row = matching_rows.iloc[0]
                    
                    po_data = {
                        'designer_name': row.get('Designer Name', ''),
                        'po_number': row.get('PO Number', ''),
                        'po_date': row.get('PO Date', ''),
                        'po_value': row.get('PO Value', 0)
                    }
                    # logger.debug(f"po_data : {po_data}")
                
                # Combine product and PO data
                mapped_products.append({
                    'sku': sku,
                    'item_id': item_id,
                    'item_name': product_data[0].get('item_name', ''),
                    'available_stock': product_data[0].get('available_stock', 0),
                    'stock_on_hand': product_data[0].get('stock_on_hand', 0),
                    **po_data
                })
                
                # Add to mapping
                product_mapping[sku] = item_id
        
        logger.debug(f"Mapped Products : {product_mapping}")
        # Process unmapped products
        for sku in product_config.get('missing_products', []):
            # Find matching row in Pernia orders
            matching_rows = pernia_orders_df[pernia_orders_df['Vendor Code'] == sku]
            
            if not matching_rows.empty:
                row = matching_rows.iloc[0]
                unmapped_products.append({
                    'sku': sku,
                    'designer_name': row.get('Designer Name', ''),
                    'po_number': row.get('PO Number', ''),
                    'po_date': row.get('PO Date', ''),
                    'po_value': row.get('PO Value', 0),
                    'error': 'Product not found in Zakya'
                })
            else:
                matching_rows = pernia_orders_df[pernia_orders_df['SKU Code'] == sku]
                row = matching_rows.iloc[0]
                unmapped_products.append({
                    'sku': '',
                    'designer_name': row.get('Designer Name', ''),
                    'po_number': row.get('PO Number', ''),
                    'po_date': row.get('PO Date', ''),
                    'po_value': row.get('PO Value', 0),
                    'error': 'Product not found in Zakya'
                })                
                

        # Return the analysis results
        return {
            'mapped_products': mapped_products,
            'unmapped_products': unmapped_products,
            'product_mapping': product_mapping,
            'raw_results': product_config
        }
    except Exception as e:
        logger.error(f"Error analyzing products: {e}")
        return {
            'mapped_products': [],
            'unmapped_products': [],
            'product_mapping': {},
            'error': str(e)
        }
    


def fetch_inventory_data(zakya_connection, product_mapping):
    """
    Fetch inventory data for mapped products.
    
    Args:
        zakya_connection (dict): Zakya API connection details
        product_mapping (dict): Mapping of SKUs to item IDs
    
    Returns:
        dict: Dictionary mapping item_ids to inventory details
    """
    try:        
        # Get all item IDs from product mapping
        item_ids = list(product_mapping.values())
        
        if not item_ids:
            return {}
            
        # Fetch product data from zakya_products table
        zakya_products_df = crud.read_table('zakya_products')
        
        # Create inventory lookup dictionary
        inventory_lookup = {}
        
        for item_id in item_ids:
            if item_id is not None:  # Skip None values
                product_rows = zakya_products_df[zakya_products_df['item_id'] == item_id]
                if not product_rows.empty:
                    product_row = product_rows.iloc[0]
                    inventory_lookup[item_id] = {
                        'available_stock': product_row.get('available_stock', 0),
                        'stock_on_hand': product_row.get('stock_on_hand', 0),
                        'actual_available_stock': product_row.get('actual_available_stock', 0),
                        'reorder_level': product_row.get('reorder_level', 0),
                        'track_inventory': product_row.get('track_inventory', False)
                    }
        
        return inventory_lookup
    except Exception as e:
        st.error(f"Error fetching inventory data: {str(e)}")
        return {}    