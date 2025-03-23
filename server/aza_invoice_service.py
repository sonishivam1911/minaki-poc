import pandas as pd
import asyncio
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import re
import streamlit as st
from config.logger import logger
from utils.postgres_connector import crud
from utils.common_filtering_database_function import find_product
from utils.zakya_api import fetch_records_from_zakya, post_record_to_zakya, fetch_object_for_each_id
from core.helper_zakya import extract_record_list
from server.invoice.route import AzaInvoiceProcessor

def analyze_aza_products(aza_orders_df, sku_field="SKU"):
    """
    Analyze Aza orders to identify mapped and unmapped products.
    This function is called from the frontend to initiate product analysis.
    
    Args:
        aza_orders_df (DataFrame): Aza order data
        sku_field (str): Field name for SKUs in the dataframe
    
    Returns:
        dict: Analysis results including mapped and unmapped products
    """
    try:
        # Convert any results to the appropriate format
        if not isinstance(aza_orders_df, pd.DataFrame):
            aza_orders_df = pd.DataFrame(aza_orders_df)
                
        # Get connection details from session state
        zakya_connection = {
            'base_url': st.session_state['api_domain'],
            'access_token': st.session_state['access_token'],
            'organization_id': st.session_state['organization_id']
        }
        
        # Create processor instance
        processor = AzaInvoiceProcessor(
            aza_orders_df, 
            datetime.now(),  # Not used for analysis
            zakya_connection,
            st.session_state['selected_customer']
        )
        
        # Run preprocess step
        processor.preprocess_data_sync()
        
        # Use the processor's analyze_uploaded_products method
        product_analysis = asyncio.run(processor.analyze_uploaded_products())
        
        return product_analysis
    except Exception as e:
        logger.error(f"Error analyzing Aza products: {e}")
        return {
            'mapped_products': [],
            'unmapped_products': [],
            'product_mapping': {},
            'error': str(e)
        }

def fetch_aza_salesorders_by_customer_service(config):
    """
    Fetch sales orders for a specific customer with enhanced inventory data for Aza orders.
    
    Args:
        config (dict): Configuration dictionary containing:
            - base_url, access_token, organization_id: Zakya API connection details
            - customer_id: Customer ID to fetch sales orders for
            - aza_orders (optional): DataFrame of Aza orders to filter by
            - include_inventory (optional): Whether to include inventory data
            - sku_field (optional): Field name for SKUs in Aza orders (default: "SKU")
    
    Returns:
        DataFrame: Sales orders with enhanced data
    """
    # Create a processor instance
    if 'aza_orders' in config:
        processor = AzaInvoiceProcessor(
            config['aza_orders'], 
            datetime.now(),
            {
                'base_url': config['base_url'],
                'access_token': config['access_token'],
                'organization_id': config['organization_id']
            },
            "Aza Customer"  # Placeholder, not used in this context
        )
        
        # Call the processor method
        result = asyncio.run(processor.find_existing_aza_salesorders(
            config['customer_id'],
            config.get('include_inventory', True)
        ))
        
        logger.debug(f"Result after fetch sales order is : {result}")
        return result
        
    #     # Fetch all sales orders directly if no Aza orders provided
    #     sales_orders_data = fetch_records_from_zakya(
    #         config['base_url'],
    #         config['access_token'],
    #         config['organization_id'],
    #         '/salesorders'
    #     )

    #     salesorder_item_mapping_df = crud.read_table('zakya_salesorder_line_item_mapping')
        
    #     # Extract sales orders
    #     all_orders = extract_record_list(sales_orders_data, "salesorders")
        
    #     # Convert to DataFrame for easier filtering
    #     sales_orders_df = pd.DataFrame(all_orders)

    #     logger.debug(f"Total sales orders fetched: {len(sales_orders_data)}")
        
    #     # Filter to only include the selected customer
    #     sales_orders_df = sales_orders_df[sales_orders_df['customer_id'] == config['customer_id']]
    #     logger.debug(f"Sales orders for customer {config['customer_id']}: {len(sales_orders_df)}")
        
    #     # Join with the salesorder_item_mapping to get item details
    #     sales_orders_df = pd.merge(
    #         left=sales_orders_df, 
    #         right=salesorder_item_mapping_df,
    #         how='left', 
    #         on=['salesorder_id']
    #     )
        
    #     # Process invoice status
    #     if not sales_orders_df.empty:
    #         invoice_item_mapping_df = crud.read_table('zakya_invoice_line_item_mapping')
            
    #         # Add invoice status column
    #         sales_orders_df['Invoice Status'] = 'Not Invoiced'
            
    #         # Check invoice status for each item
    #         for idx, row in sales_orders_df.iterrows():
    #             if 'item_id' in row and not pd.isna(row['item_id']):
    #                 item_id = row['item_id']
                    
    #                 # Check if this item is in any invoice
    #                 if not invoice_item_mapping_df.empty:
    #                     invoice_matches = invoice_item_mapping_df[
    #                         invoice_item_mapping_df['item_id'] == item_id
    #                     ]
                        
    #                     if not invoice_matches.empty:
    #                         invoice_num = invoice_matches.iloc[0].get('invoice_number', '')
    #                         sales_orders_df.at[idx, 'Invoice Status'] = f"Invoiced ({invoice_num})"
        
    #     # Add inventory data if requested
    #     if config.get('include_inventory', False) and not sales_orders_df.empty:
    #         # Fetch product data
    #         zakya_products_df = crud.read_table('zakya_products')
            
    #         # Add inventory data to each row
    #         for idx, row in sales_orders_df.iterrows():
    #             if 'item_id' in row and not pd.isna(row['item_id']):
    #                 item_id = row['item_id']
                    
    #                 product_rows = zakya_products_df[zakya_products_df['item_id'] == item_id]
    #                 if not product_rows.empty:
    #                     product_row = product_rows.iloc[0]
    #                     sales_orders_df.at[idx, 'Available Stock'] = product_row.get('available_stock', 0)
    #                     sales_orders_df.at[idx, 'Stock on Hand'] = product_row.get('stock_on_hand', 0)
        
    #     # Format and return the results
    #     if not sales_orders_df.empty:
    #         # Group by relevant fields and calculate aggregates
    #         grouped_df = sales_orders_df.groupby(
    #             ['salesorder_number', 'item_name', 'date', 'item_id', 'Invoice Status']
    #         ).agg({
    #             'quantity': 'sum',
    #             'rate': 'mean',
    #             'amount': 'sum'
    #         }).reset_index()
            
    #         # Rename columns for clarity
    #         renamed_df = grouped_df.rename(columns={
    #             'salesorder_number': 'Order Number',
    #             'item_name': 'Item Name',
    #             'date': 'Order Date',
    #             'quantity': 'Total Quantity',
    #             'rate': 'Average Rate',
    #             'amount': 'Total Amount'
    #         })
            
    #         # Add inventory columns if they exist
    #         if 'Available Stock' in sales_orders_df.columns:
    #             inventory_data = sales_orders_df.groupby(
    #                 ['item_id']
    #             ).agg({
    #                 'Available Stock': 'first',
    #                 'Stock on Hand': 'first'
    #             }).reset_index()
                
    #             renamed_df = pd.merge(
    #                 left=renamed_df,
    #                 right=inventory_data,
    #                 how='left',
    #                 on=['item_id']
    #             )
            
    #         return renamed_df
        
    #     return pd.DataFrame()
        
    # except Exception as e:
    #     logger.error(f"Error fetching Aza sales orders: {str(e)}")
    #     return pd.DataFrame()

def analyze_missing_aza_salesorders(aza_orders, product_mapping, sales_orders, sku_field="SKU"):
    """
    Analyze which Aza products need sales orders.
    
    Args:
        aza_orders (DataFrame): Aza order data
        product_mapping (dict): SKU to item_id mapping
        sales_orders (DataFrame): Existing sales orders data
        sku_field (str): Field name for SKUs in aza_orders DataFrame
    
    Returns:
        DataFrame: Missing sales order items with their details
    """
    try:
        # Create processor instance
        zakya_connection = {
            'base_url': st.session_state['api_domain'],
            'access_token': st.session_state['access_token'],
            'organization_id': st.session_state['organization_id']
        }
        
        processor = AzaInvoiceProcessor(
            aza_orders, 
            datetime.now(),
            zakya_connection,
            st.session_state['selected_customer']
        )
        
        # Store the product mapping in the processor
        processor.aza_product_mapping = product_mapping
        
        # Call the processor method
        missing_orders = asyncio.run(processor.analyze_missing_aza_salesorders(product_mapping, sales_orders))
        
        return missing_orders
    except Exception as e:
        logger.error(f"Error analyzing missing Aza sales orders: {str(e)}")
        return pd.DataFrame()

def create_missing_aza_salesorders(missing_orders, zakya_connection, customer_id, sku_field="SKU"):
    """
    Create missing sales orders for Aza items.
    
    Args:
        missing_orders (DataFrame): Missing sales order items
        zakya_connection (dict): Zakya API connection details
        customer_id (str): Customer ID in Zakya
        sku_field (str): Field name for SKUs in the dataframe
    
    Returns:
        dict: Results of the operation
    """
    try:
        # Create processor instance with placeholder sales_df
        processor = AzaInvoiceProcessor(
            missing_orders, 
            datetime.now(),
            zakya_connection,
            "Aza Customer"  # Placeholder, not used in this context
        )
        
        # Call the processor method
        results = asyncio.run(processor.create_missing_aza_salesorders(missing_orders, customer_id))
        
        return results
    except Exception as e:
        logger.error(f"Error creating missing Aza sales orders: {str(e)}")
        return {
            'success': False,
            'created_count': 0,
            'errors': [str(e)],
            'details': []
        }

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

def fetch_aza_inventory_data(zakya_connection, product_mapping):
    """
    Fetch inventory data for mapped Aza products.
    
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
        logger.error(f"Error fetching inventory data: {str(e)}")
        return {}

def check_aza_invoice_readiness(aza_orders, product_mapping, sales_orders, missing_orders):
    """
    Verify if all Aza products are ready for invoicing.
    
    Args:
        aza_orders (DataFrame): Aza order data
        product_mapping (dict): Mapping of SKUs to item IDs
        sales_orders (DataFrame): Existing sales orders
        missing_orders (DataFrame): Missing sales orders analysis
    
    Returns:
        dict: Dictionary with readiness status and any blocking issues
    """
    readiness = {
        'is_ready': False,
        'issues': []
    }
    
    # Check if we have product mapping
    if not product_mapping:
        readiness['issues'].append("Product mapping is missing. Please analyze product mapping first.")
        return readiness
    
    # Check if there are unmapped products that are required for invoicing
    if aza_orders is not None and not aza_orders.empty:
        unmapped_skus = []
        for _, row in aza_orders.iterrows():
            sku = row.get('SKU', '').strip()
            if sku and sku not in product_mapping:
                unmapped_skus.append(sku)
        
        if unmapped_skus:
            readiness['issues'].append(f"Found {len(unmapped_skus)} products not mapped in Zakya.")
    
    # Check if we have sales orders
    if sales_orders is None or sales_orders.empty:
        readiness['issues'].append("Sales orders not fetched. Please fetch sales orders first.")
        return readiness
    
    # Check if there are missing sales orders
    if missing_orders is not None and not missing_orders.empty:
        readiness['issues'].append(f"Found {len(missing_orders)} products without valid sales orders.")
    
    # Ready if no issues found
    if not readiness['issues']:
        readiness['is_ready'] = True
    
    return readiness