import asyncio
import streamlit as st
from config.logger import logger
import pandas as pd
from utils.zakya_api import (fetch_object_for_each_id, fetch_records_from_zakya)
from utils.postgres_connector import crud

def extract_record_list(input_data, key):
    records = []
    for record in input_data:
        records.extend(record[f'{key}'])
    return records

async def fetch_records_from_zakya_in_df_format(endpoint):
    object_data = fetch_records_from_zakya(
        st.session_state['api_domain'],
        st.session_state['access_token'],
        st.session_state['organization_id'],
        f'/{endpoint}'                  
    )
    object_data = extract_record_list(object_data, f"{endpoint}")
    object_data = pd.DataFrame.from_records(object_data)
    return object_data

async def fetch_details_with_semaphore(semaphore, api_domain, access_token, org_id, endpoint, item_id):
    """Fetch details for a single item with semaphore for rate limiting"""
    async with semaphore:
        # Use asyncio.to_thread to run the synchronous API call in a separate thread
        return await asyncio.to_thread(
            fetch_object_for_each_id,
            api_domain,
            access_token,
            org_id,
            f'{endpoint}/{item_id}'
        )

async def process_items_in_batches(items, endpoint, batch_size=20):
    """Process items in batches with concurrency control"""
    mapping_data = []
    
    # Create a semaphore to limit concurrency
    semaphore = asyncio.Semaphore(batch_size)
    
    # Process items in batches
    for i in range(0, len(items), batch_size):
        batch = items[i:i+batch_size]
        
        logger.info(f"Processing batch {i//batch_size + 1} of {(len(items) + batch_size - 1) // batch_size} for {endpoint}")
        
        # Create tasks for each item in the batch
        tasks = []
        for item in batch:
            item_id = item.get(f'{endpoint[:-1]}_id')  # invoice_id or salesorder_id
            if not item_id:
                continue
                
            task = fetch_details_with_semaphore(
                semaphore,
                st.session_state['api_domain'],
                st.session_state['access_token'],
                st.session_state['organization_id'],
                endpoint,
                item_id
            )
            tasks.append((item_id, task))
        
        # Wait for all tasks in this batch to complete
        for item_id, task in tasks:
            try:
                details = await task
                
                # Extract line items from the response
                if f'{endpoint[:-1]}' in details and 'line_items' in details[f'{endpoint[:-1]}']:
                    line_items = details[f'{endpoint[:-1]}']['line_items']
                elif 'line_items' in details:
                    line_items = details['line_items']
                else:
                    # Skip if no line items are found
                    #logger.debug(f"No line items found in {endpoint} {item_id}")
                    continue
                
                # Process each line item
                for line_item in line_items:
                    mapping_record = {
                        f'{endpoint[:-1]}_id': item_id,
                        f'{endpoint[:-1]}_number': details.get(f'{endpoint[:-1]}', {}).get(f'{endpoint[:-1]}_number', ''),
                        'line_item_id': line_item.get('line_item_id', ''),
                        'item_id': line_item.get('item_id', ''),
                        'item_name': line_item.get('name', ''),
                        'quantity': line_item.get('quantity', 0),
                        'rate': line_item.get('rate', 0),
                        'amount': line_item.get('item_total', 0),
                    }
                    mapping_data.append(mapping_record)
                    
            except Exception as e:
                logger.error(f"Error processing {endpoint} {item_id}: {str(e)}")
    
    return mapping_data

async def create_invoice_mapping_async(batch_size=20):
    """Create invoice mapping with async processing"""
    try:
        # Fetch all invoices
        logger.info("Fetching invoices from Zakya")
        invoice_df = await fetch_records_from_zakya_in_df_format("invoices")
        
        if invoice_df.empty:
            logger.warning("No invoices found")
            return pd.DataFrame()
            
        logger.info(f"Found {len(invoice_df)} invoices to process")
        
        # Convert DataFrame to list of dictionaries for processing
        invoice_records = invoice_df.to_dict('records')
        
        # Process invoices in batches
        mapping_data = await process_items_in_batches(invoice_records, "invoices", batch_size)
        
        # Create DataFrame from mapping data
        invoicing_mapping_df = pd.DataFrame.from_records(mapping_data)
        
        if not invoicing_mapping_df.empty:
            # Save to database
            crud.create_table('zakya_invoice_line_item_mapping', invoicing_mapping_df)
            logger.info(f"Saved {len(invoicing_mapping_df)} invoice line item mappings to database")
        else:
            logger.warning("No invoice line items found to save")
            
        return invoicing_mapping_df
        
    except Exception as e:
        logger.error(f"Error in create_invoice_mapping_async: {str(e)}")
        return pd.DataFrame()

async def create_salesorder_mapping_async(batch_size=20):
    """Create sales order mapping with async processing"""
    try:
        # Fetch all sales orders
        logger.info("Fetching sales orders from Zakya")
        salesorder_df = await fetch_records_from_zakya_in_df_format("salesorders")
        
        if salesorder_df.empty:
            logger.warning("No sales orders found")
            return pd.DataFrame()
            
        logger.info(f"Found {len(salesorder_df)} sales orders to process")
        
        # Convert DataFrame to list of dictionaries for processing
        salesorder_records = salesorder_df.to_dict('records')
        
        # Process sales orders in batches
        mapping_data = await process_items_in_batches(salesorder_records, "salesorders", batch_size)
        
        # Create DataFrame from mapping data
        salesorder_mapping_df = pd.DataFrame.from_records(mapping_data)
        
        if not salesorder_mapping_df.empty:
            # Save to database
            crud.create_table('zakya_salesorder_line_item_mapping', salesorder_mapping_df)
            logger.info(f"Saved {len(salesorder_mapping_df)} sales order line item mappings to database")
        else:
            logger.warning("No sales order line items found to save")
            
        return salesorder_mapping_df
        
    except Exception as e:
        logger.error(f"Error in create_salesorder_mapping_async: {str(e)}")
        return pd.DataFrame()

# Function to run async tasks that will be called from Streamlit
def run_async_task(task_func, *args, **kwargs):
    """Run an async task from Streamlit"""
    return asyncio.run(task_func(*args, **kwargs))