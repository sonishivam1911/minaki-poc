import asyncio
import streamlit as st
import pandas as pd
from config.logger import logger
from utils.zakya_api import fetch_records_from_zakya, fetch_object_for_each_id
from utils.postgres_connector import crud
from core.helper_zakya import extract_record_list


def fetch_all_salesorder_and_mapping_records_from_database():
    existing_mappings_df = crud.read_table('zakya_salesorder_line_item_mapping')
    logger.info(f"Found {len(existing_mappings_df)} existing sales order mappings")
    
    # Get list of salesorder IDs that already have mappings
    if not existing_mappings_df.empty and 'salesorder_id' in existing_mappings_df.columns:
        existing_salesorder_ids = existing_mappings_df['salesorder_id'].unique().tolist()
    else:
        existing_salesorder_ids = []
    
    # Step 2: Fetch all sales orders from Zakya
    sales_orders_data = fetch_records_from_zakya(
        st.session_state['api_domain'],
        st.session_state['access_token'],
        st.session_state['organization_id'],
        '/salesorders'
    )
        
    all_orders = extract_record_list(sales_orders_data, "salesorders")
    all_orders_df = pd.DataFrame(all_orders)
    # logger.info(f"Found {len(all_orders_df)} total sales orders in Zakya")
    
    # Step 3: Identify sales orders that need mapping
    new_orders_df = all_orders_df[~all_orders_df['salesorder_id'].isin(existing_salesorder_ids)]
    # logger.info(f"Found {len(new_orders_df)} sales orders that need mapping")
    
    if new_orders_df.empty:
        return pd.DataFrame(),  existing_mappings_df   

    return new_orders_df , existing_mappings_df


async def fetch_missing_salesorder_details(new_orders_df):
    # Convert to list of dictionaries for processing
    new_orders_records = new_orders_df.to_dict('records')
    
    # Set batch size for processing
    batch_size = 20
    
    # Create a semaphore to limit concurrency
    semaphore = asyncio.Semaphore(batch_size)
    
    # Create tasks for processing sales orders in batches
    new_mapping_data = []
    
    # Process orders in batches
    for i in range(0, len(new_orders_records), batch_size):
        batch = new_orders_records[i:i+batch_size]
        
        progress_msg = f"Processing batch {i//batch_size + 1} of {(len(new_orders_records) + batch_size - 1) // batch_size}"
        logger.info(progress_msg)
        
        # Create tasks for each sales order in the batch
        tasks = []
        for order in batch:
            order_id = order.get('salesorder_id')
            if not order_id:
                continue
                
            async def fetch_with_semaphore():
                async with semaphore:
                    return await asyncio.to_thread(
                        fetch_object_for_each_id,
                        st.session_state['api_domain'],
                        st.session_state['access_token'],
                        st.session_state['organization_id'],
                        f'salesorders/{order_id}'
                    )
            
            tasks.append((order_id, asyncio.create_task(fetch_with_semaphore())))
        
        # Wait for all tasks in this batch to complete
        for order_id, task in tasks:
            try:
                details = await task
                
                # Extract line items from the response
                if 'salesorder' in details and 'line_items' in details['salesorder']:
                    line_items = details['salesorder']['line_items']
                elif 'line_items' in details:
                    line_items = details['line_items']
                else:
                    logger.debug(f"No line items found in salesorder {order_id}")
                    continue
                
                # Process each line item
                for line_item in line_items:
                    mapping_record = {
                        'salesorder_id': order_id,
                        'salesorder_number': details.get('salesorder', {}).get('salesorder_number', ''),
                        'line_item_id': line_item.get('line_item_id', ''),
                        'item_id': line_item.get('item_id', ''),
                        'item_name': line_item.get('name', ''),
                        'quantity': line_item.get('quantity', 0),
                        'rate': line_item.get('rate', 0),
                        'amount': line_item.get('item_total', 0),
                    }
                    new_mapping_data.append(mapping_record)
                    
            except Exception as e:
                logger.error(f"Error processing salesorder {order_id}: {str(e)}")  

        return new_mapping_data

def save_new_mappings_to_database(new_mapping_data,existing_mappings_df):
    new_mappings_df = pd.DataFrame.from_records(new_mapping_data)
    
    # Step 5: Save new mappings to database
    if not new_mappings_df.empty:
        if existing_mappings_df.empty:
            # If no existing mappings, create a new table
            crud.create_table('zakya_salesorder_line_item_mapping', new_mappings_df)
        else:
            # Append new mappings to existing table
            all_mappings_df = pd.concat([existing_mappings_df, new_mappings_df], ignore_index=True)
            crud.create_table('zakya_salesorder_line_item_mapping', all_mappings_df)
        
        logger.info(f"Added {len(new_mappings_df)} new sales order mappings to database")
        
        # Return combined mappings
        return pd.concat([existing_mappings_df, new_mappings_df], ignore_index=True)
    else:
        return existing_mappings_df    


async def sync_salesorder_mappings():
    """
    Synchronize sales order mappings by checking for missing mappings 
    and creating them as needed.
    """
    try:
        # Step 1: Get existing mappings from database
        new_orders_df, existing_mappings_df =fetch_all_salesorder_and_mapping_records_from_database()
        
        # Step 4: Process new sales orders to create mappings
        if not new_orders_df.empty:

            new_mapping_data = await fetch_missing_salesorder_details(new_orders_df)
            
            # Create DataFrame from new mapping data
            if new_mapping_data:
                return save_new_mappings_to_database(new_mapping_data,existing_mappings_df)

            else:
                return existing_mappings_df
        else:
            return existing_mappings_df
                
    except Exception as e:
        logger.error(f"Error in sync_salesorder_mappings: {str(e)}")
        return pd.DataFrame()

# Function to run the async task from Streamlit
def sync_salesorder_mappings_sync():
    """Wrapper to run the async sync_salesorder_mappings function"""
    return asyncio.run(sync_salesorder_mappings())