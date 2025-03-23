import re
import streamlit as st
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from utils.postgres_connector import crud
from config.logger import logger
from utils.common_filtering_database_function import find_product
from utils.zakya_api import fetch_records_from_zakya, post_record_to_zakya, fetch_object_for_each_id
from core.helper_zakya import extract_record_list
from server.invoice.route import AzaInvoiceProcessor
# Load environment variables from .env file
load_dotenv()

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