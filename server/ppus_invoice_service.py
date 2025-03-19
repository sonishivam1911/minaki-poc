import re
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from utils.postgres_connector import crud
from config.logger import logger
from schema.zakya_schemas.schema import ZakyaProducts
from config.constants import products_mapping_zakya_products
from queries.zakya import queries
from utils.zakya_api import fetch_records_from_zakya
from core.helper_zakya import extract_record_list
# Load environment variables from .env file
load_dotenv()

def fetch_pernia_data_from_database(input):
    # Fetch all data from ppus_orders table
    pernia_data = crud.read_table('ppus_orders')
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

def create_whereclause_fetch_data(pydantic_model, filter_dict, query):
    """Fetch data using where clause asynchronously."""
    try:
        whereClause = crud.build_where_clause(pydantic_model, filter_dict)
        formatted_query = query.format(whereClause=whereClause)
        data = crud.execute_query(query=formatted_query, return_data=True)
        return data.to_dict('records')
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        return {"error": f"Error fetching data: {e}"}

def find_product(sku):
    """Find a product by SKU."""
    items_data = create_whereclause_fetch_data(ZakyaProducts, {
        products_mapping_zakya_products['style']: {'op': 'eq', 'value': sku}
    }, queries.fetch_prodouct_records)    
    return items_data


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
