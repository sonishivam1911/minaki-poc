import pandas as pd
from utils.zakya_api import fetch_records_from_zakya
from utils.postgres_connector import crud
from config.logger import logger
from queries.zakya import queries
from core.helper_zakya import extract_record_list
from utils.common_filtering_database_function import find_product

def fetch_salesorders_by_customer(config):
    """Fetch sales orders for a specific customer, with or without Pernia product filtering."""
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

        #logger.debug(f" Sales Order Df is : {len(sales_orders_data)}")
        
        # Filter to only include the selected customer
        sales_orders_df = sales_orders_df[sales_orders_df['customer_id'] == config['customer_id']]
        #logger.debug(f"Sales Order Df is : {(sales_orders_data)}")
        
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
                    item = find_product(sku)
                    items_data_result.extend(item)
            
            mapped_pernia_products_df = pd.DataFrame.from_records(items_data_result)
            mapped_pernia_products_df = mapped_pernia_products_df[['item_id']]
            
            # Filter to only include Pernia items
            mapped_sales_order_with_product_df = pd.merge(
                left=sales_orders_df, 
                right=mapped_pernia_products_df,
                how='inner',  # Only include matches
                on=['item_id']
            )
        else:
            # No Pernia filtering - include all items
            mapped_sales_order_with_product_df = sales_orders_df
        

        #logger.debug(f"Columns for tale :  {mapped_sales_order_with_product_df.columns}")
        # Group by salesorder, item name, and date, then calculate averages for metrics
        grouped_df = mapped_sales_order_with_product_df.groupby(
            ['salesorder_number_x', 'item_name', 'date']
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

        return renamed_df
        
    except Exception as e:
        logger.error(f"Error fetching sales orders: {str(e)}")
        return pd.DataFrame()
    


def fetch_product_metrics_for_sales_order_by_customer():
    try:
        product_analytics = crud.execute_query(queries.salesorder_product_metrics_query,True)
        return product_analytics
    except Exception as e:
        logger.debug(f"Product analytics query failed with error: {e}")

