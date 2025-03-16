import streamlit as st
from config.logger import logger
import pandas as pd
from utils.zakya_api import fetch_object_for_each_id
from core.helper_zakya import fetch_records_from_zakya_in_df_format
from utils.bhavvam.shiprocket import shiprocket_auth,list_couriers, check_service, create_sr_forward

def sales_order_id_number_mapping_dict():
    # fetch sales orders from zakya
    sales_orders_df = fetch_records_from_zakya_in_df_format('salesorders')
    sales_order_mapping = dict(zip(sales_orders_df['salesorder_id'], sales_orders_df['salesorder_number']))
    return sales_order_mapping


def create_shiprocket_for_sales_orders(salesorder_id,weight):

    # fetch sales order items from zakya
    sales_order_item_detail = fetch_object_for_each_id(
        st.session_state['api_domain'],
        st.session_state['access_token'],
        st.session_state['organization_id'],
        f'/salesorders/{salesorder_id}'
    )
    logger.debug(f"sales_order_item_detail: {sales_order_item_detail}")
    sales_order_item_detail = sales_order_item_detail['salesorder']
    logger.debug(f"{sales_order_item_detail['contact_persons']}")
    # ask for contact email and number from the user 
    auth_data=shiprocket_auth()
    st.session_state['token'] = auth_data['token']
    logger.debug(f"auth_data: {auth_data}")
    # courier_data=list_couriers(auth_data['token'])
    # courier_df = pd.DataFrame.from_records(courier_data['courier_data'])
    # logger.debug(f"courier_list: {courier_df.columns}")
    logger.debug(f"Pincode: {sales_order_item_detail["shipping_address"]["zip"]}")
    check_service_data=check_service(st.session_state['token'],'110021',sales_order_item_detail["shipping_address"]["zip"],weight)
    available_courier_companies_df = pd.DataFrame.from_records(check_service_data['data']['available_courier_companies'])
    logger.debug(f"available_courier_companies_df: {available_courier_companies_df.columns}")
    return available_courier_companies_df,sales_order_item_detail['contact_persons']
    # create_sr_forward()


def create_shiprocket_sr_forward(config):
    """
        Create a Shiprocket forward shipment for a sales order
        
        Parameters:
        - config: Dictionary containing:
            - salesorder_id: ID of the sales order
            - length, breadth, height: Package dimensions
            - weight: Package weight
            - contact_person: Optional contact person details
        
        Returns:
        - Response from Shiprocket API
        """
    # fetch sales order items from zakya
    sales_order_item_detail = fetch_object_for_each_id(
        st.session_state['api_domain'],
        st.session_state['access_token'],
        st.session_state['organization_id'],
        f'/salesorders/{config["salesorder_id"]}'
    )
    
    sales_order_item_detail = sales_order_item_detail['salesorder']
    logger.debug(f"sales_order_item_detail keys: {sales_order_item_detail.keys()}")
    auth_data=shiprocket_auth()
    # Prepare parameters dictionary for create_sr_forward
    sr_params = {
        "token": auth_data['token'],
        "order_data": sales_order_item_detail,
        "length": config['length'],
        "breadth": config['breadth'],
        "height": config['height'],
        "weight": config['weight'],
        "courier_id": config['courier_id'],
        "pickup_location": "warehouse",  # Default value
        "request_pickup": True,          # Default value
        "ewaybill_no": None              # Default value
    }
    
    # Add contact person if provided
    if 'contact_person' in config and config['contact_person']:
        sr_params["contact_person"] = config['contact_person']
        logger.debug(f"Using provided contact person: {config['contact_person']}")
    
    # Call the updated function with the parameters dictionary
    shiprocket_forward_order = create_sr_forward(sr_params)
    
    logger.debug(f"Shiprocket result after calling sr forward function: {shiprocket_forward_order}")
    return shiprocket_forward_order