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
    auth_data=shiprocket_auth()
    logger.debug(f"auth_data: {auth_data}")
    # courier_data=list_couriers(auth_data['token'])
    # courier_df = pd.DataFrame.from_records(courier_data['courier_data'])
    # logger.debug(f"courier_list: {courier_df.columns}")
    logger.debug(f"Pincode: {sales_order_item_detail["shipping_address"]["zip"]}")
    check_service_data=check_service(auth_data['token'],'110021',sales_order_item_detail["shipping_address"]["zip"],weight)
    available_courier_companies_df = pd.DataFrame.from_records(check_service_data['data']['available_courier_companies'])
    logger.debug(f"available_courier_companies_df: {available_courier_companies_df.columns}")
    return available_courier_companies_df
    # create_sr_forward()


def create_shiprocket_sr_forward(config):
    # fetch sales order items from zakya
    sales_order_item_detail = fetch_object_for_each_id(
        st.session_state['api_domain'],
        st.session_state['access_token'],
        st.session_state['organization_id'],
        f'/salesorders/{config['salesorder_id']}'
    )
    logger.debug(f"sales_order_item_detail: {sales_order_item_detail}")
    sales_order_item_detail = sales_order_item_detail['salesorder']
    shiprocket_forward_order = create_sr_forward(
        token=st.session_state['token'],
        order_data=sales_order_item_detail,
        length=config['length'],
        weight=config['weight'],
        courier_id=st.session_state['selected_courier'],
    ) 
    logger.debug(f"Shiprocket result after calling sr forward function : {shiprocket_forward_order}")
    return shiprocket_forward_order