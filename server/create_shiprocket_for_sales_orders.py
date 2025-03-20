import streamlit as st
import datetime
import json
from config.logger import logger
import pandas as pd
from utils.zakya_api import fetch_object_for_each_id, post_record_to_zakya
from core.helper_zakya import fetch_records_from_zakya_in_df_format
from utils.bhavvam.shiprocket import shiprocket_auth,list_couriers, check_service, create_sr_forward, generate_manifest, generate_label

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

    # create package payload
    package_payload = {
        # "package_number" : f"{sales_order_item_detail['salesorder_number']} package for {sales_order_item_detail['customer_name']}",
        "date" : str(datetime.datetime.now().strftime("%Y-%m-%d")),
        "line_items" : [ {
            'so_line_item_id' : obj['line_item_id'],
            'quantity' : obj['quantity']
        } for obj in sales_order_item_detail['line_items']],
        "notes" : f"Shiprocket result - {shiprocket_forward_order}"
    }
    extra_args = {
        'salesorder_id' : sales_order_item_detail['salesorder_id']
    }
    logger.debug(f'payload for packages is : {package_payload}')
    zakya_packages_result = post_record_to_zakya(
        st.session_state['api_domain'],
        st.session_state['access_token'],
        st.session_state['organization_id'],
        'packages',
        package_payload,
        extra_args    
    )

    # create shipment
    zakya_shipment_result = create_zakya_shipment_order(shiprocket_forward_order,extra_args)
    
    logger.debug(f"Shiprocket result after calling sr forward function: {shiprocket_forward_order}")
    return shiprocket_forward_order, zakya_shipment_result, zakya_packages_result


def create_zakya_shipment_order(shiprocket_result,extra_args):
    """
    Create a shipment order in Zakya API using Shiprocket result data
    
    Parameters:
    - shiprocket_result: Dictionary containing Shiprocket API response
    - contact_persons: ID of the contact person (defaults to sales order contact if None)
    - template_id: Template ID for the shipment (defaults to configuration if None)
    
    Returns:
    - Response from Zakya API
    """
    if not shiprocket_result or 'status' not in shiprocket_result or shiprocket_result['status'] != 1:
        logger.error("Invalid Shiprocket result")
        return {"error": "Invalid Shiprocket result"}
    
    # Extract payload
    payload = shiprocket_result.get('payload', {})
    
    # Get today's date if assigned date not available
    current_date = str(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))
    
    # Extract date from assigned_date_time if available
    assigned_date = None
    if 'assigned_date_time' in payload and payload['assigned_date_time'] and 'date' in payload['assigned_date_time']:
        assigned_date = payload['assigned_date_time']['date'].split(' ')[0]
    
    # Create payload for Zakya API
    zakya_payload = {
        "date": assigned_date or current_date,
        "reference_number": payload.get('awb_code', ""),
        "delivery_method": payload.get('courier_name', ""),
        "tracking_number": payload.get('awb_code', ""),
        # "shipping_charge": 7,  # Default value, could be retrieved from courier rate
        # "template_id": template_id or 4815000000017003,  # Default to example value if not provided
        "notes": f"Shiprocket Order ID: {payload.get('order_id', '')}, Channel Order: {payload.get('channel_order_id', '')}"
    }
    
    logger.debug(f"Zakya Shipment API payload: {zakya_payload}")
    result=post_record_to_zakya(
        st.session_state['api_domain'],
        st.session_state['access_token'],
        st.session_state['organization_id'],
        'shipmentorders',
        zakya_payload,
        extra_args   
    )

    return result



def generate_manifest_service(config):

    generate_manifest_result = generate_manifest(
        config['token'],
        config['shipment_ids']
    )

    return generate_manifest_result


def generate_label_service(config):

    generate_label_result = generate_label(
        config['token'],
        config['shipment_ids']
    )

    return generate_label_result

