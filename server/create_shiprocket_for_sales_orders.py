import streamlit as st
import datetime
import pandas as pd
import datetime
import time
from utils.postgres_connector import crud
from config.logger import logger
from utils.zakya_api import fetch_object_for_each_id, post_record_to_zakya, fetch_records_from_zakya
from core.helper_zakya import fetch_records_from_zakya_in_df_format
from utils.bhavvam.shiprocket import (shiprocket_auth
                                      ,check_service
                                      , create_sr_forward
                                      , generate_manifest
                                      , generate_label
                                      , list_orders
                                      , list_shipments,
                                      fetch_all_return_orders
                                    )

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
    sales_order_item_detail = sales_order_item_detail['salesorder']
    # ask for contact email and number from the user 
    auth_data=shiprocket_auth()
    st.session_state['sr_token'] = auth_data['token']
    check_service_data=check_service(st.session_state['sr_token'],'110021',sales_order_item_detail["shipping_address"]["zip"],weight)
    available_courier_companies_df = pd.DataFrame.from_records(check_service_data['data']['available_courier_companies'])
    return available_courier_companies_df,sales_order_item_detail['contact_persons']


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
    ##logger.debug(f"sales_order_item_detail keys: {sales_order_item_detail.keys()}")
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
        "generate_label": True,
        "generate_manifest": True,
        "pickup_location": "warehouse",  # Default value
        "request_pickup": True,          # Default value
        "ewaybill_no": None              # Default value
    }
    
    # Add contact person if provided
    if 'contact_person' in config and config['contact_person']:
        sr_params["contact_person"] = config['contact_person']
        ##logger.debug(f"Using provided contact person: {config['contact_person']}")
    
    # Call the updated function with the parameters dictionary
    shiprocket_forward_order = create_sr_forward(sr_params)
    ##logger.debug(f"Shiprocket result after calling sr forward function: {shiprocket_forward_order}")
    status, message = save_shipment_to_database(shiprocket_result=shiprocket_forward_order,sales_order_details=sales_order_item_detail)

    # create package payload
    # zakya_packages_result = create_packages_on_zakya(sales_order_item_detail, shiprocket_forward_order)

    # create shipment
    # zakya_shipment_result = create_zakya_shipment_order(shiprocket_forward_order,extra_args)
    return shiprocket_forward_order, None, None, {'status' : status, 'message' : message}


def create_packages_on_zakya(sales_order_item_detail):
    # Prepare line items
    line_items = []
    for obj in sales_order_item_detail['salesorder']['line_items']:
        item_id = obj['item_id']  # Directly using item_id
        quantity = obj['quantity']

        # Fetch item details including stock availability
        item_details = fetch_object_for_each_id(
            st.session_state['api_domain'],
            st.session_state['access_token'],
            st.session_state['organization_id'],
            f'/items/{item_id}')
        
        # Extract the correct warehouse stock
        warehouses = item_details['item'].get('warehouses', [])
        warehouse_stock = next(
            (wh['warehouse_actual_available_for_sale_stock'] for wh in warehouses if wh['warehouse_id'] == "1923531000001452123"),
            0  # Default to 0 if not found
        )

        # Check if available stock is sufficient
        if warehouse_stock < quantity:
            quantity_adjusted = quantity - warehouse_stock
            inv_payload = {
                "date": str(datetime.datetime.now().strftime("%Y-%m-%d")),
                "reason": "Stock Retally",
                "adjustment_type": "quantity",
                "line_items": [
                    {
                        "item_id": item_id,
                        "quantity_adjusted": quantity_adjusted
                    }
                ]
            }

            inventory_correction_response = post_record_to_zakya(
                st.session_state['api_domain'],
                st.session_state['access_token'],
                st.session_state['organization_id'],
                'inventoryadjustments',
                inv_payload   
            )

            # Ensure inventory correction was successful before proceeding
            if inventory_correction_response.status_code != 200:
                raise ValueError(f"Inventory correction failed for item {item_id}")

            # Wait briefly to allow inventory update to reflect
            time.sleep(2)

        # Append to line items for package creation
        line_items.append({
            'so_line_item_id': obj['line_item_id'],
            'quantity': quantity
        })

    # Construct package payload
    package_payload = {
        "date": str(datetime.datetime.now().strftime("%Y-%m-%d")),
        "line_items": line_items,
        "notes": "Shiprocket result "
    }

    extra_args = {
        'salesorder_id': sales_order_item_detail['salesorder']['salesorder_id']
    }
    ##logger.debug(f'payload for packages is : {package_payload}')
    zakya_packages_result = post_record_to_zakya(
        st.session_state['api_domain'],
        st.session_state['access_token'],
        st.session_state['organization_id'],
        'packages',
        package_payload,
        extra_args    
    )

    return zakya_packages_result



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
        #logger.error("Invalid Shiprocket result")
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
        "shipment_number": payload.get('shipment_id', ""),
        "reference_number": payload.get('awb_code', ""),
        "delivery_method": payload.get('courier_name', ""),
        "tracking_number": payload.get('awb_code', ""),
        "shipping_charge": st.session_state['shipping_rate'],
        "notes": f"Shiprocket Order ID: {payload.get('order_id', '')}, Channel Order: {payload.get('channel_order_id', '')}"
    }
    
    ##logger.debug(f"Zakya Shipment API payload: {zakya_payload}")
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

    #logger.debug(f"Generate Manifest Results : {generate_manifest_result}")

    return generate_manifest_result


def generate_label_service(config):

    generate_label_result = generate_label(
        config['token'],
        config['shipment_ids']
    )

    ##logger.debug(f"Generate Label Results : {generate_label_result}")

    return generate_label_result


def save_shipment_to_database(shiprocket_result, sales_order_details):
    """
    Save the shipment data to PostgreSQL database
    
    Parameters:
    - shiprocket_result: Dictionary containing Shiprocket API response
    - sales_order_details: Dictionary containing sales order details
    
    Returns:
    - Boolean indicating success or failure
    - Message describing the result
    """
    try:
        # Extract relevant data from Shiprocket result
        shiprocket_data = {}
        if shiprocket_result and 'status' in shiprocket_result and shiprocket_result['status'] == 1:
            payload = shiprocket_result.get('payload', {})
            shiprocket_data = {
                'shipment_id': payload.get('shipment_id'),
                'order_id': payload.get('order_id'),
                'awb_code': payload.get('awb_code'),
                'courier_name': payload.get('courier_name'),
                'pickup_scheduled_date': payload.get('pickup_scheduled_date'),
                'pickup_token_number': payload.get('pickup_token_number'),
                'routing_code': payload.get('routing_code'),
            }
        else:
            return False, "Invalid or unsuccessful Shiprocket response"

        # Extract sales order info
        sales_data = {
            'salesorder_id': sales_order_details.get('salesorder_id'),
            'salesorder_number': sales_order_details.get('salesorder_number'),
            'customer_name': sales_order_details.get('customer_name'),
            'shipping_city': sales_order_details.get('shipping_address', {}).get('city'),
            'shipping_pincode': sales_order_details.get('shipping_address', {}).get('zip'),
            'shipping_state': sales_order_details.get('shipping_address', {}).get('state'),
            'shipping_country': sales_order_details.get('shipping_address', {}).get('country'),
        }

        # Merge both dictionaries
        combined_data = {**sales_data, **shiprocket_data}
        combined_data['created_at'] = datetime.datetime.utcnow()

        # Save to database
        crud.insert_record('log.shiprocket_shipments', combined_data)

        return True, "Shipment data saved successfully"

    except Exception as e:
        logger.error(f"Error saving shipment to database: {e}")
        return False, str(e)
    

def flatten_order(order):
    # Flatten products
    if 'products' in order:
        # Take the first product if multiple exist
        product = order['products'][0] if order['products'] else {}
        order['product_name'] = product.get('name', '')
        order['product_quantity'] = product.get('quantity', '')
        order['product_price'] = product.get('price', '')
    
    # Flatten shipments
    if 'shipments' in order:
        # Take the first shipment if multiple exist
        shipment = order['shipments'][0] if order['shipments'] else {}
        order['courier'] = shipment.get('courier', '')
        order['awb'] = shipment.get('awb', '')
        order['shipping_status'] = shipment.get('status', '')
    
    # Flatten others dictionary
    if 'others' in order:
        others = order['others']
        order['billing_name'] = others.get('billing_name', '')
        order['billing_email'] = others.get('billing_email', '')
        order['billing_phone'] = others.get('billing_phone', '')
    
    # Remove nested dictionaries to prevent conversion issues
    order.pop('products', None)
    order.pop('shipments', None)
    order.pop('others', None)
    
    return order


def fetch_shiprocket_order_detail():
    auth_data=shiprocket_auth()
    st.session_state['token'] = auth_data['token']
    order_list_result=list_orders(token=st.session_state['token'])
    flattened_orders = [flatten_order(order) for order in order_list_result['data']]
    shipment_order_df = pd.DataFrame.from_records(flattened_orders)
    #logger.debug(f"shipment_order_df columns is : {shipment_order_df.columns}")

    return shipment_order_df


def flatten_shipments(shipments_data):
    # Extract the list of shipments from the dictionary
    shipments_list = shipments_data.get('data', [])
    
    # Prepare a list to store flattened shipments
    flattened_shipments = []
    
    for shipment in shipments_list:
        # Create a copy of the shipment to avoid modifying the original
        flat_shipment = shipment.copy()
        
        # Handle multiple products by combining them
        if shipment.get('products'):
            # Combine product names and skus
            product_names = [p['name'] for p in shipment['products']]
            product_skus = [p['sku'] for p in shipment['products']]
            product_quantities = [p['quantity'] for p in shipment['products']]
            
            flat_shipment['product_names'] = ' | '.join(product_names)
            flat_shipment['product_skus'] = ' | '.join(product_skus)
            flat_shipment['product_quantities'] = ' | '.join(map(str, product_quantities))
            
            # Remove original products list
            del flat_shipment['products']
        
        # Flatten charges if present
        if 'charges' in shipment:
            for key, value in shipment['charges'].items():
                flat_shipment[f'charge_{key}'] = value
            del flat_shipment['charges']
        
        flattened_shipments.append(flat_shipment)
    
    # Create DataFrame
    shipment_order_df = pd.DataFrame(flattened_shipments)
    
    return shipment_order_df


def fetch_shipment_details():
    
    if 'sr_token' not in st.session_state:
        auth_data=shiprocket_auth()
        st.session_state['sr_token'] = auth_data['token']
    
    list_shipment_result=list_shipments(token=st.session_state['sr_token'])
    list_shipment_result=flatten_shipments(list_shipment_result)
    #logger.debug(f"Shipment Listing Result - {list_shipment_result}")
    return list_shipment_result


def fetch_all_return_orders_service():
    
    if 'sr_token' not in st.session_state:
        auth_data=shiprocket_auth()
        st.session_state['sr_token'] = auth_data['token']
    
    all_return_orders_result=fetch_all_return_orders(token=st.session_state['sr_token'])
    #logger.debug(f"Return Orders Result - {all_return_orders_result}")
    return pd.DataFrame.from_records(all_return_orders_result['data'])
