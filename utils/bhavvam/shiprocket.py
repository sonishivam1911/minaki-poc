import requests
import json
import os
import pandas as pd
from dotenv import load_dotenv
from config.logger import logger

SR_EMAIL = os.getenv("SR_EMAIL")
SR_PASSWORD = os.getenv("SR_PASSWORD")


load_dotenv()

def shiprocket_auth():
    url = "https://apiv2.shiprocket.in/v1/external/auth/login"
    payload = json.dumps({
    "email": SR_EMAIL,
    "password": SR_PASSWORD
    })
    headers = {
    'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    return response.json()

def check_service(token, pickup_pincode, delivery_pincode, weight):
    url = f"https://apiv2.shiprocket.in/v1/external/courier/serviceability/?pickup_postcode={pickup_pincode}&delivery_postcode={delivery_pincode}&cod=0&weight={weight}&qc_check=0"
    payload={}
    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {token}'
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    return response.json()

def create_sr_forward(params):
    """
        Create a Shiprocket forward shipment using a dictionary of parameters
        
        Parameters:
        - params: Dictionary containing all the required parameters:
            - token: Shiprocket authentication token
            - order_data: Sales order data
            - length: Package length
            - breadth: Package breadth
            - height: Package height
            - weight: Package weight
            - courier_id: Selected courier ID
            - pickup_location: Pickup location name (default: "warehouse")
            - ewaybill_no: E-way bill number (default: None)
            - request_pickup: Whether to request pickup (default: True)
            - contact_person: Optional contact person details (default: None)
        
        Returns:
        - Response from Shiprocket API as JSON
    """     
    
    # Extract all parameters from the dictionary
    token = params.get("token")
    order_data = params.get("order_data")
    length = params.get("length")
    breadth = params.get("breadth")
    height = params.get("height")
    weight = params.get("weight")
    courier_id = params.get("courier_id")
    pickup_location = params.get("pickup_location", "warehouse")
    ewaybill_no = params.get("ewaybill_no")
    request_pickup = params.get("request_pickup", True)
    contact_person = params.get("contact_person")
    
    url = "https://apiv2.shiprocket.in/v1/external/shipments/create/forward-shipment"
    
    # Extract line items
    order_items = [
        {
            "name": item["name"],
            "sku": str(item["item_id"]),  # Converting to string for consistency
            "units": item["quantity"],
            "selling_price": str(item["rate"])  # Converting to string to match API requirements
        } 
        for item in order_data.get("line_items", [])
    ]

    # Log contact person information
    logger.debug(f"Contact Person information is: {order_data.get('contact_persons', [])}")
    
    # Use provided contact person or get from order data
    if contact_person:
        billing_name = contact_person.get("name")
        billing_last_name = contact_person.get("name").split(" ")[1]
        billing_email = contact_person.get("email")
        billing_phone = contact_person.get("phone")
    elif order_data.get("contact_persons") and len(order_data["contact_persons"]) > 0:
        billing_name = order_data["customer_name"]
        billing_last_name = order_data["customer_name"].split(" ")[1]
        billing_email = order_data["contact_persons"][0].get("email", "")
        billing_phone = order_data["contact_persons"][0].get("phone", "")
    else:
        billing_name = order_data["customer_name"]
        billing_last_name = ""
        billing_email = ""
        billing_phone = ""

    # Create payload
    payload = json.dumps({
        "order_id": order_data["salesorder_number"],
        "order_date": order_data["date"],
        "billing_customer_name": billing_name,
        "billing_last_name" : billing_last_name,
        "billing_address": order_data["billing_address"]["address"],
        "billing_city": order_data["billing_address"]["city"],
        "billing_pincode": order_data["billing_address"]["zip"],
        "billing_state": order_data["billing_address"]["state"],
        "billing_country": order_data["billing_address"]["country"],
        "billing_email": billing_email,
        "billing_phone": billing_phone,
        "shipping_is_billing": True,
        "order_items": order_items,
        "payment_method": "Prepaid",
        "sub_total": order_data["total"],
        "length": length,
        "breadth": breadth,
        "height": height,
        "weight": weight,
        "pickup_location": pickup_location,
        "print_label": False,
        "generate_manifest": False,
        "courier_id": courier_id,
        "ewaybill_no": ewaybill_no,
        "request_pickup": request_pickup
    })
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    parsed_data = json.loads(payload)
    for key,value in parsed_data.items():
        logger.debug(f"key is {key} and value is {value}")
    
    response = requests.request("POST", url, headers=headers, data=payload)
    return response.json()

def generate_pickup(token, shipment_id):
    url = "https://apiv2.shiprocket.in/v1/external/courier/generate/pickup"
    payload = json.dumps({"shipment_id": shipment_id})
    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {token}'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    return response.json()

def generate_label(token, shipment_ids):
    url = "https://apiv2.shiprocket.in/v1/external/courier/generate/label"
    payload = json.dumps({"shipment_id": shipment_ids})
    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {token}'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    return response.json()

def generate_manifest(token, shipment_ids):
    url = "https://apiv2.shiprocket.in/v1/external/manifests/generate"
    payload = json.dumps({"shipment_id": shipment_ids})
    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {token}'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    return response.json()


def list_couriers(token):
    """
    Get list of available couriers from Shiprocket
    
    Args:
        token (str): Authentication token from shiprocket_auth()
    
    Returns:
        dict: JSON response containing list of available couriers and their details
    """
    url = "https://apiv2.shiprocket.in/v1/external/courier/courierListWithCounts"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    response = requests.request("GET", url, headers=headers)
    return response.json()
