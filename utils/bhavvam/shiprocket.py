import requests
import json
import os
import pandas as pd
from dotenv import load_dotenv

SR_EMAIL = os.getenv("SR_EMAIL")
SR_PASSWORD = os.getenv("SR_PASSWORD")


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

def create_sr_forward(token, order_data, length, breadth, height, weight,courier_id, pickup_location = "warehouse", ewaybill_no = None, request_pickup = True): 
    url = "https://apiv2.shiprocket.in/v1/external/shipments/create/forward-shipment"
    order_items = [
        {
            "name": item["name"],
            "sku": str(item["item_id"]),  # Converting to string for consistency
            "units": item["quantity"],
            "selling_price": str(item["rate"])  # Converting to string to match API requirements
        } 
        for item in order_data.get("line_items", [])
    ]
    payload = json.dumps({
    "order_id": order_data["salesorder_number"],
    "order_date": order_data["date"],
    "billing_customer_name": order_data["customer_name"],
    "billing_address": order_data["billing_address"][0]["address"],
    "billing_city": order_data["billing_address"][0]["city"],
    "billing_pincode": order_data["billing_address"][0]["zip"],
    "billing_state": order_data["billing_address"][0]["state"],
    "billing_country": order_data["billing_address"][0]["country"],
    "billing_email": order_data["contact_persons"][0]["email"],
    "billing_phone": order_data["contact_persons"][0]["phone"],
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
    response = requests.request("POST", url, headers=headers, data=payload)
    return response.text

def generate_pickup(token, shipment_id):
    url = "https://apiv2.shiprocket.in/v1/external/courier/generate/pickup"
    payload = json.dumps({"shipment_id": shipment_id})
    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {token}'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    return response.text

def generate_label(token, shipment_ids):
    url = "https://apiv2.shiprocket.in/v1/external/courier/generate/label"
    payload = json.dumps({"shipment_id": shipment_ids})
    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {token}'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    return response.text

def generate_manifest(token, shipment_ids):
    url = "https://apiv2.shiprocket.in/v1/external/manifests/generate"
    payload = json.dumps({"shipment_id": shipment_ids})
    headers = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {token}'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    return response.text
