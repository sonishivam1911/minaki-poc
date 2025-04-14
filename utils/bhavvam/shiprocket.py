import requests
import json
import os
import pandas as pd
import asyncio
import aiohttp
from typing import List, Dict
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

    #logger.debug(f"Order items are : {order_items}")

    # Log contact person information
    #logger.debug(f"Contact Person information is: {order_data.get('contact_persons', [])}")
    
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
        "print_label": True,
        "generate_manifest": True,
        "courier_id": courier_id,
        "ewaybill_no": ewaybill_no,
        "request_pickup": request_pickup
    })
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    #  parsed_data = json.loads(payload)
    # for key,value in parsed_data.items():
    #     #logger.debug(f"key is {key} and value is {value}")
    
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



def list_orders(token, **kwargs):
    """
    Retrieve a list of orders from Shiprocket
    
    Args:
        token (str): Authentication token from shiprocket_auth()
        **kwargs: Optional query parameters to filter orders
            Possible parameters include:
            - page (int): Page number for pagination
            - per_page (int): Number of orders per page
            - filter_by (str): Filter orders by status
            - sort_by (str): Sort orders
    
    Returns:
        dict: JSON response containing list of orders
    """
    url = "https://apiv2.shiprocket.in/v1/external/orders"
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    
    # Add query parameters if provided
    params = {}
    for key, value in kwargs.items():
        params[key] = value
    
    response = requests.request("GET", url, headers=headers, params=params)
    return response.json()

def get_order_details(token, order_id):
    """
    Retrieve details of a specific order from Shiprocket
    
    Args:
        token (str): Authentication token from shiprocket_auth()
        order_id (str or int): Unique identifier of the order
    
    Returns:
        dict: JSON response containing detailed order information
    """
    url = f"https://apiv2.shiprocket.in/v1/external/orders/show/{order_id}"
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    
    response = requests.request("GET", url, headers=headers)
    return response.json()

def list_shipments(token, **kwargs):
    """
    Retrieve a list of shipments from Shiprocket
    
    Args:
        token (str): Authentication token from shiprocket_auth()
        **kwargs: Optional query parameters to filter shipments
            Possible parameters include:
            - page (int): Page number for pagination
            - per_page (int): Number of shipments per page
            - filter_by (str): Filter shipments by status
            - sort_by (str): Sort shipments
    
    Returns:
        dict: JSON response containing list of shipments
    """
    url = "https://apiv2.shiprocket.in/v1/external/shipments"
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    
    # Add query parameters if provided
    params = {}
    for key, value in kwargs.items():
        params[key] = value
    
    response = requests.request("GET", url, headers=headers, params=params)
    return response.json()



def fetch_all_return_orders(token, **kwargs):
    """
    Retrieve a list of orders which are returned from Shiprocket
    
    Args:
        token (str): Authentication token from shiprocket_auth()
        **kwargs: Optional query parameters to filter orders
            Possible parameters include:
            - page (int): Page number for pagination
            - per_page (int): Number of orders per page
            - filter_by (str): Filter orders by status
            - sort_by (str): Sort orders
    
    Returns:
        dict: JSON response containing list of orders
    """
    url = "https://apiv2.shiprocket.in/v1/external/orders/processing/return"

    # return_all_orders_df =asyncio.run(fetch_all_pages_data(
    #     token=token,
    #     initial_url=url,
    # ))

    # return return_all_orders_df
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    
    # Add query parameters if provided
    params = {}
    for key, value in kwargs.items():
        params[key] = value
    
    response = requests.request("GET", url, headers=headers, params=params)
    return response.json()




async def fetch_page(session: aiohttp.ClientSession, 
                     url: str, 
                     token: str, 
                     semaphore: asyncio.Semaphore) -> Dict:
    """
    Async function to fetch a single page of orders
    
    Args:
        session: Aiohttp client session
        url: URL to fetch orders from
        token: Authentication token
        semaphore: Concurrency control semaphore
    
    Returns:
        Dictionary of order data for the page
    """
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    
    async with semaphore:
        try:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            print(f"Error fetching page {url}: {e}")
            return {}
        

async def fetch_all_pages_data(token: str, 
                            initial_url: str, 
                            max_concurrent_requests: int = 20) -> List[Dict]:
    """
    Async function to fetch all orders across all pages
    
    Args:
        token: Authentication token
        initial_url: Base URL for orders endpoint
        max_concurrent_requests: Maximum parallel requests
    
    Returns:
        List of all order records
    """
    # Create semaphore for controlled concurrency
    semaphore = asyncio.Semaphore(max_concurrent_requests)
    
    # First, get initial page to determine total pages
    initial_response = requests.get(
        initial_url, 
        headers={'Authorization': f'Bearer {token}'}
    ).json()
    
    # Extract pagination details
    meta = initial_response.get('meta', {}).get('pagination', {})
    total_pages = meta.get('total_pages', 1)
    per_page = meta.get('per_page', 50)

    #logger.debug(f"meta data is :{meta}")
    
    # Generate page URLs
    page_urls = [
        f"{initial_url}?page={page}" 
        for page in range(1, total_pages + 1)
    ]
    
    # Async session and fetch
    async with aiohttp.ClientSession() as session:
        # Parallel page fetching
        page_results = await asyncio.gather(
            *[fetch_page(session, url, token, semaphore) for url in page_urls]
        )
    
    # Aggregate all order records
    all_orders = []
    for result in page_results:
        if result and 'data' in result:
            all_orders.extend(result['data'])
    
    return all_orders        