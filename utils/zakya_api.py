import requests
import os
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

BASE_URL = "https://api.zakya.in/inventory/v1"  # Replace with actual Zakya API base URL

# Environment variables for authentication
CLIENT_ID = os.getenv("ZAKYA_CLIENT_ID")
CLIENT_SECRET = os.getenv("ZAKYA_CLIENT_SECRET")
REDIRECT_URI = os.getenv("ZAKYA_REDIRECT_URI")
print(f"re direct url is : {REDIRECT_URI}")
TOKEN_URL = "https://accounts.zoho.in/oauth/v2/token"

def get_authorization_url():
    """
    Generate the authorization URL for Zakya login.
    """
    params = {
        "scope": "ZakyaAPI.fullaccess.all", 
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "access_type": "offline",
        "prompt": "consent"
    }
    params_list=[]
    for key,value in params.items():
        params_list.append(f'{key}={value}')

    params_url = "&".join(params_list)
    auth_url = f"https://accounts.zoho.com/oauth/v2/auth?{params_url}"
    print(f"auth url is : {auth_url}")

    return auth_url

def get_access_token(auth_code=None, refresh_token=None):
    """
    Fetch or refresh the access token from Zakya.
    """
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI,
    }
    if auth_code:
        payload["grant_type"] = "authorization_code"
        payload["code"] = auth_code
    elif refresh_token:
        payload["grant_type"] = "refresh_token"
        payload["refresh_token"] = refresh_token
    else:
        raise ValueError("Either auth_code or refresh_token must be provided.")

    print(f'url is {TOKEN_URL} and data is : {payload}')
    response = requests.post(TOKEN_URL, data=payload)
    response.raise_for_status()
    return response.json()

def fetch_contacts(base_url,access_token,organization_id):
    """
    Fetch inventory items from Zakya API.
    """
    endpoint = "/contacts"
    url = f"{base_url}/inventory/v1{endpoint}"
    
    headers = {
        'Authorization': f"Zoho-oauthtoken {access_token}",
        'Content-Type': 'application/json'
    }
    
    params = {
        'organization_id': organization_id
    }
        
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}",}
    print(f"headers is {headers}")
    response = requests.get(
        url=url,
        headers=headers,
        params=params
    )
    print(f"Response is {response}")
    response.raise_for_status()
    return response.json()


def fetch_records_from_zakya(base_url,access_token,organization_id,endpoint):
    """
    Fetch inventory items from Zakya API.
    """
    url = f"{base_url}inventory/v1{endpoint}"  
    headers = {
        'Authorization': f"Zoho-oauthtoken {access_token}",
        'Content-Type': 'application/json'
    }  
    params = {
        'organization_id': organization_id,
        'page' : 1,
        'per_page' : 200
    }
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}",}
    all_data=[]
    while True:
        response = requests.get(
            url=url,
            headers=headers,
            params=params
        )
        response.raise_for_status()
        data = response.json()                      
        page_context = data.get('page_context',{})
        all_data.append(data)

        if not page_context['has_more_page']:
            return all_data
            
        params['page'] = page_context['page'] + 1

    return []
    
def retrieve_record_from_zakya(base_url,access_token,organization_id,endpoint):
    """
    Fetch inventory items from Zakya API.
    """
    url = f"{base_url}inventory/v1/{endpoint}"  
    headers = {
        'Authorization': f"Zoho-oauthtoken {access_token}",
        'Content-Type': 'application/json'
    }  
    params = {
        'organization_id': organization_id
    }
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}",}
    response = requests.get(
            url=url,
            headers=headers,
            params=params
        )
    response.raise_for_status()
    data = response.json()                      
    
    return data


def extract_record_list(input_data,key):
    records = []
    for record in input_data:
        records.extend(record[f'{key}'])
    return records



def fetch_organizations(access_token):
    """
    Fetch organizations from Zoho Inventory API.
    """
    url = f"https://api.zakya.in/inventory/v1/organizations"
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an error for HTTP codes 4xx/5xx
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Exception is : {e}")
        return None
    

def fetch_object_for_each_id(base_url,access_token,organization_id,endpoint):
    """
    Fetch organizations from Zoho Inventory API.
    """
    url = f"{base_url}inventory/v1/{endpoint}"
    
    headers = {
        'Authorization': f"Zoho-oauthtoken {access_token}",
        'Content-Type': 'application/json'
    }

    params = {
        'organization_id': organization_id
    }    
    
    try:
        response = requests.get(url, headers=headers,params=params)
        response.raise_for_status()  # Raise an error for HTTP codes 4xx/5xx
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Exception is : {e}")
        return None


def post_record_to_zakya(base_url, access_token, organization_id, endpoint, payload, extra_args = {}):
    """
    Send a POST request to Zakya API to create a new record.
    
    :param base_url: Base URL of the Zakya API.
    :param access_token: OAuth access token for authentication.
    :param organization_id: ID of the organization in Zakya.
    :param endpoint: API endpoint for the request (e.g., "/invoices").
    :param payload: Dictionary containing the data to be sent in the request.
    :return: JSON response from the API.
    """
    url = f"{base_url}inventory/v1/{endpoint}"
    
    headers = {
        'Authorization': f"Zoho-oauthtoken {access_token}",
        'Content-Type': 'application/json'
    }

    params = {
        'organization_id': organization_id,
    }

    if "salesorders" in endpoint:
        params['ignore_auto_number_generation'] = True
    elif "invoices" in endpoint:
        params['ignore_auto_number_generation'] = True
    elif "packages" in endpoint and 'salesorder_id' in extra_args:
        params['salesorder_id'] = extra_args['salesorder_id']
    elif "shipmentorders" in endpoint and 'salesorder_id' in extra_args:
        params['salesorder_id'] = extra_args['salesorder_id']

    response = requests.post(
        url=url,
        headers=headers,
        params=params,
        json=payload
    )
    print(response.text)
    response.raise_for_status()  # Raise an error for bad responses
    return response.json() 


def put_record_to_zakya(base_url, access_token, organization_id, endpoint, txn_id, payload):
    url = f"{base_url}inventory/v1/{endpoint}/{txn_id}"
    
    headers = {
        'Authorization': f"Zoho-oauthtoken {access_token}",
        'Content-Type': 'application/json'
    }

    params = {
        'organization_id': organization_id,
    }

    response = requests.put(
        url=url,
        headers=headers,
        params=params,
        json=payload
    )
    print(response.text)
    response.raise_for_status()  # Raise an error for bad responses
    return response.json() 


