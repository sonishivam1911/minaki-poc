import requests
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

BASE_URL = "https://www.zohoapis.com/inventory/v1"  # Replace with actual Zakya API base URL

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
        "scope": "ZohoInventory.FullAccess.all", 
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
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

    print(f'data is : {payload}')
    response = requests.post(TOKEN_URL, data=payload)
    print(f"reponse is : {response.json()}")
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


def fetch_organizations(base_url,access_token):
    """
    Fetch organizations from Zoho Inventory API.
    """
    url = f"{base_url}/inventory/v1/organizations"
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Content-Type": "application/json"
    }
    
    try:
        print(f"access token is {access_token}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an error for HTTP codes 4xx/5xx
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Exception is : {e}")
        return None
    