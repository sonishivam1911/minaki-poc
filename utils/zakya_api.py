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
TOKEN_URL = "https://accounts.zoho.com/oauth/v2/token"

def get_authorization_url():
    """
    Generate the authorization URL for Zakya login.
    """
    params = {
        "response_type": "code",
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "scope": "ZohoInventory.FullAccess.all",  # Replace with appropriate scopes
        "access_type": "offline",
    }
    return f"https://accounts.zoho.com/oauth/v2/auth?{requests.compat.urlencode(params)}"

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

    response = requests.post(TOKEN_URL, data=payload)
    response.raise_for_status()
    return response.json()

def fetch_inventory_items(access_token):
    """
    Fetch inventory items from Zakya API.
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    print(f"headers is {headers}")
    response = requests.get(f"{BASE_URL}/inventory/items", headers=headers)
    print(f"Response is {response}")
    response.raise_for_status()
    return response.json()


def fetch_organizations(access_token):
    """
    Fetch organizations from Zoho Inventory API.
    """
    url = f"{BASE_URL}/organizations"
    headers = {
        "Authorization": f"authtoken {access_token}"
    }
    
    try:
        print(f"access token is {access_token}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an error for HTTP codes 4xx/5xx
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Exception is : {e}")
        return None
