import requests
import os
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

BASE_URL = "https://api.zakya.com/inventory/v1"  # Replace with actual Zakya API base URL

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
        # print(data.keys())
        all_data.append(data)
        # print(all_data[0][endpoint])

        if not page_context['has_more_page']:
            return all_data
        
        params['page'] = page_context['page'] + 1
    return none


def extract_record_list(input_data,key):
    records = []
    for record in input_data:
        records.extend(record[f'{key}'])
    return records



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
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an error for HTTP codes 4xx/5xx
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Exception is : {e}")
        return None


def make_request(method,base_url,  endpoint, access_token, organization_id, data=None, params=None):
    """
    Makes an HTTP request to the Zakya API with pagination support.
    """
    url = f"{base_url}/inventory/v1/{endpoint}"  # Ensure correct API path
    print(f"Making {method} request to {url}...")

    

def fetch_object_for_each_id(base_url,access_token,organization_id,endpoint):
    """
    Fetch organizations from Zoho Inventory API.
    """
    url = f"{base_url}/inventory/v1{endpoint}"
    
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



# Item Group Functions
def create_item_group(base_url, access_token, organization_id, item_group_data):
    """Creates a new item group in Zakya."""
    print("Creating new item group...")
    return make_request("POST",base_url,  "itemgroups", access_token, organization_id, item_group_data)

def list_all_item_groups(base_url, access_token, organization_id):
    """Lists all item groups available in Zakya."""
    print("Listing all item groups...")
    return fetch_records_from_zakya(base_url,  access_token, organization_id, "itemgroups")

def fetch_item_group(base_url, access_token, organization_id, itemgroup_id):
    """Fetches details of a specific item group by its ID."""
    print(f"Fetching details for item group ID: {itemgroup_id}...")
    return make_request("GET",base_url,  f"itemgroups/{itemgroup_id}", access_token, organization_id)

def update_item_group(base_url, access_token, organization_id, itemgroup_id, update_data):
    """Updates an existing item group with new data."""
    print(f"Updating item group ID: {itemgroup_id}...")
    return make_request("PUT",base_url,  f"itemgroups/{itemgroup_id}", access_token, organization_id, update_data)

def delete_item_group(base_url, access_token, organization_id, itemgroup_id):
    """Deletes an item group from Zakya."""
    print(f"Deleting item group ID: {itemgroup_id}...")
    return make_request("DELETE",base_url,  f"itemgroups/{itemgroup_id}", access_token, organization_id)

def mark_item_group_active(base_url, access_token, organization_id, itemgroup_id):
    """Marks an item group as active."""
    print(f"Marking item group ID {itemgroup_id} as active...")
    return make_request("POST",base_url,  f"itemgroups/{itemgroup_id}/active", access_token, organization_id)

def mark_item_group_inactive(base_url, access_token, organization_id, itemgroup_id):
    """Marks an item group as inactive."""
    print(f"Marking item group ID {itemgroup_id} as inactive...")
    return make_request("POST",base_url,  f"itemgroups/{itemgroup_id}/inactive", access_token, organization_id)


# Item Functions
def create_item(base_url, access_token, organization_id, item_data):
    """Creates a new item in Zakya."""
    print("Creating new item...")
    return make_request("POST",base_url,  "items", access_token, organization_id, item_data)

def list_all_items(base_url, access_token, organization_id):
    """Lists all items in Zakya."""
    print("Listing all items...")
    return make_request("GET",base_url,  "items", access_token, organization_id)

def fetch_item(base_url, access_token, organization_id, item_id):
    """Fetches details of a specific item by its ID."""
    print(f"Fetching details for item ID: {item_id}...")
    return make_request("GET",base_url,  f"items/{item_id}", access_token, organization_id)

def fetch_bulk_items(base_url, access_token, organization_id, item_ids):
    """Fetches details of multiple items in bulk."""
    print(f"Fetching details for item IDs: {item_ids}...")
    params = {"organization_id": organization_id, "item_ids": ",".join(item_ids)}
    return make_request("GET",base_url,  "items/bulk", access_token, organization_id, params=params)

def update_item(base_url, access_token, organization_id, item_id, update_data):
    """Updates an existing item with new data."""
    print(f"Updating item ID: {item_id}...")
    return make_request("PUT",base_url,  f"items/{item_id}", access_token, organization_id, update_data)

def delete_item(base_url, access_token, organization_id, item_id):
    """Deletes an item from Zakya."""
    print(f"Deleting item ID: {item_id}...")
    return make_request("DELETE",base_url,  f"items/{item_id}", access_token, organization_id)

def mark_item_active(base_url, access_token, organization_id, item_id):
    """Marks an item as active."""
    print(f"Marking item ID {item_id} as active...")
    return make_request("POST",base_url,  f"items/{item_id}/active", access_token, organization_id)

def mark_item_inactive(base_url, access_token, organization_id, item_id):
    """Marks an item as inactive."""
    print(f"Marking item ID {item_id} as inactive...")
    return make_request("POST",base_url,  f"items/{item_id}/inactive", access_token, organization_id)

def adjust_item_stock(access_token,base_url,  organization_id, item_id, stock_data):
    """Adjusts the stock of an item."""
    print(f"Adjusting stock for item ID: {item_id}...")
    return make_request("POST", base_url,  f"items/{item_id}/adjustment", access_token, organization_id, stock_data)


# Sales Order Functions
def create_sales_order(base_url, access_token, organization_id, sales_order_data):
    """Creates a new sales order in Zakya."""
    print("Creating new sales order...")
    return make_request("POST",base_url,  "salesorders", access_token, organization_id, sales_order_data)

def list_all_sales_orders(base_url, access_token, organization_id):
    """Lists all sales orders available in Zakya."""
    print("Listing all sales orders...")
    return fetch_records_from_zakya(base_url, access_token, organization_id, "salesorders")

def fetch_sales_order(base_url, access_token, organization_id, salesorder_id):
    """Fetches details of a specific sales order by its ID."""
    print(f"Fetching details for sales order ID: {salesorder_id}...")
    return make_request("GET",base_url,  f"salesorders/{salesorder_id}", access_token, organization_id)

def update_sales_order(base_url, access_token, organization_id, salesorder_id, update_data):
    """Updates an existing sales order with new data."""
    print(f"Updating sales order ID: {salesorder_id}...")
    return make_request("PUT",base_url,  f"salesorders/{salesorder_id}", access_token, organization_id, update_data)

def delete_sales_order(base_url, access_token, organization_id, salesorder_id):
    """Deletes a sales order from Zakya."""
    print(f"Deleting sales order ID: {salesorder_id}...")
    return make_request("DELETE",base_url,  f"salesorders/{salesorder_id}", access_token, organization_id)

def mark_sales_order_confirmed(base_url, access_token, organization_id, salesorder_id):
    """Marks a sales order as confirmed."""
    print(f"Marking sales order ID {salesorder_id} as confirmed...")
    return make_request("POST",base_url,  f"salesorders/{salesorder_id}/status/confirmed", access_token, organization_id)

def mark_sales_order_void(base_url, access_token, organization_id, salesorder_id):
    """Marks a sales order as void."""
    print(f"Marking sales order ID {salesorder_id} as void...")
    return make_request("POST",base_url,  f"salesorders/{salesorder_id}/status/void", access_token, organization_id)

# Package Functions
def create_package(base_url, access_token, organization_id, salesorder_id, package_data):
    """Creates a new package for a sales order in Zakya."""
    print(f"Creating package for Sales Order ID: {salesorder_id}...")
    params = {"salesorder_id": salesorder_id}
    return make_request("POST",base_url,  "packages", access_token, organization_id, package_data, params)

def list_all_packages(base_url, access_token, organization_id):
    """Lists all packages available in Zakya."""
    print("Listing all packages...")
    return fetch_records_from_zakya(base_url, access_token, organization_id, "packages")

def fetch_package(base_url, access_token, organization_id, package_id):
    """Fetches details of a specific package by its ID."""
    print(f"Fetching details for package ID: {package_id}...")
    return make_request("GET",base_url,  f"packages/{package_id}", access_token, organization_id)

def update_package(base_url, access_token, organization_id, package_id, update_data):
    """Updates an existing package with new data."""
    print(f"Updating package ID: {package_id}...")
    return make_request("PUT",base_url,  f"packages/{package_id}", access_token, organization_id, update_data)

def delete_package(base_url, access_token, organization_id, package_id):
    """Deletes a package from Zakya."""
    print(f"Deleting package ID: {package_id}...")
    return make_request("DELETE",base_url,  f"packages/{package_id}", access_token, organization_id)


# Invoice Functions
def create_invoice(base_url, access_token, organization_id, invoice_data):
    """Creates a new invoice in Zakya."""
    print("Creating new invoice...")
    return make_request("POST",base_url,  "invoices", access_token, organization_id, invoice_data)

def list_all_invoices(base_url, access_token, organization_id):
    """Lists all invoices available in Zakya."""
    print("Listing all invoices...")
    return fetch_records_from_zakya(base_url, access_token, organization_id, "invoices")

def fetch_invoice(base_url, access_token, organization_id, invoice_id):
    """Fetches details of a specific invoice by its ID."""
    print(f"Fetching details for invoice ID: {invoice_id}...")
    return make_request("GET",base_url,  f"invoices/{invoice_id}", access_token, organization_id)

def update_invoice(base_url, access_token, organization_id, invoice_id, update_data):
    """Updates an existing invoice with new data."""
    print(f"Updating invoice ID: {invoice_id}...")
    return make_request("PUT",base_url,  f"invoices/{invoice_id}", access_token, organization_id, update_data)

def delete_invoice(base_url, access_token, organization_id, invoice_id):
    """Deletes an invoice from Zakya."""
    print(f"Deleting invoice ID: {invoice_id}...")
    return make_request("DELETE",base_url,  f"invoices/{invoice_id}", access_token, organization_id)

def mark_invoice_sent(base_url, access_token, organization_id, invoice_id):
    """Marks an invoice as sent."""
    print(f"Marking invoice ID {invoice_id} as sent...")
    return make_request("POST",base_url,  f"invoices/{invoice_id}/status/sent", access_token, organization_id)

def mark_invoice_void(base_url, access_token, organization_id, invoice_id):
    """Marks an invoice as void."""
    print(f"Marking invoice ID {invoice_id} as void...")
    return make_request("POST",base_url,  f"invoices/{invoice_id}/status/void", access_token, organization_id)

def mark_invoice_draft(base_url, access_token, organization_id, invoice_id):
    """Marks an invoice as draft."""
    print(f"Marking invoice ID {invoice_id} as draft...")
    return make_request("POST",base_url,  f"invoices/{invoice_id}/status/draft", access_token, organization_id)

def send_invoice_email(base_url, access_token, organization_id, invoice_id, email_data):
    """Sends an invoice email to the customer."""
    print(f"Sending invoice ID {invoice_id} via email...")
    return make_request("POST",base_url,  f"invoices/{invoice_id}/email", access_token, organization_id, email_data)

def fetch_invoice_payments(base_url, access_token, organization_id, invoice_id):
    """Fetches all payments associated with a specific invoice."""
    print(f"Fetching payments for invoice ID: {invoice_id}...")
    return make_request("GET",base_url,  f"invoices/{invoice_id}/payments", access_token, organization_id)

def apply_credits_to_invoice(base_url, access_token, organization_id, invoice_id, credit_data):
    """Applies customer credits to an invoice."""
    print(f"Applying credits to invoice ID: {invoice_id}...")
    return make_request("POST",base_url,  f"invoices/{invoice_id}/credits", access_token, organization_id, credit_data)

def update_invoice_template(base_url, access_token, organization_id, invoice_id, template_id):
    """Updates the PDF template associated with an invoice."""
    print(f"Updating template for invoice ID: {invoice_id}...")
    return make_request("PUT",base_url,  f"invoices/{invoice_id}/templates/{template_id}", access_token, organization_id)

def get_invoice_email_content(base_url, access_token, organization_id, invoice_id):
    """Retrieves the email content of a specific invoice."""
    print(f"Fetching email content for invoice ID: {invoice_id}...")
    return make_request("GET",base_url,  f"invoices/{invoice_id}/email", access_token, organization_id)

def email_multiple_invoices(base_url, access_token, organization_id, email_data):
    """Sends emails for multiple invoices."""
    print("Sending multiple invoices via email...")
    return make_request("POST",base_url,  "invoices/email", access_token, organization_id, email_data)

def get_payment_reminder_email_content(base_url, access_token, organization_id, invoice_id):
    """Fetches the content of the payment reminder email for a specific invoice."""
    print(f"Fetching payment reminder email content for invoice ID: {invoice_id}...")
    return make_request("GET",base_url,  f"invoices/{invoice_id}/paymentreminder", access_token, organization_id)

def bulk_export_invoices(base_url, access_token, organization_id, export_data):
    """Exports multiple invoices in bulk."""
    print("Initiating bulk export of invoices...")
    return make_request("POST",base_url,  "invoices/export", access_token, organization_id, export_data)

def bulk_print_invoices(base_url, access_token, organization_id, print_data):
    """Prints multiple invoices in bulk."""
    print("Initiating bulk print of invoices...")
    return make_request("POST",base_url,  "invoices/print", access_token, organization_id, print_data)

def disable_payment_reminder(base_url, access_token, organization_id, invoice_id):
    """Disables payment reminders for a specific invoice."""
    print(f"Disabling payment reminder for invoice ID: {invoice_id}...")
    return make_request("POST",base_url,  f"invoices/{invoice_id}/paymentreminder/disable", access_token, organization_id)

def enable_payment_reminder(base_url, access_token, organization_id, invoice_id):
    """Enables payment reminders for a specific invoice."""
    print(f"Enabling payment reminder for invoice ID: {invoice_id}...")
    return make_request("POST",base_url,  f"invoices/{invoice_id}/paymentreminder/enable", access_token, organization_id)


# Customer Payments Functions
def create_payment(base_url, access_token, organization_id, payment_data):
    """Creates a new customer payment in Zakya."""
    print("Creating new payment...")
    return make_request("POST",base_url,  "customerpayments", access_token, organization_id, payment_data)

def list_all_payments(base_url, access_token, organization_id):
    """Lists all customer payments in Zakya."""
    print("Listing all payments...")
    return make_request("GET",base_url,  "customerpayments", access_token, organization_id)

def fetch_payment(base_url,  access_token, organization_id, payment_id):
    """Fetches details of a specific payment by its ID."""
    print(f"Fetching details for payment ID: {payment_id}...")
    return make_request("GET",base_url, base_url,  f"/customerpayments/{payment_id}", access_token, organization_id)

def update_payment(base_url,  access_token, organization_id, payment_id, update_data):
    """Updates an existing customer payment with new data."""
    print(f"Updating payment ID: {payment_id}...")
    return make_request("PUT",base_url, base_url,  f"/customerpayments/{payment_id}", access_token, organization_id, update_data)

def delete_payment(base_url, access_token, organization_id, payment_id):
    """Deletes a customer payment from Zakya."""
    print(f"Deleting payment ID: {payment_id}...")
    return make_request("DELETE",base_url,  f"customerpayments/{payment_id}", access_token, organization_id)


# Sales Returns Functions
def create_sales_return(base_url, access_token, organization_id, sales_return_data):
    """Creates a new sales return in Zakya."""
    print("Creating new sales return...")
    return make_request("POST",base_url,  "salesreturns", access_token, organization_id, sales_return_data)

def list_all_sales_returns(base_url, access_token, organization_id):
    """Lists all sales returns in Zakya."""
    print("Listing all sales returns...")
    return make_request("GET",base_url,  "salesreturns", access_token, organization_id)

def fetch_sales_return(base_url, access_token, organization_id, salesreturn_id):
    """Fetches details of a specific sales return by its ID."""
    print(f"Fetching details for sales return ID: {salesreturn_id}...")
    return make_request("GET",base_url,  f"salesreturns/{salesreturn_id}", access_token, organization_id)

def update_sales_return(base_url, access_token, organization_id, salesreturn_id, update_data):
    """Updates an existing sales return with new data."""
    print(f"Updating sales return ID: {salesreturn_id}...")
    return make_request("PUT",base_url,  f"salesreturns/{salesreturn_id}", access_token, organization_id, update_data)

def delete_sales_return(base_url, access_token, organization_id, salesreturn_id):
    """Deletes a sales return from Zakya."""
    print(f"Deleting sales return ID: {salesreturn_id}...")
    return make_request("DELETE",base_url,  f"salesreturns/{salesreturn_id}", access_token, organization_id)

def create_sales_return_receive(base_url, access_token, organization_id, salesreturn_id, receive_data):
    """Creates a sales return receive to mark the returned goods as received."""
    print(f"Creating sales return receive for sales return ID: {salesreturn_id}...")
    params = {"salesreturn_id": salesreturn_id}
    return make_request("POST",base_url,  "salesreturnreceives", access_token, organization_id, receive_data, params)

def delete_sales_return_receive(base_url, access_token, organization_id, receive_id):
    """Deletes a sales return receive record."""
    print(f"Deleting sales return receive ID: {receive_id}...")
    return make_request("DELETE",base_url,  f"salesreturnreceives/{receive_id}", access_token, organization_id)


# Credit Notes Functions
def create_credit_note(base_url, access_token, organization_id, credit_note_data):
    """Creates a new credit note in Zakya."""
    print("Creating new credit note...")
    return make_request("POST",base_url,  "creditnotes", access_token, organization_id, credit_note_data)

def list_all_credit_notes(base_url, access_token, organization_id):
    """Lists all credit notes in Zakya."""
    print("Listing all credit notes...")
    return make_request("GET",base_url,  "creditnotes", access_token, organization_id)

def fetch_credit_note(base_url, access_token, organization_id, creditnote_id):
    """Fetches details of a specific credit note by its ID."""
    print(f"Fetching details for credit note ID: {creditnote_id}...")
    return make_request("GET",base_url,  f"creditnotes/{creditnote_id}", access_token, organization_id)

def update_credit_note(base_url, access_token, organization_id, creditnote_id, update_data):
    """Updates an existing credit note with new data."""
    print(f"Updating credit note ID: {creditnote_id}...")
    return make_request("PUT",base_url,  f"creditnotes/{creditnote_id}", access_token, organization_id, update_data)

def delete_credit_note(base_url, access_token, organization_id, creditnote_id):
    """Deletes a credit note from Zakya."""
    print(f"Deleting credit note ID: {creditnote_id}...")
    return make_request("DELETE",base_url,  f"creditnotes/{creditnote_id}", access_token, organization_id)

def void_credit_note(base_url, access_token, organization_id, creditnote_id):
    """Marks a credit note as void."""
    print(f"Voiding credit note ID: {creditnote_id}...")
    return make_request("POST",base_url,  f"creditnotes/{creditnote_id}/void", access_token, organization_id)

def convert_credit_note_to_draft(base_url, access_token, organization_id, creditnote_id):
    """Converts a credit note to draft status."""
    print(f"Converting credit note ID: {creditnote_id} to draft...")
    return make_request("POST",base_url, f"creditnotes/{creditnote_id}/converttoopen", access_token, organization_id)

def refund_credit_note(base_url, access_token, organization_id, creditnote_id, refund_data):
    """Processes a refund for a credit note."""
    print(f"Processing refund for credit note ID: {creditnote_id}...")
    return make_request("POST",base_url,  f"creditnotes/{creditnote_id}/refunds", access_token, organization_id, refund_data)

def fetch_credit_note_refunds(base_url, access_token, organization_id, creditnote_id):
    """Fetches all refunds related to a credit note."""
    print(f"Fetching refunds for credit note ID: {creditnote_id}...")
    return make_request("GET",base_url,  f"creditnotes/{creditnote_id}/refunds", access_token, organization_id)

def delete_credit_note_refund(base_url,access_token, organization_id, creditnote_id, refund_id):
    """Deletes a specific refund from a credit note."""
    print(f"Deleting refund ID: {refund_id} from credit note ID: {creditnote_id}...")
    return make_request("DELETE",base_url,  f"creditnotes/{creditnote_id}/refunds/{refund_id}", access_token, organization_id)

def email_credit_note(base_url, access_token, organization_id, creditnote_id, email_data):
    """Sends an email for a specific credit note."""
    print(f"Sending email for credit note ID: {creditnote_id}...")
    return make_request("POST",base_url, f"creditnotes/{creditnote_id}/email", access_token, organization_id, email_data)

def get_credit_note_email_content(base_url,access_token, organization_id, creditnote_id):
    """Retrieves the email content of a specific credit note."""
    print(f"Fetching email content for credit note ID: {creditnote_id}...")
    return make_request("GET",base_url, f"creditnotes/{creditnote_id}/email", access_token, organization_id)

def submit_credit_note_for_approval(base_url,access_token, organization_id, creditnote_id):
    """Submits a credit note for approval."""
    print(f"Submitting credit note ID {creditnote_id} for approval...")
    return make_request("POST",base_url,  f"creditnotes/{creditnote_id}/submit", access_token, organization_id)

def approve_credit_note(base_url,access_token, organization_id, creditnote_id):
    """Approves a submitted credit note."""
    print(f"Approving credit note ID: {creditnote_id}...")
    return make_request("POST",base_url, f"creditnotes/{creditnote_id}/approve", access_token, organization_id)

def fetch_credit_note_email_history(base_url,access_token, organization_id, creditnote_id):
    """Fetches the email history of a specific credit note."""
    print(f"Fetching email history for credit note ID: {creditnote_id}...")
    return make_request("GET",base_url, f"creditnotes/{creditnote_id}/emailhistory", access_token, organization_id)

def list_credit_note_templates(base_url,access_token, organization_id):
    """Lists all available credit note templates."""
    print("Listing all credit note templates...")
    return make_request("GET",base_url, "creditnotes/templates", access_token, organization_id)

def update_credit_note_template(base_url,access_token, organization_id, creditnote_id, template_id):
    """Updates the template of a specific credit note."""
    print(f"Updating template for credit note ID: {creditnote_id}...")
    return make_request("PUT",base_url, f"creditnotes/{creditnote_id}/templates/{template_id}", access_token, organization_id)

def apply_credits_to_invoices(base_url,access_token, organization_id, creditnote_id, apply_data):
    """Applies a credit note to one or more invoices."""
    print(f"Applying credit note ID: {creditnote_id} to invoices...")
    return make_request("POST",base_url, f"creditnotes/{creditnote_id}/invoices", access_token, organization_id, apply_data)


# Contact Functions
def create_contact(base_url,access_token, organization_id, contact_data):
    """Creates a new contact in Zakya."""
    print("Creating new contact...")
    return make_request("POST",base_url, "contacts", access_token, organization_id, contact_data)

def list_all_contacts(base_url,access_token, organization_id):
    """Lists all contacts in Zakya."""
    print("Listing all contacts...")
    return make_request("GET",base_url, "contacts", access_token, organization_id)

def fetch_contact(base_url,access_token, organization_id, contact_id):
    """Fetches details of a specific contact by its ID."""
    print(f"Fetching details for contact ID: {contact_id}...")
    return make_request("GET",base_url, f"contacts/{contact_id}", access_token, organization_id)

def update_contact(base_url, access_token, organization_id, contact_id, update_data):
    """Updates an existing contact with new data."""
    print(f"Updating contact ID: {contact_id}...")
    return make_request("PUT",base_url, f"contacts/{contact_id}", access_token, organization_id, update_data)

def delete_contact(base_url,access_token, organization_id, contact_id):
    """Deletes a contact from Zakya."""
    print(f"Deleting contact ID: {contact_id}...")
    return make_request("DELETE",base_url, f"contacts/{contact_id}", access_token, organization_id)

def mark_contact_active(base_url, access_token, organization_id, contact_id):
    """Marks a contact as active."""
    print(f"Marking contact ID {contact_id} as active...")
    return make_request("POST",base_url, f"contacts/{contact_id}/active", access_token, organization_id)

def mark_contact_inactive(base_url, access_token, organization_id, contact_id):
    """Marks a contact as inactive."""
    print(f"Marking contact ID {contact_id} as inactive...")
    return make_request("POST",base_url,  f"contacts/{contact_id}/inactive", access_token, organization_id)

def email_contact_statement(base_url, access_token, organization_id, contact_id, email_data):
    """Emails a statement to the contact."""
    print(f"Emailing statement to contact ID: {contact_id}...")
    return make_request("POST",base_url,  f"contacts/{contact_id}/statements/email", access_token, organization_id, email_data)

def get_contact_statement_email_content(base_url, access_token, organization_id, contact_id):
    """Retrieves email content for a contact's statement."""
    print(f"Fetching email content for contact statement ID: {contact_id}...")
    return make_request("GET",base_url,  f"contacts/{contact_id}/statements/email", access_token, organization_id)

def email_contact(base_url, access_token, organization_id, contact_id, email_data):
    """Sends an email to a contact."""
    print(f"Sending email to contact ID: {contact_id}...")
    return make_request("POST",base_url,  f"contacts/{contact_id}/email", access_token, organization_id, email_data)

def list_contact_comments(base_url, access_token, organization_id, contact_id):
    """Lists recent activities and comments of a contact."""
    print(f"Fetching comments for contact ID: {contact_id}...")
    return make_request("GET",base_url,  f"contacts/{contact_id}/comments", access_token, organization_id)


# Transfer Order Functions
def create_transfer_order(base_url, access_token, organization_id, transfer_order_data):
    """Creates a new transfer order in Zakya."""
    print("Creating new transfer order...")
    return make_request("POST",base_url,  "transferorders", access_token, organization_id, transfer_order_data)

def list_all_transfer_orders(base_url, access_token, organization_id):
    """Lists all transfer orders in Zakya."""
    print("Listing all transfer orders...")
    return make_request("GET",base_url,  "transferorders", access_token, organization_id)

def fetch_transfer_order(base_url, access_token, organization_id, transfer_order_id):
    """Fetches details of a specific transfer order by its ID."""
    print(f"Fetching details for transfer order ID: {transfer_order_id}...")
    return make_request("GET",base_url,  f"transferorders/{transfer_order_id}", access_token, organization_id)

def delete_transfer_order(base_url, access_token, organization_id, transfer_order_id):
    """Deletes a transfer order from Zakya."""
    print(f"Deleting transfer order ID: {transfer_order_id}...")
    return make_request("DELETE",base_url,  f"transferorders/{transfer_order_id}", access_token, organization_id)

def mark_transfer_order_as_transferred(base_url, access_token, organization_id, transfer_order_id):
    """Marks a transfer order as transferred."""
    print(f"Marking transfer order ID {transfer_order_id} as transferred...")
    return make_request("POST",base_url,  f"transferorders/{transfer_order_id}/markastransferred", access_token, organization_id)


# Price List Functions
def create_price_list(base_url, access_token, organization_id, price_list_data):
    """Creates a new price list in Zakya."""
    print("Creating new price list...")
    return make_request("POST",base_url,  "pricebooks", access_token, organization_id, price_list_data)

def list_all_price_lists(base_url, access_token, organization_id):
    """Lists all price lists in Zakya."""
    print("Listing all price lists...")
    return make_request("GET",base_url,  "pricebooks", access_token, organization_id)

def fetch_price_list(base_url, access_token, organization_id, pricebook_id):
    """Fetches details of a specific price list by its ID."""
    print(f"Fetching details for price list ID: {pricebook_id}...")
    return make_request("GET",base_url,  f"pricebooks/{pricebook_id}", access_token, organization_id)

def update_price_list(base_url, access_token, organization_id, pricebook_id, update_data):
    """Updates an existing price list with new data."""
    print(f"Updating price list ID: {pricebook_id}...")
    return make_request("PUT",base_url,  f"pricebooks/{pricebook_id}", access_token, organization_id, update_data)

def delete_price_list(base_url, access_token, organization_id, pricebook_id):
    """Deletes a price list from Zakya."""
    print(f"Deleting price list ID: {pricebook_id}...")
    return make_request("DELETE",base_url,  f"pricebooks/{pricebook_id}", access_token, organization_id)

def mark_price_list_active(base_url, access_token, organization_id, pricebook_id):
    """Marks a price list as active."""
    print(f"Marking price list ID {pricebook_id} as active...")
    return make_request("POST",base_url,  f"pricebooks/{pricebook_id}/active", access_token, organization_id)

def mark_price_list_inactive(base_url, access_token, organization_id, pricebook_id):
    """Marks a price list as inactive."""
    print(f"Marking price list ID {pricebook_id} as inactive...")
    return make_request("POST",base_url,  f"pricebooks/{pricebook_id}/inactive", access_token, organization_id)

    

def post_record_to_zakya(base_url, access_token, organization_id, endpoint, payload):
    """
    Send a POST request to Zakya API to create a new record.
    
    :param base_url: Base URL of the Zakya API.
    :param access_token: OAuth access token for authentication.
    :param organization_id: ID of the organization in Zakya.
    :param endpoint: API endpoint for the request (e.g., "/invoices").
    :param payload: Dictionary containing the data to be sent in the request.
    :return: JSON response from the API.
    """
    url = f"{base_url}/inventory/v1{endpoint}?organization_id={organization_id}"
    
    headers = {
        'Authorization': f"Zoho-oauthtoken {access_token}",
        'Content-Type': 'application/json'
    }

    response = requests.post(
        url=url,
        headers=headers,
        json=payload
    )
    print(response.text)
    response.raise_for_status()  # Raise an error for bad responses
    return response.json() 
