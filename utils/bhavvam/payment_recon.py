import pandas as pd
import requests
import logging
import time
from utils.zakya_api import (get_authorization_url
    ,fetch_contacts
    ,fetch_records_from_zakya)

from utils.postgres_connector import crud

def fetch_zakya_code():
    auth_url = get_authorization_url()
    st.markdown(f"[Login with Zakya]({auth_url})")


def zakya_integration_function():
    st.title("MINAKI Intell")
        # Check if authorization code is present in the URL
    auth_code = st.session_state['code'] if 'code' in st.session_state else None
    access_token = st.session_state['access_token'] if 'access_token' in st.session_state else None
    api_domain = st.session_state['api_domain'] if 'api_domain' in st.session_state else None
    # if auth_code and access_token and api_domain:

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Function to fetch payment by Reference Number
def fetch_payment_by_reference(reference_number, access_token):
    url = f"{BASE_URL}customerpayments?organization_id={ORG_ID}&reference_number={reference_number}"
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if "customerpayments" in data and data["customerpayments"]:
                return data["customerpayments"][0]  # Return first matching payment
        else:
            logging.error(f"Error fetching payments: {response.text}")
    except requests.RequestException as e:
        logging.error(f"Request error while fetching payment: {e}")
    return None

# Function to update a payment in Zakya
def update_payment(payment_id, update_data, access_token):
    url = f"{BASE_URL}/{payment_id}?organization_id={ORG_ID}"
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Content-Type": "application/json",
    }
    try:
        response = requests.put(url, headers=headers, json=update_data)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Error updating payment {payment_id}: {response.text}")
    except requests.RequestException as e:
        logging.error(f"Request error while updating payment: {e}")
    return None

# Function to process transactions and update Zakya
def process_transactions():
    try:
        txn = pd.read_excel("txn.xlsx", sheet_name="Trxn details", header=1)
    except Exception as e:
        logging.error(f"Error loading txn.xlsx: {e}")
        return

    # Validate required columns
    if "RRN number" not in txn.columns or "Total Fee(including Taxes)" not in txn.columns:
        logging.error("Required columns not found in txn.xlsx")
        return

    txn = txn[pd.to_numeric(txn["RRN number"], errors="coerce").notna()]
    txn["RRN number"] = txn["RRN number"].astype(int)

    # Get access token
    access_token = get_access_token()
    if not access_token:
        logging.error("Exiting due to authentication failure.")
        return

    # Process each transaction
    for _, row in txn.iterrows():
        reference_number = row["RRN number"]
        bank_charges = row["Total Fee(including Taxes)"] if not pd.isna(row["Total Fee(including Taxes)"]) else 0
        settle_dt = pd.to_datetime(row["Settlement Date"], format="%d-%b-%y").strftime("%Y-%m-%d")
        ins_type = row["Instrument Type"]
        fd = row["Foreign/ Domestic"]
        card_cat = row["Card Category"]
        card_net = row["Card network"]
        settle_ref = row["Bank reference number"]
        vpa = row["Payer VPA"]

        payment = fetch_payment_by_reference(reference_number, access_token)
        if payment:
            update_data = {
                "cf_settlement_date": settle_dt,
                "cf_instrument_type": ins_type,
                "cf_f_d": fd,
                "cf_card_category": card_cat,
                "cf_card_network": card_net,
                "cf_settlement_reference_number": settle_ref,
                "cf_vpa": vpa,
                "bank_charges": bank_charges
            }

            response = update_payment(payment["payment_id"], update_data, access_token)
            if response and response.get("code") == 0:
                logging.info(f"Updated Payment ID {payment['payment_id']} successfully.")
            else:
                logging.error(f"Failed to update Payment ID {payment['payment_id']}.")
        else:
            logging.warning(f"No payment found for Reference Number: {reference_number}")

        # Avoid hitting API rate limits
        time.sleep(1)

# Execute the process
if __name__ == "__main__":
    process_transactions()
