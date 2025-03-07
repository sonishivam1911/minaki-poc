import pandas as pd
import requests
import logging
import time
from utils.zakya_api import (list_all_payments, update_payment, fetch_records_from_zakya, extract_record_list)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

import numpy as np

# Function to sanitize JSON payload
def sanitize_json(data):
    """Replace NaN, inf, -inf values in a dictionary with None for JSON compliance."""
    if isinstance(data, dict):
        return {k: sanitize_json(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [sanitize_json(v) for v in data]
    elif isinstance(data, float):
        if np.isnan(data) or np.isinf(data):
            return None  # Convert invalid float values to None (JSON null)
    return data  # Return unchanged valid data


def process_transactions(txn, base_url, access_token, organization_id):
    if "RRN number" not in txn.columns or "Total Fee(including Taxes)" not in txn.columns:
        logging.error("Required columns not found in txn.xlsx")
        return pd.DataFrame()
    
    txn = txn[pd.to_numeric(txn["RRN number"], errors="coerce").notna()]
    txn["RRN number"] = txn["RRN number"].astype(int)
    
    payment_fetch = fetch_records_from_zakya(base_url, access_token, organization_id, 'customerpayments')
    payment_data = extract_record_list(payment_fetch, 'customerpayments')
    payment_df = pd.DataFrame.from_records(payment_data)
    
    payment_df["reference_number"] = pd.to_numeric(payment_df["reference_number"], errors="coerce")
    payment_df = payment_df.dropna(subset=["reference_number"])
    payment_df["reference_number"] = payment_df["reference_number"].astype(int)
    
    processed_payments = []
    
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
        
        payment = payment_df[payment_df["reference_number"] == reference_number]
        
        if not payment.empty:
            payment_id = payment.iloc[0]["payment_id"]
            update_data = {
                "custom_fields": [
                    # {"api_name": "settlement_date", "value": settle_dt},
                    {'label': 'Settlement Date', 'api_name': 'cf_settlement_date', 'search_entity': 'customer_payment', 'placeholder': 'cf_settlement_date', 'value': settle_dt.strftime("%Y-%m-%d")},
                    {'label': 'Status', 'api_name': 'cf_status', 'search_entity': 'customer_payment', 'placeholder': 'cf_status', 'value': 'Settled'}
                    # {"name": "instrument_type", "value": ins_type},
                    # {"name": "f_d", "value": fd},
                    # {"name": "card_category", "value": card_cat},
                    # {"name": "card_network", "value": card_net},
                    # {"name": "settlement_reference_number", "value": settle_ref},
                    # {"name": "vpa", "value": vpa
                    #  }
                ],
                "bank_charges": bank_charges
            }

            # 🔥 Sanitize update_data before making the request
            update_data = sanitize_json(update_data)

            response = update_payment(base_url, access_token, organization_id, payment_id, update_data)
            status = "Success" if response and response.get("code") == 0 else "Failed"
            logging.info(f"Payment ID {payment_id}: {status}")
            print(response)
        else:
            payment_id = None
            status = "Not Found"
            logging.warning(f"No payment found for Reference Number: {reference_number}")
        
        processed_payments.append({
            "Payment ID": payment_id,
            "Reference Number": reference_number,
            "Status": status,
            "Settlement Date": settle_dt,
            "Bank Charges": bank_charges
        })
        
        time.sleep(1)  # Prevent API rate limiting issues
    
    return pd.DataFrame(processed_payments)
