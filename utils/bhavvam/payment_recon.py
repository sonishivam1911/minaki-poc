import pandas as pd
import requests
import logging
import time
from utils.zakya_api import (put_record_to_zakya, fetch_records_from_zakya, extract_record_list)

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
        vpa = row["Payer VPA"]
        
        payment = payment_df[payment_df["reference_number"] == reference_number]
        
        if not payment.empty:
            print(payment)
            payment_id = payment.iloc[0]["payment_id"]
            update_data = {
                "bank_charges": bank_charges,
                "custom_fields": [
                    {
                        "index": 1,
                        "label": "Status",
                        "api_name": "cf_status",
                        "placeholder": "cf_status",
                        "value": "Settled"
                    },
                    {
                        "index": 2,
                        "label": "Settlement Date",
                        "api_name": "cf_settlement_date",
                        "placeholder": "cf_settlement_date",
                        "value": settle_dt
                    },
                    {
                        "index": 3,
                        "label": "Instrument Type",
                        "api_name": "cf_instrument_type",
                        "value": ins_type,
                        "placeholder": "cf_instrument_type"
                    },
                    {
                        "index": 4,
                        "label": "F/D",
                        "api_name": "cf_f_d",
                        "placeholder": "cf_f_d",
                        "value": fd
                    },
                    {
                        "index": 5,
                        "label": "Card Category",
                        "api_name": "cf_card_category",
                        "placeholder": "cf_card_category",
                        "value": card_cat
                    },
                    {
                        "index": 6,
                        "label": "Card network",
                        "api_name": "cf_card_network",
                        "placeholder": "cf_card_network",
                        "value": card_net
                    },
                    {
                        "index": 7,
                        "label": "VPA",
                        "api_name": "cf_vpa",
                        "placeholder": "cf_vpa",
                        "value": vpa
                    }
                ]
            }

            # 🔥 Sanitize update_data before making the request
            update_data = sanitize_json(update_data)

            response = put_record_to_zakya(base_url, access_token, organization_id,'customerpayments', payment_id, update_data)
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
