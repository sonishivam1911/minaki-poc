import pandas as pd
import requests
import logging
import time
from utils.zakya_api import (list_all_payments, update_payment)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def process_transactions(txn, access_token, organization_id):
    if "RRN number" not in txn.columns or "Total Fee(including Taxes)" not in txn.columns:
        logging.error("Required columns not found in txn.xlsx")
        return pd.DataFrame()
    
    txn = txn[pd.to_numeric(txn["RRN number"], errors="coerce").notna()]
    txn["RRN number"] = txn["RRN number"].astype(int)
    
    payment_df = list_all_payments(access_token, organization_id)
    
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
                "cf_settlement_date": settle_dt,
                "cf_instrument_type": ins_type,
                "cf_f_d": fd,
                "cf_card_category": card_cat,
                "cf_card_network": card_net,
                "cf_settlement_reference_number": settle_ref,
                "cf_vpa": vpa,
                "bank_charges": bank_charges
            }
            
            response = update_payment(access_token, organization_id, payment_id, update_data)
            status = "Success" if response and response.get("code") == 0 else "Failed"
            logging.info(f"Payment ID {payment_id}: {status}")
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
        
        time.sleep(1)
    
    return pd.DataFrame(processed_payments)
