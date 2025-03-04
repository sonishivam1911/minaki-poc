import streamlit as st
import io
import pandas as pd
import logging
from main import process_taj_sales
from utils.bhavvam.payment_recon import process_transactions

st.title("Process Settlements")

# File Uploader for Taj Sales Excel File
uploaded_file = st.file_uploader("Upload Pinelabs MPR", type=["xlsx"])
if uploaded_file:
    try:
        txn = pd.read_excel("txn.xlsx", sheet_name="Trxn details", header=1)
    except Exception as e:
        logging.error(f"Error loading txn.xlsx: {e}")
    st.dataframe(txn)

    # Process the DataFrame
    if st.button("Process Transactions"):
        invoice_template = process_transactions(txn,
                        st.session_state['access_token'],
                        st.session_state['organization_id'])

        # Display processed DataFrame
        st.subheader("Processed Invoice Template")
        st.dataframe(invoice_template)

        # Download Button
        csv_buffer = io.StringIO()
        invoice_template.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        st.download_button(
            label="Download Invoice Template (CSV)",
            data=csv_buffer.getvalue(),
            file_name="taj_sales_invoice_template.csv",
            mime="text/csv",
        )
