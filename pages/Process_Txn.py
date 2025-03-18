import streamlit as st
import io
import pandas as pd
import logging
from utils.bhavvam.payment_recon import process_transactions
from utils.zakya_api import (get_authorization_url)


st.title("Process Settlements")


def zakya_payment_function():
    uploaded_file = st.file_uploader("Upload Pinelabs MPR", type=["xlsx"])
    if uploaded_file:
        try:
            txn = pd.read_excel(uploaded_file, sheet_name="Trxn details", header=1)
        except Exception as e:
            logging.error(f"Error loading txn.xlsx: {e}")
        st.dataframe(txn)

        # Process the DataFrame
        if st.button("Process Transactions"):
            payment = process_transactions(txn,
                    st.session_state['api_domain'],
                    st.session_state['access_token'],
                    st.session_state['organization_id'])

            # Display processed DataFrame
            st.subheader("Processed Invoice Template")
            st.dataframe(payment)

            # Download Button
            csv_buffer = io.StringIO()
            payment.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            st.download_button(
                label="Download Summary",
                data=csv_buffer.getvalue(),
                file_name="payment_summary.csv",
                mime="text/csv",
            )




zakya_payment_function()