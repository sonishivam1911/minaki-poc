import streamlit as st
import io
import pandas as pd
import logging
from utils.bhavvam.payment_recon import process_transactions
from utils.zakya_api import (get_authorization_url)


st.title("Process Settlements")

def fetch_zakya_code():
    auth_url = get_authorization_url()
    st.markdown(f"[Login with Zakya]({auth_url})")


def zakya_payment_function():
        # Check if authorization code is present in the URL
    auth_code = st.session_state['code'] if 'code' in st.session_state else None
    access_token = st.session_state['access_token'] if 'access_token' in st.session_state else None
    api_domain = st.session_state['api_domain'] if 'api_domain' in st.session_state else None
    if auth_code and access_token and api_domain:
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
    else:
        # Display login button
        fetch_zakya_code()




zakya_payment_function()