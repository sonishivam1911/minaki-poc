import streamlit as st
import io
import pandas as pd
from utils.bhavvam.taj import process_taj_sales
from utils.zakya_api import (get_authorization_url)


def fetch_zakya_code():
    auth_url = get_authorization_url()
    st.markdown(f"[Login with Zakya]({auth_url})")

st.title("Taj Sales Invoice Generator")

# Date picker for invoice date
invoice_date = st.date_input("Select Invoice Date")

# File Uploader for Taj Sales Excel File
uploaded_file = st.file_uploader("Upload Taj Sales Excel File", type=["xlsx"])
if uploaded_file:
    # Read uploaded Excel file
    taj_sales_df = pd.read_excel(uploaded_file, sheet_name="Sheet1", skiprows=7) # Skip the first 7 rows
    print(taj_sales_df.columns)  # Check available columns
    print(taj_sales_df.head())   # Inspect the first few rows

    taj_sales_df = taj_sales_df[taj_sales_df['BR.'].apply(lambda x: "Total" not in str(x) and pd.notna(x))]
    st.dataframe(taj_sales_df)

    # Process the DataFrame
    auth_code = st.session_state['code'] if 'code' in st.session_state else None
    access_token = st.session_state['access_token'] if 'access_token' in st.session_state else None
    api_domain = st.session_state['api_domain'] if 'api_domain' in st.session_state else None
    if auth_code and access_token and api_domain:
        if st.button("Generate Invoice"):
            invoice_template = process_taj_sales(taj_sales_df, invoice_date, st.session_state['api_domain'],
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
                label="Download Taj Invoice Template (CSV)",
                data=csv_buffer.getvalue(),
                file_name="taj_sales_invoice_template.csv",
                mime="text/csv",
            )
    else:
        # Display login button

        fetch_zakya_code()
