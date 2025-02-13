import streamlit as st
import io
import pandas as pd
from main import process_taj_sales

st.title("Taj Sales Invoice Generator")

# Date picker for invoice date
invoice_date = st.date_input("Select Invoice Date")

# File Uploader for Taj Sales Excel File
uploaded_file = st.file_uploader("Upload Taj Sales Excel File", type=["xlsx"])
if uploaded_file:
    # Read uploaded Excel file
    taj_sales_df = pd.read_excel(uploaded_file, sheet_name="Sheet1", skiprows=7) # Skip the first 7 rows
    taj_sales_df = taj_sales_df[taj_sales_df['Br'].apply(lambda x: "Total" not in str(x) and pd.notna(x))]
    st.dataframe(taj_sales_df)

    # Process the DataFrame
    if st.button("Generate Invoice"):
        invoice_template = process_taj_sales(taj_sales_df,invoice_date)

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
