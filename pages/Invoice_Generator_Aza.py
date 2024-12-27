import streamlit as st
import io
import pandas as pd
from main import process_aza_sales, fetch_customer_name_list

st.title("Aza Sales Invoice Generator")

# Date picker for invoice date
invoice_date = st.date_input("Select Invoice Date")
customer_list = fetch_customer_name_list()
selected_customer = st.selectbox("Select Customer", customer_list)

# File Uploader for Taj Sales Excel File
uploaded_file = st.file_uploader("Upload Aza Sales Excel File", type=["xlsx"])
sheet_name = uploaded_file.name.split(".")[0].split(" ")[0]
print(f"uploaded_file name is {sheet_name}")
if uploaded_file:
    # Read uploaded Excel file
    aza_sales_df = pd.read_excel(uploaded_file, sheet_name=sheet_name, skiprows=8) # Skip the first 8 rows
    aza_sales_df = aza_sales_df[(aza_sales_df['Item#'].notnull()) & (aza_sales_df["PO No./Cust Order"].isnull())]
    # st.dataframe(aza_sales_df)

    # Process the DataFrame
    if st.button("Generate Invoice"):
        invoice_template = process_aza_sales(aza_sales_df,invoice_date,selected_customer)

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
