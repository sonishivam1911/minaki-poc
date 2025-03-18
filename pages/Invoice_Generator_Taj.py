import streamlit as st
import io
import pandas as pd
from server.taj_without_sales_order import process_taj_sales

st.title("Taj Sales Invoice Generator")

# Date picker for invoice date
invoice_date = st.date_input("Select Invoice Date")

# File Uploader for Taj Sales Excel File
uploaded_file = st.file_uploader("Upload Taj Sales Excel File", type=["xlsx"])
if uploaded_file:
    # Read uploaded Excel file
    taj_sales_df = pd.read_excel(uploaded_file, sheet_name="Sheet1", skiprows=7) # Skip the first 7 rows
    
    # Get the first column name
    first_col = taj_sales_df.columns[0]
    
    # Filter rows where first column doesn't contain 'Total' and is not null
    taj_sales_df = taj_sales_df[taj_sales_df[first_col].apply(lambda x: "Total" not in str(x) and pd.notna(x))]
    st.dataframe(taj_sales_df)
    
    # Process the DataFrame
    if st.button("Generate Invoice"):
        try:
            invoice_template = process_taj_sales(taj_sales_df, invoice_date, {
                'base_url': st.session_state['api_domain'],
                'access_token': st.session_state['access_token'],
                'organization_id': st.session_state['organization_id']
            })
            
            # Display processed DataFrame
            st.subheader("Processed Invoice Template")
            st.dataframe(invoice_template)
            
            # Download Button
            if not invoice_template.empty:
                csv_buffer = io.StringIO()
                invoice_template.to_csv(csv_buffer, index=False)
                csv_buffer.seek(0)
                st.download_button(
                    label="Download Invoice Template (CSV)",
                    data=csv_buffer.getvalue(),
                    file_name="taj_sales_invoice_template.csv",
                    mime="text/csv",
                )
            else:
                st.warning("No invoices were generated. Please check the logs for details.")
        except Exception as e:
            st.error(f"Error processing the file: {str(e)}")