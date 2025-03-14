import streamlit as st
import pandas as pd
from utils.postgres_connector import crud
from utils.auth_decorator import require_auth

  # Enforce authentication on this page
def refresh_master_function():

    # Set page configuration
    st.set_page_config(
        page_title="Data Upload App",
        page_icon="📂",
        layout="centered"
    )

    # App title
    st.title("📂 Upload Data to Refresh Various Masters")

    # Sidebar navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.radio(
        "Select Option",
        ["Upload Product Master", "Upload Customer Master", "Upload SKU Vendor Mapping"]
    )

    # Function to upload and process CSV or Excel files
    def upload_file(table_name, file_type):
        st.subheader(f"Upload {file_type.upper()} for {table_name}")
        
        # File uploader with dynamic file type
        uploaded_file = st.file_uploader(
            f"Choose a {file_type.upper()} file for {table_name}",
            type=[file_type]
        )
        
        if uploaded_file is not None:
            try:
                # Read the uploaded file into a pandas DataFrame
                if file_type == "csv":
                    df = pd.read_csv(uploaded_file)
                elif file_type == "xlsx":
                    df = pd.read_excel(uploaded_file)
                
                # Display a preview of the data
                st.write("Preview of Uploaded Data:")
                st.dataframe(df.head())
                
                # Create table in PostgreSQL using PostgresCRUD class
                result = crud.create_table(table_name, df)
                st.success(result)
            except Exception as e:
                st.error(f"An error occurred: {e}")

    # Page logic
    if page == "Upload Product Master":
        upload_file("product_master", "csv")  # Only CSV allowed for Product Master
    elif page == "Upload Customer Master":
        upload_file("customer_master", "csv")  # Only CSV allowed for Customer Master
    elif page == "Upload SKU Vendor Mapping":
        upload_file("vendor_sku_mapping", "xlsx")  # Only Excel allowed for SKU Vendor Mapping


refresh_master_function()