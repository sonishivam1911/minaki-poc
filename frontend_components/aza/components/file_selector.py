import streamlit as st
import pandas as pd
from datetime import datetime
from config.logger import logger
from schema.zakya_schemas.schema import ZakyaContacts
from main import fetch_customer_data, fetch_customer_name_list

def aza_file_selection_section():
    """Create the file selection section UI for Aza."""
    st.subheader("1️⃣ Upload Aza Sales File")
    
    # Create columns for file upload and date selection
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # File uploader for Aza sales file
        uploaded_file = st.file_uploader("Upload Aza Sales Excel File", type=["xlsx"])
    
    with col2:
        # Default to current date
        default_date = datetime.now()
        invoice_date = st.date_input("Invoice Date", default_date)
        
        # Store invoice date in session state
        if invoice_date != st.session_state.get('invoice_date'):
            st.session_state['invoice_date'] = invoice_date
    
    # Process the uploaded file
    if uploaded_file:
        process_aza_file(uploaded_file)

def process_aza_file(uploaded_file):
    """Process the uploaded Aza sales file."""
    with st.spinner("Processing Aza file..."):
        designer_name, designer_city, aza_sales_df, selected_customer = process_aza_upload(uploaded_file)
        
        if designer_name and aza_sales_df is not None and not aza_sales_df.empty:
            # Update session state with extracted data
            st.session_state['aza_designer_name'] = designer_name
            st.session_state['selected_customer'] = selected_customer
            st.session_state['aza_orders'] = aza_sales_df
            
            # Reset other related state variables when a new file is uploaded
            st.session_state['aza_mapped_products'] = None
            st.session_state['aza_unmapped_products'] = None
            st.session_state['aza_product_mapping'] = {}
            st.session_state['aza_sales_orders'] = None
            st.session_state['aza_missing_sales_orders'] = None
            st.session_state['all_items_mapped'] = False
            
            # Fetch customer ID for the selected customer
            update_customer_selection(selected_customer)
            
            st.success(f"Successfully processed Aza file for designer {designer_name}")
        else:
            st.error("Could not process the file. Please check if it follows the expected format.")

def update_customer_selection(selected_customer):
    """Update session state when customer selection changes."""
    st.session_state['selected_customer'] = selected_customer
            
    customer_data = fetch_customer_data(ZakyaContacts, {
        'contact_name': {
            'op': 'eq', 'value': selected_customer
        }
    })
    
    if customer_data and len(customer_data) > 0:
        customer_data = customer_data[0]
        #logger.debug(f"Customer data after filtering is {customer_data}")
        customer_id = customer_data.get('contact_id')
        st.session_state['customer_id'] = customer_id
        
        # Display customer details
        st.info(f"Selected Customer: {selected_customer} (ID: {customer_id})")
    else:
        st.error(f"Could not fetch details for customer {selected_customer}")
        st.session_state['customer_id'] = None

def process_aza_upload(uploaded_file):
    """
    Process AZA upload Excel files to extract designer info and data.
    
    Args:
        uploaded_file: Streamlit uploaded file object
        
    Returns:
        tuple: (designer_name, designer_city, dataframe, selected_customer) where dataframe contains the product data
    """
    
    try:
        # Read the Excel file without specifying headers
        excel_data = pd.read_excel(uploaded_file, header=None)
        
        # Initialize variables
        designer_name = None
        designer_city = None
        data_start_row = None
        headers_row = None
        
        # Scan through rows to find designer name and data headers
        for i, row in excel_data.iterrows():
            # Check if this row contains "Designer Name" in the first column
            first_col_value = str(row[0]) if pd.notna(row[0]) else ""
            if "designer name" in first_col_value.lower():
                designer_name = row[1] if pd.notna(row[1]) else None
                
                # Extract city code if it exists (e.g., MINAKI-D -> D for Delhi)
                if designer_name and "-" in designer_name:
                    # Get the base name and location code
                    parts = designer_name.split("-")
                    location_code = parts[1].strip() if len(parts) > 1 else ""
                    
                    # Fetch customer list
                    customer_list = fetch_customer_name_list(is_aza=True)
                    
                    # Find possible matches
                    matching_customers = []
                    
                    for customer in customer_list:
                        customer_words = customer.split(" ")
                        
                        # Check if customer name contains designer base name
                        if len(customer_words) >= 2:
                            # The second word might be the city
                            matching_customers.append(customer)
                    
                    if matching_customers:
                        # First try to find exact match with location code
                        exact_matches = None
                        for customer_name in matching_customers:
                            customer_name_list = customer_name.split(" ")
                            
                            if location_code.lower() == customer_name_list[1][0].lower():
                                exact_matches = customer_name
                        if exact_matches:
                            selected_customer = exact_matches
                            designer_city = selected_customer
                            st.success(f"Selected customer: {selected_customer} (matched by location code)")
                        else:
                            # If no exact match, take the first match
                            selected_customer = matching_customers[0]
                            st.info(f"Selected customer: {selected_customer} (best match)")
                    else:
                        # If no matches found, let user select
                        customer_list = fetch_customer_name_list(is_aza=True)
                        selected_customer = st.selectbox("Select Customer", customer_list)
                else:
                    # Original code if no pattern is found
                    customer_list = fetch_customer_name_list(is_aza=True)
                    selected_customer = st.selectbox("Select Customer", customer_list)
                
                # Look for headers starting with "Code" in the upcoming rows
                for j in range(i+1, min(i+5, len(excel_data))):
                    first_col = str(excel_data.iloc[j, 0]) if pd.notna(excel_data.iloc[j, 0]) else ""
                    if "code" == first_col.lower() or "vc" == first_col.lower():
                        headers_row = j
                        data_start_row = j + 1
                        break
                
                if headers_row is not None:
                    break
        
        if designer_name is None or headers_row is None:
            st.error("Could not find designer name or data headers in the file.")
            return None, None, None, None
        
        # Find the last row of data (usually before "Grand Total" or empty rows)
        last_row = len(excel_data)
        for i in range(data_start_row, len(excel_data)):
            first_col = str(excel_data.iloc[i, 0]) if pd.notna(excel_data.iloc[i, 0]) else ""
            if first_col == "":
                last_row = i
                
            if "grand total" in first_col.lower():
                last_row = i
                break
        
        # Extract the headers
        headers = excel_data.iloc[headers_row].tolist()
        headers_list = [str(h) if pd.notna(h) else f"Column_{i}" for i, h in enumerate(headers)]
        
        # Standardize column names
        if headers_list[0].lower() == "vc":
            # Find positions of VC and Code
            vc_index = 0  # We already know it's the first column
            code_index = next((i for i, h in enumerate(headers_list) if h.lower() == "code"), None)
            
            if code_index is not None:
                # Rename VC to Code and Code to SKU
                headers_list[vc_index] = "Code"
                headers_list[code_index] = "SKU"
        else:
            # First column is already "Code", look for Code2
            code2_index = next((i for i, h in enumerate(headers_list) if h.lower() == "code2"), None)
            
            if code2_index is not None:
                # Rename Code2 to SKU
                headers_list[code2_index] = "SKU"

        # Extract only the data rows (excluding Grand Total)
        data_df = excel_data.iloc[data_start_row:last_row].copy()
        
        # Set the column names
        data_df.columns = headers_list
        
        # Reset index
        data_df = data_df.reset_index(drop=True)
        
        # Clean up the DataFrame
        # - Fill forward the Code column (since it might be empty for some rows)
        if 'Code' in data_df.columns:
            data_df['Code'] = data_df['Code'].fillna(method='ffill')

        # Filter out rows where SKU is empty if SKU column exists
        if 'SKU' in data_df.columns:
            data_df = data_df[data_df['SKU'].notna()]
        
        # Filter rows that have Item# and no PO No./Cust Order (only if these columns exist)
        item_col = 'Item#' if 'Item#' in data_df.columns else None
        po_col = 'PO No./Cust Order' if 'PO No./Cust Order' in data_df.columns else None
        
        if item_col and po_col:
            filtered_df = data_df[data_df[item_col].notna()]
            # Only apply PO filter if it makes sense (there are actually rows with this value)
            if any(data_df[po_col].isna()):
                filtered_df = filtered_df[filtered_df[po_col].isna()]
            data_df = filtered_df
        
        return designer_name, designer_city, data_df, selected_customer
        
    except Exception as e:
        st.error(f"Error in process_aza_upload: {str(e)}")
        import traceback
        st.code(traceback.format_exc())
        return None, None, None, None