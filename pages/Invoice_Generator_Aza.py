import streamlit as st
import io
import pandas as pd
from server.invoice.route import process_aza_sales
from main import fetch_customer_name_list

DEBUG = False
def process_aza_upload(uploaded_file):
    """
    Process AZA upload Excel files to extract designer info and data.
    
    Args:
        uploaded_file: Streamlit uploaded file object
        
    Returns:
        tuple: (designer_name, designer_city, dataframe) where dataframe contains the product data
    """
    
    try:
        # Read the Excel file without specifying headers
        excel_data = pd.read_excel(uploaded_file, header=None)
        
        # Initialize variables
        designer_name = None
        designer_city = None
        data_start_row = None
        headers_row = None
        
        # Debug info
        if DEBUG:
            st.write(f"Excel file has {len(excel_data)} rows and {len(excel_data.columns)} columns")
        
        # Scan through rows to find designer name and data headers
        for i, row in excel_data.iterrows():
            # Check if this row contains "Designer Name" in the first column
            first_col_value = str(row[0]) if pd.notna(row[0]) else ""
            if "designer name" or "designer" in first_col_value.lower():
                designer_name = row[1] if pd.notna(row[1]) else None
                
                # Extract city code if it exists (e.g., MINAKI-D -> D for Delhi)
                if designer_name and "-" in designer_name:
                    # Get the base name and location code
                    parts = designer_name.split("-")
                    location_code = parts[1].strip() if len(parts) > 1 else ""  # e.g., "K"   
                    print(f"location is {location_code}")
                    # Fetch customer list
                    customer_list = fetch_customer_name_list()     
                    # Find possible matches where:
                    # 1. Customer name contains the designer base name
                    # 2. Customer name has words
                    # 3. Customer name's second word could be a city
                    matching_customers = []
                    
                    for customer in customer_list:
                        customer_words = customer.split(" ")
                        
                        # Check if customer name contains designer base name
                        if len(customer_words) >= 2:
                            # The second word might be the city
                            matching_customers.append(customer)
                    
                    print(f"matching customers is : {matching_customers}")
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
                        selected_customer = st.selectbox("Select Customer", customer_list)
                else:
                    # Original code if no pattern is found
                    selected_customer = st.selectbox("Select Customer", customer_list)
                            
                
                # Debug info
                if DEBUG: 
                    st.write(f"Found designer row at {i}: {designer_name}")
                
                # Look for headers starting with "Code" in the upcoming rows
                for j in range(i+1, min(i+5, len(excel_data))):
                    first_col = str(excel_data.iloc[j, 0]) if pd.notna(excel_data.iloc[j, 0]) else ""
                    print(f"Frst column is : {first_col}")
                    if "code" == first_col.lower() or "vc" == first_col.lower():
                        headers_row = j
                        data_start_row = j + 1
                        if DEBUG:
                            st.write(f"Found headers at row {j}, data starts at row {data_start_row}")
                        break
                
                if headers_row is not None:
                    break
        
        if designer_name is None or headers_row is None:
            st.error("Could not find designer name or data headers in the file.")
            # Show the first few rows to help debug
            if DEBUG: 
                st.write("First 10 rows of the file:") 
            if DEBUG:
                st.write(excel_data.head(10))
            return None, None, None
        
        # Find the last row of data (usually before "Grand Total" or empty rows)
        last_row = len(excel_data)
        print(f"last row length is : {last_row}")
        for i in range(data_start_row, len(excel_data)):
            first_col = str(excel_data.iloc[i, 0]) if pd.notna(excel_data.iloc[i, 0]) else ""
            if first_col == "":
                last_row = i
                
            if "grand total" in first_col.lower():
                last_row = i
                break
        
        if DEBUG:
            st.write(f"Data ends at row {last_row}")
        
        # Extract the headers
        headers = excel_data.iloc[headers_row].tolist()
        # 2 cases are possible when vc is there code will be there , whenever code2 is there then first col is code 
        # rename code when vc to code and code to sku and if first col is code update code2 to sku okay ?
        headers_list = [str(h) if pd.notna(h) else f"Column_{i}" for i, h in enumerate(headers)]
        
        # Standardize column names:
        # Case 1: When first column is "VC" (rename VC->Code and Code->SKU)
        # Case 2: When first column is "Code" (rename Code2->SKU)
        if headers_list[0].lower() == "vc":
            # Find positions of VC and Code
            vc_index = 0  # We already know it's the first column
            code_index = next((i for i, h in enumerate(headers_list) if h.lower() == "code"), None)
            
            if code_index is not None:
                # Rename VC to Code and Code to SKU
                headers_list[vc_index] = "Code"
                headers_list[code_index] = "SKU"
                if DEBUG:
                    st.write("Renamed headers: VC -> Code, Code -> SKU")
        else:
            # First column is already "Code", look for Code2
            code2_index = next((i for i, h in enumerate(headers_list) if h.lower() == "code2"), None)
            
            if code2_index is not None:
                # Rename Code2 to SKU
                headers_list[code2_index] = "SKU"
                if DEBUG:
                    st.write("Renamed headers: Code2 -> SKU")


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
            if DEBUG:
                st.write(f"Filtered rows where SKU is not empty: {len(data_df)} rows remaining")

        
        # Filter rows that have Item# and no PO No./Cust Order (only if these columns exist)
        item_col = 'Item#' if 'Item#' in data_df.columns else None
        po_col = 'PO No./Cust Order' if 'PO No./Cust Order' in data_df.columns else None
        
        if item_col and po_col:
            filtered_df = data_df[data_df[item_col].notna()]
            # Only apply PO filter if it makes sense (there are actually rows with this value)
            if any(data_df[po_col].isna()):
                filtered_df = filtered_df[filtered_df[po_col].isna()]
            data_df = filtered_df
        
        # Show final dataframe structure
        st.write(f"Final dataframe has {len(data_df)} rows and {len(data_df.columns)} columns")
        
        return designer_name, designer_city, data_df, selected_customer
        
    except Exception as e:
        st.error(f"Error in process_aza_upload: {str(e)}")
        import traceback
        st.code(traceback.format_exc())
        return None, None, None, None

st.title("Aza Sales Invoice Generator")

# Date picker for invoice date
invoice_date = st.date_input("Select Invoice Date")
customer_list = fetch_customer_name_list()
# selected_customer = st.selectbox("Select Customer", customer_list)

# File Uploader for Aza Sales Excel File
uploaded_file = st.file_uploader("Upload Aza Sales Excel File", type=["xlsx"])

if uploaded_file:
    with st.expander("Debug Information", expanded=False):
        if DEBUG:
            st.write("File debugging information will appear here")
        
        # Create a copy of the uploaded file for debugging
        debug_file = uploaded_file
        debug_file.seek(0)  # Reset file pointer

    # Main processing
    try:
        # Use the improved method exclusively
        with st.spinner("Processing file..."):
            designer_name, designer_city, aza_sales_df, selected_customer = process_aza_upload(uploaded_file)

        
        if designer_name and aza_sales_df is not None and not aza_sales_df.empty:            
            # Show preview of the data
            st.subheader("Data Preview:")
            st.dataframe(aza_sales_df)
            
            # Process the DataFrame
            if st.button("Generate Invoice"):
                with st.spinner("Generating invoice..."):
                    invoice_template = process_aza_sales(
                        aza_sales_df,
                        invoice_date,
                        selected_customer,
                        {
                            'base_url': st.session_state['api_domain'],
                            'access_token': st.session_state['access_token'],
                            'organization_id': st.session_state['organization_id']
                        }
                    )

                    # Display processed DataFrame
                    st.subheader("Invoice Status")
                    st.dataframe(invoice_template)

                    # Download Button
                    if not invoice_template.empty:
                        csv_buffer = io.StringIO()
                        invoice_template.to_csv(csv_buffer, index=False)
                        csv_buffer.seek(0)
                        st.download_button(
                            label="Download Invoice Summary (CSV)",
                            data=csv_buffer.getvalue(),
                            file_name=f"{designer_name}_invoice_summary.csv",
                            mime="text/csv",
                        )
                        
                        if "Success" in invoice_template["status"].values:
                            st.success("Invoice created successfully!")
                        else:
                            st.error("Failed to create invoice. See details in the table above.")
                    else:
                        st.warning("No invoice data was generated.")
        else:
            st.error("Could not process the file. Please check if it follows the expected format.")
    except Exception as e:
        st.error(f"Error processing file: {str(e)}")
        import traceback
        st.code(traceback.format_exc())