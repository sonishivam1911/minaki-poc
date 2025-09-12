import streamlit as st
import io
import pandas as pd
from datetime import datetime
from server.invoice.route import AzaInvoiceProcessor
from main import fetch_customer_name_list

DEBUG = False

def process_aza_upload_new(uploaded_file):
    """
    Process AZA upload Excel files to extract designer info and create region-based dataframes.
    
    Args:
        uploaded_file: Streamlit uploaded file object
        
    Returns:
        tuple: (designer_name, region_dataframes_dict, selected_customers_dict) 
               where region_dataframes_dict contains separate dataframes for each region
    """
    
    try:
        # Read the Excel file without specifying headers
        excel_data = pd.read_excel(uploaded_file, header=None)
        
        # Initialize variables
        designer_name = None
        headers_row = None
        data_start_row = None
        
        # Debug info
        if DEBUG:
            st.write(f"Excel file has {len(excel_data)} rows and {len(excel_data.columns)} columns")
        
        # Step 1: Find designer name
        for i, row in excel_data.iterrows():
            first_col_value = str(row[0]) if pd.notna(row[0]) else ""
            if first_col_value.lower() == 'designer name':
                designer_name = row[1] if pd.notna(row[1]) else None
                if DEBUG:
                    st.write(f"Found designer at row {i}: {designer_name}")
                break
        
        # Step 2: Find headers row (where first column is "Rgn" or "Region")
        for i, row in excel_data.iterrows():
            first_col_value = str(row[0]) if pd.notna(row[0]) else ""
            if first_col_value.lower() in ['rgn', 'region']:
                headers_row = i
                data_start_row = i + 1
                if DEBUG:
                    st.write(f"Found headers at row {i}, data starts at row {data_start_row}")
                break
        
        if designer_name is None or headers_row is None:
            st.error("Could not find designer name or headers (Rgn/Region) in the file.")
            if DEBUG:
                st.write("First 10 rows of the file:")
                st.write(excel_data.head(10))
            return None, None, None
        
        # Step 3: Find the last row of data (before "Grand Total")
        last_row = len(excel_data)
        for i in range(data_start_row, len(excel_data)):
            first_col = str(excel_data.iloc[i, 0]) if pd.notna(excel_data.iloc[i, 0]) else ""
            if "grand total" in first_col.lower():
                last_row = i
                break
        
        if DEBUG:
            st.write(f"Data ends at row {last_row}")
        
        # Step 4: Extract headers and rename Code to SKU
        headers = excel_data.iloc[headers_row].tolist()
        headers_list = [str(h) if pd.notna(h) else f"Column_{i}" for i, h in enumerate(headers)]
        
        # Rename "Code" to "SKU"
        if 'Code' in headers_list:
            code_index = headers_list.index('Code')
            headers_list[code_index] = 'SKU'
            if DEBUG:
                st.write("Renamed 'Code' to 'SKU'")
        
        # Step 5: Extract data and create main dataframe
        data_df = excel_data.iloc[data_start_row:last_row].copy()
        data_df.columns = headers_list
        data_df = data_df.reset_index(drop=True)
        
        # Step 6: Get unique regions and create separate dataframes
        region_col = headers_list[0]  # First column is the region column
        unique_regions = data_df[region_col].dropna().unique()
        unique_regions = [r for r in unique_regions if str(r).strip() != '']
        
        if DEBUG:
            st.write(f"Found regions: {list(unique_regions)}")
        
        # Step 7: Create dataframes for each region
        region_dataframes = {}
        selected_customers = {}
        
        # Get customer list once and convert to list if it's numpy array
        customer_list = fetch_customer_name_list(is_aza=True)
        if hasattr(customer_list, 'tolist'):  # Check if it's numpy array
            customer_list = customer_list.tolist()
        elif not isinstance(customer_list, list):  # Convert other array types to list
            customer_list = list(customer_list)
        
        for region in unique_regions:
            # Filter data for this region
            region_mask = data_df[region_col] == region
            region_df = data_df[region_mask].copy()
            
            # Remove the region column from the dataframe (we don't need it in final data)
            region_df = region_df.drop(columns=[region_col])
            
            # Clean the dataframe - filter out rows with empty SKU
            if 'SKU' in region_df.columns:
                region_df = region_df[region_df['SKU'].notna() & (region_df['SKU'] != '')]
            
            # Add Item# column if it doesn't exist (using index)
            if 'Item#' not in region_df.columns:
                region_df['Item#'] = range(len(region_df))
            
            # Add PO No. column if it doesn't exist 
            if 'PO No.' not in region_df.columns:
                region_df['PO No.'] = f"AZA-{region}-" + pd.Series(range(len(region_df))).astype(str)
            
            # Ensure numeric columns are properly formatted
            numeric_columns = ['Qty', 'Amount', 'Total']
            for col in numeric_columns:
                if col in region_df.columns:
                    region_df[col] = pd.to_numeric(region_df[col], errors='coerce').fillna(0)
            
            # Store the dataframe
            region_dataframes[region] = region_df
            
            # Step 8: Customer matching for each region
            # Create designer name with region: e.g., "MINAKI-D"
            base_designer_name = designer_name.split('-')[0] if '-' in designer_name else designer_name
            designer_with_region = f"{base_designer_name}-{region}"
            
            # Define region to location mapping
            region_to_location = {
                'D': ['Delhi', 'Mehrauli'],
                'H': ['Hyderabad'],
                'M': ['Mumbai', 'Alt', 'Mount'],
                'K': ['Kolkata'],
                'A': ['Ahmedabad']  # Adding common regions
            }
            
            # Get location keywords for this region
            location_keywords = region_to_location.get(region.upper(), [])
            
            # Find recommended customer based on location keywords
            recommended_customer = None
            if location_keywords:
                for customer_name in customer_list:
                    customer_lower = customer_name.lower()
                    # Check if ALL location keywords are present in customer name
                    if all(keyword.lower() in customer_lower for keyword in location_keywords):
                        recommended_customer = customer_name
                        break
            
            # Find the index of recommended customer for default selection
            default_index = 0
            if recommended_customer and recommended_customer in customer_list:
                try:
                    default_index = customer_list.index(recommended_customer)
                except (ValueError, AttributeError):
                    # Fallback if index method fails or customer not found
                    default_index = 0
            
            # Show dropdown with recommended customer pre-selected
            selected_customers[region] = st.selectbox(
                f"Select Customer for Region {region}:", 
                customer_list,
                index=default_index,
                key=f"customer_{region}",
                help=f"Recommended: {recommended_customer}" if recommended_customer else "No specific recommendation for this region"
            )
            
            if DEBUG:
                st.write(f"Region {region}: Recommended {recommended_customer}, Selected {selected_customers[region]}")
        
        return designer_name, region_dataframes, selected_customers
        
    except Exception as e:
        st.error(f"Error in process_aza_upload_new: {str(e)}")
        import traceback
        st.code(traceback.format_exc())
        return None, None, None

def analyze_region_products(region_df, zakya_connection_object):
    """
    Analyze products in a region to show mapped vs unmapped items.
    """
    try:
        # Create processor instance
        processor = AzaInvoiceProcessor(
            sales_df=region_df,
            invoice_date=datetime.now(),
            zakya_connection_object=zakya_connection_object,
            customer_name="temp"  # Not used for analysis
        )
        
        # Preprocess the data
        processor.preprocess_data_sync()
        
        # Get product analysis
        import asyncio
        product_analysis = asyncio.run(processor.analyze_uploaded_products())
        
        return product_analysis
        
    except Exception as e:
        st.error(f"Error analyzing products: {str(e)}")
        return {
            'mapped_products': [],
            'unmapped_products': [],
            'product_mapping': {},
            'error': str(e)
        }

def process_aza_sales_region(region_df, invoice_date, customer_name, zakya_connection_object):
    """
    Process AZA sales for a specific region using the simplified AzaInvoiceProcessor.
    """
    try:
        # Create processor instance with simplified parameters
        processor = AzaInvoiceProcessor(
            sales_df=region_df,
            invoice_date=invoice_date,
            zakya_connection_object=zakya_connection_object,
            customer_name=customer_name
        )
        
        # Preprocess the data
        processor.preprocess_data_sync()
        
        # Create invoice object with required format
        invoice_object = {
            'invoice_date': invoice_date
        }
        
        # Create invoices using the processor
        import asyncio
        result = asyncio.run(processor.create_invoices(invoice_object))
        
        # Handle the return format
        if isinstance(result, dict) and 'invoice_df' in result:
            return result['invoice_df']
        else:
            # Fallback to old format
            return result
            
    except Exception as e:
        st.error(f"Error processing AZA sales for region: {str(e)}")
        import traceback
        st.code(traceback.format_exc())
        return pd.DataFrame([{
            "customer_name": customer_name,
            "status": "Failed",
            "error": str(e)
        }])

# Streamlit App
st.title("Aza Sales Invoice Generator (Multi-Region) - Simplified")

# Date picker for invoice date
invoice_date = st.date_input("Select Invoice Date")

# File Uploader for Aza Sales Excel File
uploaded_file = st.file_uploader("Upload Aza Sales Excel File", type=["xlsx"])

if uploaded_file:
    if DEBUG:
        with st.expander("Debug Information", expanded=False):
            st.write("File debugging information will appear here")

    # Main processing
    try:
        with st.spinner("Processing file..."):
            designer_name, region_dataframes, selected_customers = process_aza_upload_new(uploaded_file)

        if designer_name and region_dataframes:
            st.success(f"✅ Found designer: **{designer_name}**")
            st.info(f"📊 Found **{len(region_dataframes)}** regions: {', '.join(list(region_dataframes.keys()))}")
            
            # Prepare zakya connection object
            zakya_connection_object = {
                'base_url': st.session_state.get('api_domain', ''),
                'access_token': st.session_state.get('access_token', ''),
                'organization_id': st.session_state.get('organization_id', '')
            }
            
            # Show each region as a separate card
            for region, df in region_dataframes.items():
                # Create a container for each region card
                with st.container():
                    st.markdown(f"### 🏷️ Region {region}")
                    
                    # Basic info row
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Customer", selected_customers.get(region, 'Not selected'))
                    with col2:
                        st.metric("Items", len(df))
                    with col3:
                        st.metric("Total Value", f"₹{df['Total'].sum():,.2f}")
                    
                    # DataFrame display
                    st.dataframe(df, use_container_width=True, height=200)
                    
                    # Action buttons row
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if st.button(f"🔍 Analyze Products", key=f"analyze_{region}", use_container_width=True):
                            with st.spinner(f"Analyzing products for Region {region}..."):
                                analysis = analyze_region_products(df, zakya_connection_object)
                                
                                if 'error' not in analysis:
                                    mapped_count = len(analysis.get('mapped_products', []))
                                    unmapped_count = len(analysis.get('unmapped_products', []))
                                    
                                    # Store analysis in session state
                                    st.session_state[f'analysis_{region}'] = analysis
                                    
                                    # Show results
                                    col_a, col_b = st.columns(2)
                                    with col_a:
                                        st.success(f"✅ {mapped_count} Mapped SKUs")
                                    with col_b:
                                        if unmapped_count > 0:
                                            st.warning(f"⚠️ {unmapped_count} Unmapped SKUs")
                                        else:
                                            st.success("🎉 All SKUs Mapped!")
                                    
                                    # Show unmapped details if any
                                    if unmapped_count > 0:
                                        unmapped_df = pd.DataFrame(analysis['unmapped_products'])
                                        if not unmapped_df.empty:
                                            st.write("**Unmapped SKUs:**")
                                            st.dataframe(unmapped_df[['sku', 'item_description', 'total']], height=150)
                                else:
                                    st.error(f"❌ Analysis failed: {analysis['error']}")
                    
                    with col2:
                        # Check if connection details are available
                        connection_ready = all([
                            zakya_connection_object['base_url'], 
                            zakya_connection_object['access_token'], 
                            zakya_connection_object['organization_id']
                        ])
                        
                        if not connection_ready:
                            st.button(f"🚫 Create Invoice", disabled=True, use_container_width=True, 
                                     help="Missing Zakya connection details")
                        else:
                            if st.button(f"💰 Create Invoice", key=f"invoice_{region}", 
                                        use_container_width=True, type="primary"):
                                customer = selected_customers[region]
                                
                                with st.spinner(f"Creating invoice for Region {region}..."):
                                    try:
                                        invoice_result = process_aza_sales_region(
                                            df,
                                            invoice_date,
                                            customer,
                                            zakya_connection_object
                                        )
                                        
                                        # Store result in session state
                                        st.session_state[f'invoice_result_{region}'] = invoice_result
                                        
                                        if invoice_result is not None and not invoice_result.empty:
                                            # Check if successful
                                            if "status" in invoice_result.columns:
                                                success_rows = invoice_result[invoice_result["status"] == "Success"]
                                                if not success_rows.empty:
                                                    st.success(f"✅ Invoice created successfully!")
                                                    
                                                    # Show invoice details
                                                    st.dataframe(invoice_result, use_container_width=True)
                                                    
                                                    # Show metrics if available
                                                    if "mapped_items" in invoice_result.columns and "unmapped_items" in invoice_result.columns:
                                                        col_x, col_y = st.columns(2)
                                                        with col_x:
                                                            st.metric("Mapped Items", success_rows["mapped_items"].sum())
                                                        with col_y:
                                                            st.metric("Unmapped Items", success_rows["unmapped_items"].sum())
                                                    
                                                    # Download button
                                                    csv_buffer = io.StringIO()
                                                    invoice_result.to_csv(csv_buffer, index=False)
                                                    csv_buffer.seek(0)
                                                    
                                                    base_designer_name = designer_name.split('-')[0] if '-' in designer_name else designer_name
                                                    download_filename = f"{base_designer_name}-{region}_invoice.csv"
                                                    
                                                    st.download_button(
                                                        label=f"📥 Download Invoice Summary",
                                                        data=csv_buffer.getvalue(),
                                                        file_name=download_filename,
                                                        mime="text/csv",
                                                        key=f"download_{region}",
                                                        use_container_width=True
                                                    )
                                                else:
                                                    st.error(f"❌ Invoice creation failed for Region {region}")
                                                    st.dataframe(invoice_result, use_container_width=True)
                                            else:
                                                st.dataframe(invoice_result, use_container_width=True)
                                        else:
                                            st.error(f"❌ No invoice data generated for Region {region}")
                                    
                                    except Exception as e:
                                        st.error(f"❌ Error creating invoice: {str(e)}")
                    
                    # Show stored results if they exist
                    if f'analysis_{region}' in st.session_state and f'invoice_result_{region}' not in st.session_state:
                        analysis = st.session_state[f'analysis_{region}']
                        mapped_count = len(analysis.get('mapped_products', []))
                        unmapped_count = len(analysis.get('unmapped_products', []))
                        
                        col_info1, col_info2 = st.columns(2)
                        with col_info1:
                            st.info(f"📊 Analysis: {mapped_count} mapped, {unmapped_count} unmapped")
                        with col_info2:
                            if unmapped_count > 0:
                                st.warning("⚠️ Some SKUs are unmapped - they will be added as generic items")
                    
                    if f'invoice_result_{region}' in st.session_state:
                        result = st.session_state[f'invoice_result_{region}']
                        if result is not None and not result.empty:
                            success_rows = result[result["status"] == "Success"] if "status" in result.columns else result
                            if not success_rows.empty:
                                st.success(f"✅ Invoice already created for Region {region}")
                    
                    # Add separator between regions
                    st.divider()
                    
        else:
            st.error("❌ Could not process the file. Please check if it follows the expected format.")
            st.info("""
            **Expected format:**
            - Excel file with 'Designer Name' row
            - Headers row starting with 'Rgn' or 'Region'  
            - Data rows with regions (D, H, M, K, A, etc.)
            - 'Code' column (will be renamed to 'SKU')
            """)
            
    except Exception as e:
        st.error(f"❌ Error processing file: {str(e)}")
        import traceback
        if DEBUG:
            st.code(traceback.format_exc())