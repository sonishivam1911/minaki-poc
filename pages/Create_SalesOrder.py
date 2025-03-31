import streamlit as st
import tempfile
import os
import pandas as pd

from utils.bhavvam.sales_order_gen import (
    pdf_extract__po_details_ppus, 
    pdf_extract__po_details_aza,
    process_sales_order,
    process_csv_file
)


def process_multiple_pdfs(uploaded_files, po_format, zakya_config):
    """
    Processes multiple uploaded PDF files.
    
    Args:
        uploaded_files: List of uploaded PDF files from Streamlit
        po_format: String indicating the format ('PPUS' or 'AZA')
        zakya_config: Dictionary with Zakya API configuration
        
    Returns:
        Dictionary with processing results and statistics
    """
    results = {
        "total": len(uploaded_files),
        "processed": 0,
        "failed": 0,
        "details": []
    }
    
    for uploaded_file in uploaded_files:
        temp_path = None
        try:
            # Save uploaded file to a temporary location
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_file:
                temp_file.write(uploaded_file.getvalue())
                temp_path = temp_file.name
            
            # Extract data based on selected format
            if po_format == "PPUS":
                result = pdf_extract__po_details_ppus(temp_path)
                customer_name = "Pernia Delhi"
            else:  # AZA
                result = pdf_extract__po_details_aza(temp_path)
                customer_name = "Aza Delhi"
            
            # Process the sales order
            process_sales_order(result, customer_name, zakya_config)
            
            # Update statistics
            results["processed"] += 1
            results["details"].append({
                "filename": uploaded_file.name,
                "status": "success",
                "po_number": result.get("PO No")
            })
            
        except Exception as e:
            # Handle errors
            results["failed"] += 1
            results["details"].append({
                "filename": uploaded_file.name,
                "status": "failed",
                "error": str(e)
            })
            
        finally:
            # Clean up temp file
            if temp_path and os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except:
                    pass
    
    return results


def on_click_create_sales_order(result,po_format):
    customer_name = "Aza Delhi" if po_format != "PPUS" else "Pernia Delhi"

    process_sales_order(result,customer_name,{
        "base_url" : st.session_state['api_domain'],
        "access_token" : st.session_state['access_token'],
        "organization_id" : st.session_state['organization_id'],
    })
    st.success("Sales order created successfully!")



def process_csv_and_create_salesorder(po_format_csv, uploaded_csv):
    with st.spinner("Processing POs from CSV..."):
                    # Get Zakya config
        zakya_config = {
                        "base_url": st.session_state['api_domain'],
                        "access_token": st.session_state['access_token'],
                        "organization_id": st.session_state['organization_id'],
                    }
        result = process_csv_file(
                        csv_file=uploaded_csv,
                        vendor=po_format_csv,
                        zakya_config=zakya_config
                    )   

        st.json(result)


def main():
    st.title("PO PDF Processor")

    tab1, tab2 = st.tabs(["Process Single PO", "Process Multiple POs from CSV"])

    with tab1:
        # File upload
        uploaded_files = st.file_uploader("Upload PO PDF", type="pdf",accept_multiple_files=True)

    # Format selection radio button
        po_format = st.radio("Select PO Format", ["PPUS", "AZA"])

        if uploaded_files is not None:
        # Save uploaded file to a temporary location
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_file:
                temp_file.write(uploaded_files.getvalue())
                temp_path = temp_file.name
        
        # Process button
            if st.button("Process PDF"):
                with st.spinner("Processing PDF files..."):
                    # Setup Zakya config
                    zakya_config = {
                        "base_url": st.session_state['api_domain'],
                        "access_token": st.session_state['access_token'],
                        "organization_id": st.session_state['organization_id'],
                    }
                    
                    # Add a progress bar
                    progress = st.progress(0)
                    
                    # Process the files
                    results = process_multiple_pdfs(uploaded_files, po_format, zakya_config)
                    
                    # Update progress bar to complete
                    progress.progress(1.0)
                    
                    # Display results
                    st.success(f"Processed {results['processed']} out of {results['total']} files")
                    if results["failed"] > 0:
                        st.warning(f"Failed to process {results['failed']} files")
                    
                    # Show detailed results
                    st.subheader("Processing Results:")
                    st.json(results["details"])
        
        # Clean up temp file
            os.unlink(temp_path)


    # Tab 2: CSV Processing (new functionality)
    with tab2:    
        # CSV file upload
        uploaded_csv = st.file_uploader("Upload CSV with PO links", type="csv")

        # Format selection radio button for CSV
        po_format_csv = st.radio("Select PO Format", ["PPUS", "AZA"], key="csv_format")        
        
        if uploaded_csv is not None:
            # Show preview of CSV
            df_preview = pd.read_csv(uploaded_csv)
            st.write("CSV Preview:")
            st.dataframe(df_preview.head())
            
            # Process button for CSV
            st.button("Process CSV",on_click=process_csv_and_create_salesorder,kwargs=(po_format_csv, uploaded_csv))


main()