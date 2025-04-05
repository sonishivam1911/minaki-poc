import streamlit as st
import tempfile
import os
import pandas as pd
import pdfplumber
from utils.bhavvam.sales_order_gen import (
    pdf_extract__po_details_ppus, 
    pdf_extract__po_details_aza,
    process_sales_order,
    process_csv_file
)
from server.file_management.main_file_management import upload_to_drive
from utils.zakya_api import put_record_to_zakya


def process_multiple_pdfs(uploaded_files, zakya_config):
    results = {
        "total": len(uploaded_files),
        "processed": 0,
        "failed": 0,
        "details": []
    }
    
    for uploaded_file in uploaded_files:
        temp_path = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_file:
                temp_file.write(uploaded_file.getvalue())
                temp_path = temp_file.name

            with pdfplumber.open(temp_path) as pdf:
                text = "\n".join([page.extract_text() for page in pdf.pages if page.extract_text()])
            
            lines = text.split("\n")
            result_extract = None
            
            for line in lines:
                if "Aza " in line:
                    result_extract = pdf_extract__po_details_aza(lines)
                    break
                elif "PSL" in line:
                    result_extract = pdf_extract__po_details_ppus(lines)
                    break
            
            if not result_extract:
                raise ValueError("Could not determine PO format (Aza/PPUS) from PDF.")

            result_order = process_sales_order(result_extract, zakya_config)
            
            if isinstance(result_order, str) and "already exists" in result_order:
                raise ValueError(result_order)

            billid = result_order["salesorder"]["salesorder_id"]
            serial_number = f"{result_order['salesorder']['customer_name']}-{result_order['salesorder']['salesorder_number']}"
            function_date = result_order["salesorder"]["date"]
            link = upload_to_drive(temp_path, 'salesorder', serial_number, function_date)

            payload = {
                "custom_fields": [
                    {
                        "api_name": "cf_orders_drive_link",
                        "placeholder": "cf_orders_drive_link",
                        "value": link
                    }
                ]
            }

            put_record_to_zakya(
                zakya_config["base_url"],
                zakya_config["access_token"],
                zakya_config["organization_id"],
                'salesorders',
                billid,
                payload
            )

            results["processed"] += 1
            results["details"].append({
                "filename": uploaded_file.name,
                "status": "success",
                "po_number": result_extract.get("PO No")
            })

        except Exception as e:
            results["failed"] += 1
            results["details"].append({
                "filename": uploaded_file.name,
                "status": "failed",
                "error": str(e)
            })

        finally:
            if temp_path and os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except Exception:
                    pass

    return results
    


def on_click_create_sales_order(result, po_format):
    customer_name = "Aza Delhi" if po_format != "PPUS" else "Pernia Delhi"
    process_sales_order(result, customer_name, {
        "base_url": st.session_state['api_domain'],
        "access_token": st.session_state['access_token'],
        "organization_id": st.session_state['organization_id'],
    })
    st.success("Sales order created successfully!")


def process_csv_and_create_salesorder(po_format_csv, uploaded_csv):
    with st.spinner("Processing POs from CSV..."):
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

    # --- Tab 1: Process PDF files ---
    with tab1:
        uploaded_files = st.file_uploader("Upload PO PDF(s)", type="pdf", accept_multiple_files=True)

        if uploaded_files:
            if st.button("Process PDF"):
                with st.spinner("Processing PDF files..."):
                    zakya_config = {
                        "base_url": st.session_state['api_domain'],
                        "access_token": st.session_state['access_token'],
                        "organization_id": st.session_state['organization_id'],
                    }

                    progress = st.progress(0)

                    results = process_multiple_pdfs(uploaded_files, zakya_config)

                    progress.progress(1.0)

                    st.success(f"Processed {results['processed']} out of {results['total']} files")
                    if results["failed"] > 0:
                        st.warning(f"Failed to process {results['failed']} files")

                    st.subheader("Processing Results:")
                    st.json(results["details"])

    # --- Tab 2: Process from CSV ---
    with tab2:    
        uploaded_csv = st.file_uploader("Upload CSV with PO links", type="csv")
        po_format_csv = st.radio("Select PO Format", ["PPUS", "AZA"], key="csv_format")        

        if uploaded_csv is not None:
            df_preview = pd.read_csv(uploaded_csv)
            st.write("CSV Preview:")
            st.dataframe(df_preview.head())

            st.button(
                "Process CSV",
                on_click=process_csv_and_create_salesorder,
                kwargs={"po_format_csv": po_format_csv, "uploaded_csv": uploaded_csv}
            )


main()
