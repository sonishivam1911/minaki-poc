import streamlit as st
import tempfile
import os

from utils.bhavvam.sales_order_gen import pdf_extract__po_details_ppus, pdf_extract__po_details_aza,process_sales_order


def on_click_create_sales_order(result,po_format):
    customer_name = "Aza Delhi" if po_format != "PPUS" else "Pernia Delhi"

    process_sales_order(result,customer_name,{
        "base_url" : st.session_state['api_domain'],
        "access_token" : st.session_state['access_token'],
        "organization_id" : st.session_state['organization_id'],
    })
    st.success("Sales order created successfully!")

def main():
    st.title("PO PDF Processor")

# File upload
    uploaded_file = st.file_uploader("Upload PO PDF", type="pdf")
    print(uploaded_file)

# Format selection radio button
    po_format = st.radio("Select PO Format", ["PPUS", "AZA"])

    if uploaded_file is not None:
    # Save uploaded file to a temporary location
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_file:
            temp_file.write(uploaded_file.getvalue())
            temp_path = temp_file.name
    
    # Process button
        if st.button("Process PDF"):
            with st.spinner("Processing..."):
            # Call the appropriate function based on the selected format
                result = None
                if po_format == "PPUS":
                    result = pdf_extract__po_details_ppus(temp_path)
                else:  # AZA
                    result = pdf_extract__po_details_aza(temp_path)
            
            # Display the result
                st.json(result)
                st.button("Create Sales Order",on_click=on_click_create_sales_order,args=(result,po_format))
    
    # Clean up temp file
        os.unlink(temp_path)

main()