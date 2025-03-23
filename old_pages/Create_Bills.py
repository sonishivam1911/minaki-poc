import streamlit as st
import tempfile
import pdfplumber
import os
from utils.zakya_api import extract_record_list
from server.bills.dial import process_bills_dial, process_bills
from server.bills.pkj import process_bills_pkj
from server.bills.taj import process_bills_taj


def on_click_create_bill(result):
    process_bills(result,{
        "base_url" : st.session_state['api_domain'],
        "access_token" : st.session_state['access_token'],
        "organization_id" : st.session_state['organization_id'],
    })
    st.success("Bill saved successfully!")

def main():
    st.title("DIAL Bill Processor")

# File upload
    uploaded_file = st.file_uploader("Upload Bill PDF", type="pdf")
    print(uploaded_file)

    if uploaded_file is not None:
    # Save uploaded file to a temporary location
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_file:
            temp_file.write(uploaded_file.getvalue())
            temp_path = temp_file.name
            print(temp_path)
    
    # Process button
        if st.button("Process PDF"):
            with st.spinner("Processing..."):
            # Call the appropriate function based on the selected format
                with pdfplumber.open(temp_path) as pdf:
                    data = "\n".join([page.extract_text() for page in pdf.pages if page.extract_text()])
                    lines = data.split("\n")
                    for line in lines:
                        if line.startswith("DELHI INTERNATIONAL AIRPORT LIMITED"):
                            result = process_bills_dial(lines)
                            break
                        elif line.startswith("AAMIR KHAN'S COLLECTION"):
                            result = process_bills_pkj(lines)
                            break
                        elif line.startswith("Taj Trade And Transport Co. Ltd."):
                            result = process_bills_taj(lines)
                            break
                    st.json(result)
                    bill_data = process_bills(result,{
                        "base_url" : st.session_state['api_domain'],
                        "access_token" : st.session_state['access_token'],
                        "organization_id" : st.session_state['organization_id'],
                    })
                    # r2 = extract_record_list(bill_data,"bills")
                    st.success("Bill saved successfully!")
                    print(bill_data)
                    st.json(bill_data['bill'])
                    # bill_id = bill_data['bill']['bill_id']
                    # attach_bill = attach_pdf(bill_id, temp_path,{
                    #     "base_url" : st.session_state['api_domain'],
                    #     "access_token" : st.session_state['access_token'],
                    #     "organization_id" : st.session_state['organization_id'],
                    # })
                    # st.json(attach_bill)
                    # st.button("Create Sales Order",on_click=on_click_create_bill,args=(result))
    
    # Clean up temp file
        # os.unlink(temp_path)

main()