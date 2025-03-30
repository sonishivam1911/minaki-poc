import streamlit as st
import tempfile
import pdfplumber
import os
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from utils.zakya_api import fetch_object_for_each_id, put_record_to_zakya
from server.bills.dial import process_bills_dial, process_bills
from server.bills.pkj import process_bills_pkj
from server.bills.taj import process_bills_taj
from server.bills.shiprocket import process_bills_sr
from server.bills.np import process_bills_np
from server.bills.aza_opc import process_bills_aza_opc
from server.bills.zakya import process_bills_zakya
from server.file_management.main_file_management import upload_to_drive

# Configure logging (Set to WARNING to suppress unnecessary logs)
logging.basicConfig(level=logging.WARNING, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Adjust if needed

executor = ThreadPoolExecutor(max_workers=5)

# def on_click_bill(api, access, orgid):
#     result = None
#     result = fetch_object_for_each_id(api,access,orgid,
#             'bills/1923531000003563903'
#     ) 
#     return result

async def process_pdf(temp_path):
    try:
        logger.info(f"Processing PDF: {temp_path}")

        # Open PDF asynchronously
        pdf = await asyncio.to_thread(pdfplumber.open, temp_path)
        with pdf:
            pages = pdf.pages
            if not pages:
                logger.warning(f"Empty PDF: {temp_path}")
                return {"error": "Empty PDF"}

            # Extract text without debugging
            extracted_text = [page.extract_text() for page in pages if page.extract_text()]
            lines = "\n".join(extracted_text).split("\n")

        # Determine processing function (No logging of extracted text)
        result = None
        for line in lines[:15]:  # Only check first 15 lines
            if line.startswith("DELHI INTERNATIONAL AIRPORT LIMITED"):
                result = process_bills_dial(lines)
                break
            elif line.startswith("AAMIR KHAN'S COLLECTION"):
                result = process_bills_pkj(lines)
                break
            elif line.startswith("Taj Trade And Transport Co. Ltd."):
                result = process_bills_taj(lines)
                break
            elif line.startswith("Shiprocket Private Limited"):
                result = process_bills_sr(lines)
                break
            elif line.startswith("N.P. JEWELLERS"):
                result = process_bills_np(lines)
                break
            elif line.startswith("Aza Fashions"):
                result = process_bills_aza_opc(lines)
                break
            elif line.startswith("ZOHO Corporation Private Limited"):
                result = process_bills_zakya(lines)
                break

        if not result:
            logger.warning(f"Unknown bill format: {temp_path}")
            return {"error": "Unknown bill format"}

        # API call
        bill_data = await asyncio.to_thread(
            process_bills,
            result,
            {
                "base_url": st.session_state.get('api_domain', ''),
                "access_token": st.session_state.get('access_token', ''),
                "organization_id": st.session_state.get('organization_id', ''),
            }
        )

        logger.info(f"✅ Successfully processed: {temp_path}")
        billid = bill_data["bill"]["bill_id"]
        serial_number = bill_data["bill"]["bill_number"]
        function_date = bill_data["bill"]["date"]
        link = upload_to_drive(temp_path, 'bill', serial_number, function_date)
        payload = {
            "custom_fields": [
                    {
                        "api_name": "cf_bills_drive_link",
                        "placeholder": "cf_bills_drive_link",
                        "value": link
                    }
                ]
        }
        addlink = put_record_to_zakya(st.session_state.get('api_domain', ''), 
                                      st.session_state.get('access_token', ''), 
                                      st.session_state.get('organization_id', ''), 
                                      'bills', billid, payload)
        return addlink['bill']

    except Exception as e:
        logger.error(f"❌ Error processing {temp_path}: {str(e)}")
        return {"error": str(e)}

async def process_multiple_pdfs(uploaded_files):
    if not uploaded_files:
        st.error("Please upload at least one PDF file.")
        return

    temp_files = []
    for uploaded_file in uploaded_files:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_file:
            temp_file.write(uploaded_file.getvalue())
            temp_files.append(temp_file.name)

    total_files = len(temp_files)
    progress_bar = st.progress(0)

    # Process PDFs concurrently
    results = await asyncio.gather(*[process_pdf(temp_path) for temp_path in temp_files])

    # Cleanup temp files
    for temp_path in temp_files:
        os.unlink(temp_path)

    # Display results
    for i, result in enumerate(results):
        st.subheader(f"Result for File {i+1}")
        st.json(result)

    progress_bar.progress(100)
    st.success(f"✅ {total_files} bills processed successfully!")

def main():
    st.title("Bulk Bill Processor")

    # File upload
    uploaded_files = st.file_uploader("Upload Bill PDFs", type="pdf", accept_multiple_files=True)

    if uploaded_files and st.button("Process PDFs"):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(process_multiple_pdfs(uploaded_files))

if __name__ == "__main__":
    main()
