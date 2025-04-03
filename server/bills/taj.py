import json
import re
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def process_bills_taj(lines):
    logger.info("Starting bill processing...")
    
    # Extract GSTIN
    gstin = None
    for line in lines:
        match = re.search(r"GSTIN\s*[:/-]\s*([\w\d]+)", line)
        if match:
            gstin = match.group(1)
            logger.info(f"Extracted GSTIN: {gstin}")
            break

    # Extract Bill Number & Store Code
    bill, store_code, vid = None, None, None
    for line in lines:
        match = re.search(r"Invoice No. : (\S+)", line)
        if match:
            bill = match.group(1)
            store_code = bill.split("-")[1].split("/")[0] if "-" in bill else None
            logger.info(f"Extracted Bill No: {bill}, Store Code: {store_code}")
            break
    
    vendor_mapping = {
        'TMC': '1923531000003042024',
        'TPH': '1923531000002349593',
        'TWE': '1923531000003042076',
        'TWR': '1923531000002349659'
    }
    vid = vendor_mapping.get(store_code)
    if vid:
        logger.info(f"Mapped Vendor ID: {vid}")
    else:
        logger.warning("Vendor ID not found for store code")

    # Extract Bill Date
    billdt = None
    for line in lines:
        match = re.search(r"Invoice Date : (\d{2}/\d{2}/\d{4})", line)
        if match:
            billdt = datetime.strptime(match.group(1), "%d/%m/%Y").strftime("%Y-%m-%d")
            logger.info(f"Extracted Bill Date: {billdt}")
            break

    # Calculate Due Date (30 days after bill date)
    billddt = None
    if billdt:
        billddt = (datetime.strptime(billdt, "%Y-%m-%d") + timedelta(days=30)).strftime("%Y-%m-%d")
        logger.info(f"Calculated Due Date: {billddt}")
    
    # Extract Total Price
    price = None
    for line in lines:
        if "Bill Amount Including GST : " in line:
            price = re.sub(r",", "", line.split('Bill Amount Including GST : ')[1].split()[0])
            try:
                price = float(price)/1.18
                logger.info(f"Extracted Total Price: {price}")
            except ValueError:
                logger.error("Failed to extract Total Price")
                price = None
            break
    
    # Extract Description
    desc_lines = []
    capture_desc = False
    for line in lines:
        if re.search(r"Lice|Licence Fee|Rent", line, re.IGNORECASE):
            capture_desc = True
        if "CJE" in line:
            break
        if capture_desc:
            desc_lines.append(line)
    
    desc = " ".join(desc_lines).strip() if desc_lines else "Licence Fee"
    logger.info(f"Extracted Description: {desc}")
    
    # Assign Tax ID based on GSTIN
    taxid = "1923531000000027522" if store_code == "TPH" else "1923531000000027456"
    logger.info(f"Assigned Tax ID: {taxid}")
    
    # Construct Payload
    payload = {
        "vendor_id": vid,
        "bill_number": bill,
        "date": billdt,
        "due_date": billddt,
        "is_inclusive_tax": False,
        "line_items": [
            {
                "account_name": "Rent Expense",
                "account_id": '1923531000000000528',
                "description": desc,
                "rate": price,
                "quantity": 1,
                "tax_id": taxid,
                "item_total": price,
                "unit": "qty",
                "hsn_or_sac": 996211
            }
        ],
        "gst_treatment": "business_gst"
    }
    
    logger.info("Bill processing completed successfully.")
    return payload
