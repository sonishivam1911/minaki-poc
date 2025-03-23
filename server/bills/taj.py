import json
import re
from datetime import datetime, timedelta

def process_bills_taj(lines):
    # Extract GSTIN (Generalized search)
    gstin = None
    for line in lines:
        match = re.search(r"GSTIN\s*[:/-]\s*([\w\d]+)", line)
        if match:
            gstin = match.group(1)
            break

    # Extract Bill Number & Store Code
    bill, store_code, vid = None, None, None
    for line in lines:
        match = re.search(r"Invoice No. : (\S+)", line)
        if match:
            bill = match.group(1)
            store_code = bill.split("-")[1].split("/")[0] if "-" in bill else None
            break

    vendor_mapping = {
        'TMC': '1923531000003042024',
        'TPH': '1923531000002349593',
        'TWE': '1923531000003042076',
        'TWR': '1923531000002349659'
    }
    vid = vendor_mapping.get(store_code)

    # Extract Bill Date
    billdt = None
    for line in lines:
        match = re.search(r"Invoice Date : (\d{2}/\d{2}/\d{4})", line)
        if match:
            billdt = datetime.strptime(match.group(1), "%d/%m/%Y").strftime("%Y-%m-%d")
            break

    # Calculate Due Date (30 days after billdt)
    billddt = None
    if billdt:
        billddt = (datetime.strptime(billdt, "%Y-%m-%d") + timedelta(days=30)).strftime("%Y-%m-%d")

    # Extract Total Price (Last numeric value before discounts/taxes)
    price = None
    for i, line in enumerate(lines):
        if "Grand Total" in line:
            price = re.sub(r",", "", line.split()[-1])
            try:
                price = float(price)
            except ValueError:
                price = None
            break

    # Extract Description (Generalized)
    desc_lines = []
    capture_desc = False
    for line in lines:
        if re.search(r"Lice|Licence Fee|Rent", line, re.IGNORECASE):
            capture_desc = True
        if "CJE" in line:  # Stop at this identifier
            break
        if capture_desc:
            desc_lines.append(line)
    
    desc = " ".join(desc_lines).strip() if desc_lines else "Licence Fee"

    # Assign Tax ID based on GSTIN
    taxid = "1923531000000027522" if store_code == "TPH" else "1923531000000027456"

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

    return payload

# Example Usage
# sample_lines = [
#     "GSTIN : 07ABJFM2026Q1ZW",
#     "Invoice No. : TMC-1234/25",
#     "Invoice Date : 04/03/2025",
#     "SNo Description of Goods HSN/SAC Qty. Unit Price Amount",
#     "1. Sample Item 996211 2 Pcs 100.00 200.00",
#     "Grand Total 2 Pcs 7,990.00",
# ]

# payload = process_bills_taj(sample_lines)
# print(json.dumps(payload, indent=4))
