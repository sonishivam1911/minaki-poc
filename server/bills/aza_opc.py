import json
import re
from datetime import datetime, timedelta

def process_bills_aza_opc(lines):
        # Extract GSTIN
        gstin = '27AAACO7149M1ZZ'
        bill = None
        billdt = None
        billddt = None
        price = None
        for line in lines:
            if "MUM/B2B/OPC" in line:
                parts = line.split()
                billdt = parts[-1]
                billdt = datetime.strptime(billdt, "%d-%b-%y").strftime("%Y-%m-%d")
                billddt = billdt
                bill = parts[-2]
                break
        for line in lines:
            if "Charges-B2B" in line:
                price = line.split()[-1]
                price = re.sub(r",", "", price)  # Remove commas
                try:
                    price = float(price)
                except ValueError:
                    price = price
                break

        # Extract type of bill
        desc = None
        for line in lines:
                desc = "Order Processing Charges-B2B"
                break

        # Assign Tax ID based on GSTIN
        taxid = "1923531000000027456"

        # Construct Payload
        payload = {
            "vendor_id": '1923531000004880377',
            "bill_number": bill,
            "date": billdt,
            "due_date": billddt,
            "is_inclusive_tax": False,
            "line_items": [
                {
                    'account_id': '1923531000004880423', 
                    'account_name': 'Order Processing Charges',
                    "description": desc,
                    "rate": price,
                    "quantity": 1,
                    "tax_id": taxid,
                    "item_total": price,
                    "unit": "unit",
                    "hsn_or_sac": 999799
                }
            ],
            "gst_treatment": "business_gst",
            "gst_no": gstin
        }

        return payload

# # Example usage
# invoice_data = "inv87.pdf"

# # Generate payload
# payload = process_bills_sr(invoice_data)
# print(json.dumps(payload, indent=4))
