import json
import re
import pdfplumber
from datetime import datetime, timedelta

def process_bills_sr(lines):
        # Extract GSTIN
        gstin = None
        for line in lines:
            if "GSTIN: " in line:
                gstin = line.split("GSTIN: ")[1].split(" ")[0]
                break
        
        # Extract Bill Number
        bill = None
        for line in lines:
            if "Invoice No. : " in line:
                bill = line.split("Invoice No. : ")[1].split(" ")[0]
                break
        
        # Extract Bill Date
        billdt = None
        for line in lines:
            if "Invoice Date : " in line:
                billdt_match = re.search(r"(\d{2}/\d{2}/\d{4})", line)
                # billdt_match = billdt_match.strftime("%Y-%m-%d")
                billdt = datetime.strptime(billdt_match.group(1), "%d/%m/%Y").strftime("%Y-%m-%d")
                break
        
        # Calculate Due Date (7 days after billdt)
        billddt = None
        for line in lines:
            if "Due Date : " in line:
                billddt_match = re.search(r"(\d{2}/\d{2}/\d{4})", line)
                # billddt_match = billddt_match.strftime("%Y-%m-%d")
                billddt = datetime.strptime(billddt_match.group(1), "%d/%m/%Y").strftime("%Y-%m-%d")
                break
        
        # Extract Price 
        price = None
        for line in lines:
            if line.startswith("996812"):
                pr = line.split("Shiprocket V2 Freight* ")[1].split(" ")[0]
                price = re.sub(r",", "", pr)  # Remove commas
                try:
                    price = float(price)
                except ValueError:
                    price = price
                break
        
        
        # Extract type of bill
        desc = None
        for line in lines:
                desc = "Shiprocket V2 Freight*"
                break

        # Assign Tax ID based on GSTIN
        taxid = "1923531000000027456"
        
        # Construct Payload
        payload = {
            "vendor_id": '1923531000001566458',
            "bill_number": bill,
            "date": billdt,
            "due_date": billddt,
            "is_inclusive_tax": False,
            "line_items": [
                {
                    'account_id': '1923531000000027253', 
                    'account_name': 'Transportation Expense',
                    "description": desc,
                    "rate": price,
                    "quantity": 1,
                    "tax_id": taxid,
                    "item_total": price,
                    "unit": "unit",
                    "hsn_or_sac": 997212
                }
            ],
            "gst_treatment": "business_gst",
            "gst_no": gstin
        }
        
        return payload

# # Example usage
# invoice_data = "srf.pdf"

# # Generate payload
# payload = process_bills_sr(invoice_data)
# print(json.dumps(payload, indent=4))
