import json
import re
from datetime import datetime, timedelta

def process_bills_zakya(lines):
        # Extract GSTIN
        gstin = None
        for line in lines:
            if "GSTIN: " in line:
                gstin = line.split("GSTIN: ")[1].split(" ")[0]
                break

        # Extract Bill Number
        bill = None
        for line in lines:
            if "INVOICE# : " in line:
                bill = line.split("INVOICE# : ")[1].split(" ")[0]
                break

        # Extract Bill HSN
        hsn = None
        for line in lines:
            if "SAC" in line:
                hsn = line.split("SAC: ")[1].split(" ")[0]
                break

        # Extract Bill Date
        billdt = None
        for line in lines:
            if "DATE : " in line:
                billdt_match = re.search(r"(\d{2} \w{3} \d{4})", line)  # Match "dd Mon YYYY"
                if billdt_match:
                    billdt = datetime.strptime(billdt_match.group(1), "%d %b %Y").strftime("%Y-%m-%d")
                    break

        # Calculate Due Date (same day)
        billddt = billdt

        # Extract Price
        price = None
        for line in lines:
            if line.startswith("Sub Total "):
                pr = line.split("Sub Total ")[1].split(" ")[0]
                price = re.sub(r",", "", pr)  # Remove commas
                try:
                    price = float(price)
                except ValueError:
                    price = price
                break


        # Extract type of bill
        desc = None
        for line in lines:
                desc = "Zakya Monthly Invoice - Premium Plan"
                break

        # Assign Tax ID based on GSTIN
        taxid = "1923531000000027456"

        # Construct Payload
        payload = {
            "vendor_id": '1923531000004748664',
            "bill_number": bill,
            "date": billdt,
            "due_date": billddt,
            "is_inclusive_tax": False,
            "line_items": [
                {
                    "account_id":"1923531000000000525",
                    "account_name":"IT and Internet Expenses",
                    "description": desc,
                    "rate": price,
                    "quantity": 1,
                    "tax_id": taxid,
                    "item_total": price,
                    "unit": "unit",
                    "hsn_or_sac": hsn
                }
            ],
            "gst_treatment": "business_gst",
            "gst_no": gstin
        }

        return payload

# # Example usage
# invoice_data = "zakya.pdf"

# # Generate payload
# payload = process_bills_zakya(invoice_data)
# print(json.dumps(payload, indent=4))
