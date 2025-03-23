import json
import re
from datetime import datetime
from utils.zakya_api import post_record_to_zakya, fetch_records_from_zakya, retrieve_record_from_zakya, attach_zakya

def process_bills_dial(lines):
        # Extract GSTIN
        gstin = None
        for line in lines:
            if "MINAKI " in line:
                gstin = line.split("MINAKI GSTIN : ")[1].split(" ")[0]
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
            if "Bill Date : " in line:
                billdt_match = re.search(r"(\d{2}.\d{2}.\d{4})", line)
                # billdt_match = billdt_match.strftime("%Y-%m-%d")
                billdt = datetime.strptime(billdt_match.group(1), "%d.%m.%Y").strftime("%Y-%m-%d")
                break
        
        # Calculate Due Date (7 days after billdt)
        billddt = None
        for line in lines:
            if "Due Date : " in line:
                billddt_match = re.search(r"(\d{2}.\d{2}.\d{4})", line)
                # billddt_match = billddt_match.strftime("%Y-%m-%d")
                billddt = datetime.strptime(billddt_match.group(1), "%d.%m.%Y").strftime("%Y-%m-%d")
                break
        
        # Extract Price 
        price = None
        for line in lines:
            if line.startswith("Grand Total "):
                pr = line.split("Grand Total ")[1].split(" ")[0]
                price = re.sub(r",", "", pr) 
                price = re.sub(r"-", "", price)  # Remove commas
                try:
                    price = float(price)
                except ValueError:
                    price = price
                break
        print(price)
        
        # Extract type of bill
        desc = None
        for line in lines:
            if "Minimum Guarantee" in line:
                desc = "MINAKI Temporary Jewellry Kiosk(Cart) - Minimum Guarantee"
                break
            elif "Revenue Share" in line:
                desc = "MINAKI Temporary Jewellry Kiosk(Cart) - Revenue Share"
                break

        # Assign Tax ID based on GSTIN
        taxid = "1923531000000027522" if gstin and gstin.startswith("07") else "1923531000000027456"
        
        # Construct Payload
        payload = {
            "vendor_id": '1923531000002360928',
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
                    "unit": "unit",
                    "hsn_or_sac": 997212
                }
            ],
            "gst_treatment": "business_gst",
            "gst_no": gstin
        }
        
        
        return payload
    
def process_bills(payload,zakya_config):
    result = None
    result = post_record_to_zakya(
            'https://api.zakya.in/',
            zakya_config['access_token'],  
            zakya_config['organization_id'],
            'bills',
            payload
    ) 
    return result

def attach_pdf(bill_id,pdf_link,zakya_config):
    result = None
    endpoint = f'bills/{bill_id}/attachment'
    result = attach_zakya(
            zakya_config['base_url'],
            zakya_config['access_token'],  
            zakya_config['organization_id'],
            endpoint,
            pdf_link
    ) 
    return result

# # Example usage
# invoice_data = "mg.pdf"

# # Generate payload
# payload = process_bills_dial(invoice_data)
# print(json.dumps(payload, indent=4))
