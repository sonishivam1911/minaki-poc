import json
import re
from datetime import datetime, timedelta

def process_bills_np(lines):
    # Extract GSTIN
    gstin = None
    for line in lines:
        if "GSTIN : " in line:
            gstin = line.split("GSTIN : ")[1].split()[0]
            break

    # Extract Bill Number
    bill = None
    for line in lines:
        if "Invoice No. : " in line:
            bill = line.split("Invoice No. : ")[1].split()[0]
            break

    # Extract Bill Date
    billdt = None
    for line in lines:
        if "Dated : " in line:
            billdt_match = re.search(r"(\d{2}[-.]\d{2}[-.]\d{4})", line)
            if billdt_match:
                billdt = datetime.strptime(billdt_match.group(1).replace(".", "-"), "%d-%m-%Y").strftime("%Y-%m-%d")
                break

    # Calculate Due Date (30 days after billdt)
    billddt = None
    if billdt:
        billddt = (datetime.strptime(billdt, "%Y-%m-%d") + timedelta(days=30)).strftime("%Y-%m-%d")
    
    # Extract Product Details
    product_lines = []
    product_start = False
    for line in lines:
        if re.match(r"^\d+\.\s", line):  # Starts with "1.", "2.", etc.
            product_start = True
        if product_start and line.strip():
            product_lines.append(line)
    
    line_items = []
    for line in product_lines:
        parts = line.strip().split()
        
        if len(parts) < 6:
            continue  # Skip invalid lines
        
        try:
            amount = float(parts[-1].replace(',', ''))
            unit_price = float(parts[-2].replace(',', ''))
            unit = parts[-3]
            qty = float(parts[-4])
            hsn_sac = parts[-5]
            description = " ".join(parts[1:-5])  # Join everything in between as description
            
            line_items.append({
                'account_id': '1923531000000000567', 
                'account_name': 'Cost of Goods Sold',
                "description": description.strip(),
                "rate": unit_price,
                "quantity": int(qty),
                "tax_id": "1923531000000027522" if gstin and gstin.startswith("07") else "1923531000000027456",
                "item_total": round(amount, 2),
                "unit": 'pcs',
                "hsn_or_sac": hsn_sac
            })
        except ValueError:
            continue  # Skip lines with incorrect number formats

    # Construct Payload
    payload = {
        "vendor_id": 1923531000004880003,
        "bill_number": bill,
        "date": billdt,
        "due_date": billddt,
        "is_inclusive_tax": False,
        "line_items": line_items,
        "gst_treatment": "business_gst",
        "gst_no": gstin
    }

    return payload

# # Example usage
# bill_data = "np1.pdf"

# # Generate payload
# payload = process_bills_np(bill_data)
# print(json.dumps(payload, indent=4))