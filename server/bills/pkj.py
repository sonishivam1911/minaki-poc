import json
import re
from datetime import datetime, timedelta

def process_bills_pkj(lines):    
    # Extract GSTIN
    gstin = None
    for line in lines:
        if "GSTIN : " in line:
            gstin = line.split("GSTIN : ")[1].split(" ")[0]
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
        if "Dated : " in line:
            billdt_match = re.search(r"(\d{2}[-.]\d{2}[-.]\d{4})", line)  # Handle both formats
            if billdt_match:
                billdt = datetime.strptime(billdt_match.group(1).replace(".", "-"), "%d-%m-%Y").strftime("%Y-%m-%d")
                break

    # Calculate Due Date (30 days after billdt)
    billddt = None
    if billdt:
        billddt = (datetime.strptime(billdt, "%Y-%m-%d") + timedelta(days=30)).strftime("%Y-%m-%d")


    # Extract Discounts
    discount = 0.0
    for line in lines:
        if "Less : Discount @" in line:
            discount_match = re.search(r"(\d+,\d+|\d+\.\d+|\d+)$", line)
            if discount_match:
                discount = float(discount_match.group().replace(",", ""))
            break

    # Extract Shipping (Freight & Forwarding Charges)
    shipping = 0.0
    for line in lines:
        if "Add : Freight & Forwarding Charges" in line:
            shipping_match = re.search(r"(\d+,\d+|\d+\.\d+|\d+)$", line)
            if shipping_match:
                shipping = float(shipping_match.group().replace(",", ""))
            break

    # Extract Tax Amount
    tax_amount = 0.0
    for line in lines:
        if "Add : IGST @" in line:
            tax_match = re.search(r"(\d+,\d+|\d+\.\d+|\d+)$", line)
            if tax_match:
                tax_amount = float(tax_match.group().replace(",", ""))
            break

    # Extract Total Amount
    total_amt = 1.0
    for line in lines:
        if "Grand Total " in line:
            total_amt = line.split("Pcs. ")[1].split(" ")[0]
            if total_amt:
                total_amt = float(total_amt.replace(",", ""))
            break

    net_amt = total_amt - tax_amount - discount + shipping
    fact = net_amt/total_amt

    # Extract Product Details
    product_lines = []
    product_start = False
    for line in lines:
        if re.match(r"^\d+\.\s", line):  # Starts with "1.", "2.", etc.
            product_start = True
        if product_start:
            product_lines.append(line)
    
    line_items = []
    for line in product_lines:
        match = re.search(r"(.+)\((.+)\)\s(\d{4})\s(\d+\.\d+)\sPcs\.\s([\d,]+\.\d+|\d+)\s([\d,]+\.\d+|\d+)", line)
        if match:
            description, item_code, hsn, qty, rate, amount = match.groups()
            line_items.append({
                'account_id': '1923531000000000567', 
                'account_name': 'Cost of Goods Sold',
                "description": description.strip(),
                "rate": float(rate.replace(",", "")),
                "quantity": int(float(qty)),
                "tax_id": "1923531000000032071" ,
                "item_total": round(float(amount.replace(",", ""))*fact,2),
                "unit": "unit",
                "hsn_or_sac": hsn + "00"
            })

    # Construct Payload
    payload = {
        "vendor_id": '1923531000002360928',
        "bill_number": bill,
        "date": billdt,
        "due_date": billddt,
        "is_inclusive_tax": False,
        "line_items": line_items,
        "gst_treatment": "business_gst",
        "gst_no": gstin,
        "adjustment": shipping - discount
    }

    return payload

# # Example usage
# invoice_data = "PK2058.pdf"

# # Generate payload
# payload = process_bills_pkj(invoice_data)
# print(json.dumps(payload, indent=4))
