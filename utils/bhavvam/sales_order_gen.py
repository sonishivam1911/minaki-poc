import requests
import pdfplumber
import json
import fitz 
import re
import pandas as pd
from utils.zakya_api import list_all_sales_orders, create_sales_order
from utils.zakya_api import (list_all_payments, update_payment, fetch_records_from_zakya, extract_record_list)
from utils.postgres_connector import crud

fields = {
        "PO No": None,
        "PO Date": None,
        "PO Delivery Date": None,
        "Order Source": None,
        "SKU": None,
        "Order Ref No": None,
        "Partner SKU": None,
        "Description": None,
        "Quantity": None,
        "Unit Price": None,
        "Other Costs": None,
        "Total": None,
        "Size": None,
        "Product Link": None
    }

def pdf_extract__po_details_ppus(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        text = "\n".join([page.extract_text() for page in pdf.pages if page.extract_text()])

    lines = text.split("\n")
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if "PO No" in line and i + 1 < len(lines):
            fields["PO No"] = lines[i + 1].strip().split()[0]
        elif "PO Date" in line:
            fields["PO Date"] = line.split("PO Date")[-1].strip()
        elif "PO Delivery Date" in line:
            fields["PO Delivery Date"] = line.split("PO Delivery Date")[-1].strip()
        elif "Order Source" in line:
            fields["Order Source"] = line.split("Order Source")[-1].strip()
        elif "Vendor Code" in line:
            fields["SKU"] = line.split("Vendor Code")[-1].strip()
        elif "Order Ref No" in line:
            fields["Order Ref No"] = line.split("Order Ref No")[-1].strip()
        elif "SKU Code" in line:
            sku_code = line.split("SKU Code")[-1].strip()
            fields["Partner SKU"] = sku_code
            fields["Product Link"] = f"https://dimension-six.perniaspopupshop.com/skuDetail.php?sku={sku_code}"
        elif "Description" in line:
            desc = ""
            for j in range(i + 1, len(lines)):  # Start from the next line of SKU
                if "Quantity" in lines[j]:  # Stop when reaching "Quantity"
                    break
                if lines[j].strip():  # Append non-empty lines to description
                    desc += " " + lines[j].strip()
            fields["Description"] = desc.strip()
        elif "Quantity" in line:
            fields["Quantity"] = line.split("Quantity")[-1].strip()
        elif "Unit Price" in line:
            fields["Unit Price"] = line.split("Unit Price")[-1].strip()
        elif "Other Costs" in line:
            fields["Other Costs"] = line.split("Other Costs")[-1].strip()
        elif "Total" in line:
            fields["Total"] = line.split("Total")[-1].strip()
        elif "Size" in line:
            fields["Size"] = line.split("Size", 1)[1]
        i += 1  # Move to the next line
    return fields


def pdf_extract__po_details_aza(pdf_path):
    doc = fitz.open(pdf_path)
    text = "\n".join([page.get_text("text") for page in doc])
    fields = {}
    lines = text.split("\n")
    i = 0

    while i < len(lines):
        line = lines[i].strip()

        # Extract PO Details
        if "PO Number:" in line:
            fields["PO No"] = line.split("PO Number:")[-1].strip()
        elif "PO Date:" in line:
            fields["PO Date"] = line.split("PO Date:")[-1].strip()
        elif "Delivery Date:" in line:
            fields["PO Delivery Date"] = line.split("Delivery Date:")[-1].strip()

        # Identify Product Information Section
        elif "DESIGNER" in line and "CODE" in lines[i + 1] and "PRODUCT ID" in lines[i + 2]:
            j = i + 11  # Skip table headers
            while j < len(lines):

                # Identify Designer Code (Starts with M, No Spaces)
                if re.match(r"^M[A-Za-z0-9]+$", lines[j]):
                    designer_code = lines[j].strip()
                    j += 1

                    # Identify Product ID (6-digit number)
                    product_id = lines[j].strip() if re.match(r"^\d{6}$", lines[j]) else None
                    j += 1

                    # Extract Product Title (Multi-line Handling)
                    product_title = []
                    while j < len(lines) and not re.match(r"^[A-Z]+$", lines[j]) and not lines[j].isdigit():
                        product_title.append(lines[j].strip())
                        j += 1
                    product_title = " ".join(product_title).strip()  # Merge title parts

                    # Extract Size
                    size = lines[j].strip()
                    if size == "FREE":
                        if j + 1 < len(lines) and lines[j + 1].strip() == "SIZE":
                            size = "FREE SIZE"
                            j += 1  # Move past "SIZE"
                    elif re.match(r"^(XS|S|M|L|XL|XXL|XXXL|\d{2,3})$", size):  # Capture standard sizes
                        pass
                    else:
                        size = "Unknown"  # Assign "Unknown" if size is missing or incorrect

                    j += 1  # Move past SIZE field


                    if j + 3 < len(lines):  # Ensure there are enough elements
                        quantity = lines[j].strip()
                        cost = lines[j + 1].strip()
                        customization_charges = lines[j + 2].strip()
                        total_cost = lines[j + 3].strip()
                        j += 4

                        # Store extracted product details
                        fields.append({
                            "SKU": designer_code,
                            "Partner SKU": product_id,
                            "Description": product_title,
                            "Size": size,
                            "Quantity": quantity,
                            "Unit Price": cost,
                            "Other Costs": customization_charges,
                            "Total": total_cost,
                            "Product Link": None
                        })

                j += 1  # Move to the next product entry

        # Extract Order Processing Charges (OPC)
        elif "Order Processing Charges" in line:
            opc_value = re.findall(r"\(\d+\)", line)
            fields["Other Costs"] = fields["Other Costs"] + opc_value[0].replace("(", "").replace(")", "") if opc_value else None

        i += 1

    return fields


def process_sales_order(fields, cust, base_url, access_token, organization_id):
    """Checks if a Sales Order exists for the given reference number and creates one if not."""
    mapping_product = crud.read_table("mapping__product")
    mapping_order = crud.read_table("mapping__order")
    mapping_partner = crud.read_table("mapping__partner")
    print(mapping_product)
    # Step 2: Prepare lookup sets
    if isinstance(mapping_product, str):
        import json
        mapping_product = json.loads(mapping_product)  # Convert JSON string to dict

    if isinstance(mapping_product, dict):
        mapping_product = pd.DataFrame(mapping_product.get("items", []))  # Adjust based on API response

    existing_skus = set(mapping_product["SKU"].astype(str).dropna())
    reference_number = fields.get("PO No")
    if not reference_number:
        print("Reference number is missing!")
        return
    existing_orders = mapping_order
    for order in existing_orders:
        if order.get("reference_number") == reference_number:
            print(f"Sales Order with reference number {reference_number} already exists.")
            return
    
    sales_order_data = {
        "customer_id": 1923531000000170000,
        "salesorder_number": reference_number,
        "date": fields["PO Date"],
        "shipment_date": fields["PO Delivery Date"],
        "reference_number": reference_number,
        "line_items": [
            {
                "item_id": po_item_id,
                "description": f"PO: {po} and PPUS Code: {ppus_code}",
                "rate": po_data["Unit Price"],
                "quantity": po_data["Quantity"],
                "item_total": po_data["Total"]
            }
        ],
        "notes": f"Order Source : {os}",
        "terms": "Terms and Conditions"
    }
    
    print(f"Creating a new Sales Order with reference number {reference_number}...")
    create_sales_order(base_url, access_token, organization_id, sales_order_data)


pdf_path = "po.pdf"
po_data = pdf_extract__po_details_ppus(pdf_path)
print(json.dumps(po_data, indent=4))
po = po_data["PO No"]
os = po_data["Order Source"]
po_item_id = ""
ppus_code  = po_data["SKU Code"]
sales_order_data = {
    "customer_id": 1923531000000170000,
    "salesorder_number": po,
    "date": po_data["PO Date"],
    "shipment_date": po_data["PO Delivery Date"],
    "reference_number": po,
    "line_items": [
        {
            "item_id": po_item_id,
            "description": f"PO: {po} and PPUS Code: {ppus_code}",
            "rate": po_data["Unit Price"],
            "quantity": po_data["Quantity"],
            "item_total": po_data["Total"]
        }
    ],
    "notes": f"Order Source : {os}",
    "terms": "Terms and Conditions"
}

# Process the sales order
process_sales_order(sales_order_data)
