
import pdfplumber
from datetime import datetime
import fitz  # PyMuPDF
import re
import pandas as pd
from utils.zakya_api import post_record_to_zakya
from utils.postgres_connector import crud
from config.logger import logger

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
        "Product Link": None,
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
                        fields_temp = {
                            "SKU": designer_code,
                            "Partner SKU": product_id,
                            "Description": product_title,
                            "Size": size,
                            "Order Source" : None,
                            "Quantity": quantity,
                            "Unit Price": cost,
                            "Other Costs": customization_charges,
                            "Total": total_cost,
                            "Product Link": None
                        }

                        fields = {**fields, **fields_temp}

                j += 1  # Move to the next product entry

        # Extract Order Processing Charges (OPC)
        elif "Order Processing Charges" in line:
            opc_value = re.findall(r"\(\d+\)", line)
            fields["Other Costs"] = fields["Other Costs"] + opc_value[0].replace("(", "").replace(")", "") if opc_value else None

        i += 1

    return fields


def format_date_for_api(date_str):
    try:
        # Try to parse the date from the PDF - adjust formats as needed
        # Common formats might include "DD/MM/YYYY", "MM/DD/YYYY", "YYYY-MM-DD"
        for fmt in ["%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d.%m.%Y"]:
            try:
                parsed_date = datetime.strptime(date_str.strip(), fmt)
                # Convert to the API-required format (ISO format)
                return parsed_date.strftime("%Y-%m-%d")
            except ValueError:
                continue
        # If none of the formats worked, try a more relaxed approach
        return date_str.strip()
    except Exception as e:
        print(f"Error formatting date {date_str}: {e}")
        return date_str


def process_sales_order(fields, customer_name, zakya_config):
    """Checks if a Sales Order exists for the given reference number and creates one if not."""
    mapping_product = crud.read_table("zakya_products")
    mapping_order = crud.read_table("zakya_sales_order")
    mapping_partner = crud.read_table("mapping__partner")
    #logger.debug(f"mapping partner is : {mapping_partner}")
    customer_matches = mapping_partner[mapping_partner["Display Name"] == customer_name]
    #logger.debug(f"customer matched is {customer_matches}")
    if len(customer_matches) > 0:
        customer_id = customer_matches["Contact ID"].iloc[0]
        #logger.debug(f"customer_id is {customer_id}")
    # else:
    #     print(f"No customer found with name: {customer_name}")
    #     # Use a default or raise an exception
    #     customer_id = default_customer_id     
    # customer_id = mapping_partner[["Display Name"] == customer_name]["Contact ID"][0]
    print(mapping_product.columns)
    # Step 2: Prepare lookup sets
    if isinstance(mapping_product, str):
        import json
        mapping_product = json.loads(mapping_product)  # Convert JSON string to dict

    if isinstance(mapping_product, dict):
        mapping_product = pd.DataFrame(mapping_product.get("items", []))  # Adjust based on API response
    
    item_id = None
    existing_skus = set(mapping_product["sku"].astype(str).dropna())
    filtered_products = mapping_product[mapping_product["sku"] == fields["SKU"]]
    if not filtered_products.empty:
        item_id = filtered_products["item_id"].iloc[0]
    else:
        # Handle the case where no matching SKU was found
        print(f"No product found with SKU: {fields['SKU']}")       

    reference_number = fields.get("PO No")
    os = fields.get("Order Source")
    if not reference_number:
        print("Reference number is missing!")
        return
    
    existing_orders = mapping_order
    for _,order in existing_orders.iterrows():
        logger.debug(f"order is {order}")
        po_refnum = order.get("reference_number")
        po_refnum = re.sub(r"PO:\s*", "", po_refnum) 
        if po_refnum == reference_number:
            print(f"Sales Order with reference number {reference_number} already exists.")
            return
    
    salesorder_payload = {
        "customer_id": int(customer_id),
        "date": format_date_for_api(fields["PO Date"]),
        "shipment_date": format_date_for_api(fields["PO Delivery Date"]),
        "reference_number": reference_number,
        "custom_fields": [
                    {
                        "index": 1,
                        "label": "Status",
                        "api_name": "cf_status",
                        "placeholder": "cf_status",
                        "value": "Created"
                    },
                    {
                        "index": 2,
                        "label": "Order Type",
                        "api_name": "cf_order_type",
                        "placeholder": "cf_order_type",
                        "value": 'eCommerce Order'
                    }
                ],
        "line_items": [
            {
                "item_id": int(item_id) if item_id else '',
                "description": f"PO: {reference_number}",
                "rate": int(fields["Unit Price"]),
                "quantity": int(fields["Quantity"]),
                "item_total": int(fields["Total"])
            }
        ],
        "notes": f"Order Source : {os}",
        "terms": "Terms and Conditions"
    }
    
    print(f"Creating a new Sales Order with reference number {reference_number}...")
    post_record_to_zakya(
        zakya_config['base_url'],
        zakya_config['access_token'],  
        zakya_config['organization_id'],
        'salesorders',
        salesorder_payload
    )    


# pdf_path = "po.pdf"
# po_data = pdf_extract__po_details_ppus(pdf_path)
# print(json.dumps(po_data, indent=4))
# po = po_data["PO No"]
# os = po_data["Order Source"]
# po_item_id = ""
# ppus_code  = po_data["SKU Code"]
# sales_order_data = {
#     "customer_id": 1923531000000170000,
#     "salesorder_number": po,
#     "date": po_data["PO Date"],
#     "shipment_date": po_data["PO Delivery Date"],
#     "reference_number": po,
#     "line_items": [
#         {
#             "item_id": po_item_id,
#             "description": f"PO: {po} and PPUS Code: {ppus_code}",
#             "rate": po_data["Unit Price"],
#             "quantity": po_data["Quantity"],
#             "item_total": po_data["Total"]
#         }
#     ],
#     "notes": f"Order Source : {os}",
#     "terms": "Terms and Conditions"
# }

# # Process the sales order
# process_sales_order(sales_order_data)
