import requests
import pdfplumber
import json

def extract_purchase_order_fields(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        text = "\n".join([page.extract_text() for page in pdf.pages if page.extract_text()])
    fields = {
        "PO No": None,
        "PO Date": None,
        "PO Delivery Date": None,
        "Order Source": None,
        "Vendor Code": None,
        "Order Ref No": None,
        "SKU Code": None,
        "SKU Description": None,
        "Quantity": None,
        "Unit Price": None,
        "Other Costs": None,
        "Total": None,
        "Size": None,
        "Product Link": None
    }

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
            fields["Vendor Code"] = line.split("Vendor Code")[-1].strip()
        elif "Order Ref No" in line:
            fields["Order Ref No"] = line.split("Order Ref No")[-1].strip()
        elif "SKU Code" in line:
            sku_code = line.split("SKU Code")[-1].strip()
            fields["SKU Code"] = sku_code
            fields["Product Link"] = f"https://dimension-six.perniaspopupshop.com/skuDetail.php?sku={sku_code}"
        elif "SKU Description" in line:
            desc = ""
            for j in range(i + 1, len(lines)):  # Start from the next line of SKU
                if "Quantity" in lines[j]:  # Stop when reaching "Quantity"
                    break
                if lines[j].strip():  # Append non-empty lines to description
                    desc += " " + lines[j].strip()
            fields["SKU Description"] = desc.strip()
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

# Replace with your actual credentials and organization ID
ZAKYA_API_BASE_URL = "https://api.zakya.com/inventory/v1"
ORGANIZATION_ID = "your_organization_id"
ACCESS_TOKEN = "your_access_token"

HEADERS = {
    "Authorization": f"Zoho-oauthtoken {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

def get_sales_orders():
    """Fetches all sales orders from Zakya."""
    url = f"{ZAKYA_API_BASE_URL}/salesorders?organization_id={ORGANIZATION_ID}"
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        return response.json().get("salesorders", [])
    else:
        print(f"Error fetching sales orders: {response.text}")
        return []

def create_sales_order(sales_order_data):
    """Creates a new sales order in Zakya."""
    url = f"{ZAKYA_API_BASE_URL}/salesorders?organization_id={ORGANIZATION_ID}"
    response = requests.post(url, headers=HEADERS, json=sales_order_data)

    if response.status_code == 201:
        print("Sales Order created successfully!")
        return response.json()
    else:
        print(f"Error creating Sales Order: {response.text}")
        return None

def process_sales_order(sales_order_data):
    """Checks if a Sales Order exists for the given reference number and creates one if not."""
    reference_number = sales_order_data.get("reference_number")
    if not reference_number:
        print("Reference number is missing!")
        return
    existing_orders = get_sales_orders()
    for order in existing_orders:
        if order.get("reference_number") == reference_number:
            print(f"Sales Order with reference number {reference_number} already exists.")
            return
    print(f"Creating a new Sales Order with reference number {reference_number}...")
    create_sales_order(sales_order_data)


pdf_path = "po.pdf"
po_data = extract_purchase_order_fields(pdf_path)
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
