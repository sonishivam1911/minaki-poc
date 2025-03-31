
import pdfplumber
import requests
import tempfile
import os 
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
        '/salesorders',
        salesorder_payload
    )    


def download_pdf_from_link(url):
    """
    Downloads a PDF from a given URL and saves it to a temporary file.
    
    Args:
        url (str): The URL of the PDF to download.
        
    Returns:
        str: Path to the temporary file containing the downloaded PDF.
        
    Raises:
        Exception: If download fails or URL is invalid.
    """
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()  # Raise exception for HTTP errors
        
        # Create a temporary file to store the PDF
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_file:
            temp_file.write(response.content)
            return temp_file.name
    except Exception as e:
        raise Exception(f"Failed to download PDF from {url}: {str(e)}")


def get_customer_name_from_vendor(vendor):
    """
    Maps vendor code to customer name for Zakya.
    
    Args:
        vendor (str): The vendor name ('PPUS' or 'AZA').
        
    Returns:
        str: The corresponding customer name in Zakya.
    """
    if vendor == "PPUS":
        return "Pernia Delhi"
    elif vendor == "AZA":
        return "Aza Delhi"
    else:
        raise ValueError(f"Unknown vendor: {vendor}")


def process_single_po_link(po_link, vendor, zakya_config):
    """
    Processes a single PO link from the CSV.
    
    Args:
        po_link (str): URL to the PDF purchase order.
        vendor (str): The vendor name ('PPUS' or 'AZA').
        zakya_config (dict): Configuration for Zakya API.
        
    Returns:
        dict: Result of processing this specific PO.
    """
    temp_path = None
    try:
        # Download the PDF
        temp_path = download_pdf_from_link(po_link)
        
        # Extract data using existing functions based on vendor
        if vendor == "PPUS":
            result = pdf_extract__po_details_ppus(temp_path)
        else:  # AZA
            result = pdf_extract__po_details_aza(temp_path)
        
        # Get customer name
        customer_name = get_customer_name_from_vendor(vendor)
        
        # Process the sales order using existing function
        process_sales_order(result, customer_name, zakya_config)
        
        return {
            "status": "success",
            "po_number": result.get("PO No"),
            "link": po_link
        }
        
    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
            "link": po_link
        }
    finally:
        # Clean up the temporary file
        if temp_path and os.path.exists(temp_path):
            try:
                os.unlink(temp_path)
            except:
                pass


def process_csv_file(csv_file, vendor, zakya_config):
    """
    Processes a CSV file containing PO links for a specific vendor.
    
    Args:
        csv_file: The uploaded CSV file object.
        vendor (str): The vendor name ('PPUS' or 'AZA').
        zakya_config (dict): Configuration for Zakya API.
        
    Returns:
        dict: Processing results with statistics and details.
    """
    try:
        # Read the CSV file
        df = pd.read_csv(csv_file)
        
        # Check if the required column exists
        if 'PO link' not in df.columns:
            return {"error": "CSV file must contain a 'PO link' column"}
        
        results = {
            "total": len(df),
            "processed": 0,
            "failed": 0,
            "details": []
        }
        
        # Process each PO link
        for index, row in df.iterrows():
            po_link = row['PO link']
            
            # Skip empty links
            if not po_link or pd.isna(po_link):
                continue
                
            # Process the single PO
            result = process_single_po_link(po_link, vendor, zakya_config)
            
            # Update statistics
            if result["status"] == "success":
                results["processed"] += 1
            else:
                results["failed"] += 1
                
            # Add to details
            results["details"].append(result)
            
        return results
        
    except Exception as e:
        return {"error": f"Failed to process CSV: {str(e)}"}