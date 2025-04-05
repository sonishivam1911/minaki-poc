import pdfplumber
import requests
import tempfile
import os 
import streamlit as st
from datetime import datetime
# import fitz  # PyMuPDF
import re
import pandas as pd
from utils.zakya_api import post_record_to_zakya, fetch_records_from_zakya, extract_record_list
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
        "OPC": None,
        "Pricebook_id": None,
        "Vendor": None,
        "Price List": None
    }

def pdf_extract__po_details_ppus(lines):
    fields = {}
    i = 0
    fields["Pricebook_id"] = "1923531000000090263"
    fields["Vendor"] = "1923531000000176206"
    fields["OPC"] = 0
    fields["Price List"] = 0.5
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
        elif "GST No :" in line:
            gstin = line.split()[1].split()[0]
            if gstin.startswith("07"):
                fields["Vendor"] = 1923531000000176206
        i += 1  # Move to the next line
    return fields

def pdf_extract__po_details_aza(lines):
    fields = {}
    i = 0
    fields["Price List"] = 0.55
    if "AZA FASHION PVT LTD DTDC E-fulfilment" in lines[7]:
        fields["Vendor"] = 1923531000000809250
    else:
        fields["Vendor"] = 1923531000000176011
    while i < len(lines):
        line = lines[i].strip()
        fields["Pricebook_id"] = "1923531000000287311"

        # Extract PO Details
        if "PO Number:" in line:
            fields["PO No"] = line.split("PO Number:")[-1].strip()
        elif "GST NO: " in line:
            gstin = line.split()[1].split()[0]
        elif "PO Date:" in line:
            fields["PO Date"] = line.split("PO Date:")[-1].strip()
        elif "Delivery Date:" in line:
            fields["PO Delivery Date"] = line.split("Delivery Date:")[-1].strip()
        elif line == "GST":
            detail_line = lines[i+2].split()
            designer_code = detail_line[0]
            product_id = detail_line[1]
            quantity = detail_line[2]
            cost = detail_line[3]
            customization_charges = detail_line[4]
            total_cost = detail_line[5]
            product_title = lines[i+1].rsplit(" ", 1)[0] + " " + lines[i+3].rsplit(" ", 1)[0]
            size = lines[i+1].rsplit(" ", 1)[1] + " " + lines[i+3].rsplit(" ", 1)[1]
            size = size.strip()
            fields_temp = {
                                "SKU": designer_code,
                                "Partner SKU": product_id,
                                "Description": product_title,
                                "Size": size,
                                "Order Source": 'Aza Online',
                                "Quantity": quantity,
                                "Unit Price": cost,
                                "Other Costs": customization_charges,
                                "Total": total_cost,
                                "Product Link": None
            }

            fields = {**fields, **fields_temp}

        # Extract Order Processing Charges (OPC)
        elif "Order Processing Charges" in line:
            opc_value = re.findall(r"\(\d+\)", line)
            fields["OPC"] = opc_value[0].replace("(", "").replace(")", "") if opc_value else None
        
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


def process_sales_order(fields, zakya_config):
    """Checks if a Sales Order exists for the given reference number and creates one if not."""
    sales_order_data = fetch_records_from_zakya(
                    st.session_state['api_domain'],
                    st.session_state['access_token'],
                    st.session_state['organization_id'],
                    '/salesorders'                  
    )
    mapping_order = extract_record_list(sales_order_data,"salesorders")
    item_data = fetch_records_from_zakya(
                    st.session_state['api_domain'],
                    st.session_state['access_token'],
                    st.session_state['organization_id'],
                    '/items'                  
            )
    mapping_product = extract_record_list(item_data,"items")
    mapping_product = pd.DataFrame(mapping_product)
    item_id = None
    filtered_products = mapping_product[mapping_product["sku"] == fields["SKU"]]
    if not filtered_products.empty:
        item_id = filtered_products["item_id"].iloc[0]
        mrp_rate = filtered_products["rate"].iloc[0]
        item_name = mrp_rate = filtered_products["name"].iloc[0]
    else:
        # Handle the case where no matching SKU was found
        print(f"No product found with SKU: {fields['SKU']}")       

    reference_number = fields.get("PO No")
    os = fields.get("Order Source")
    if not reference_number:
        print("Reference number is missing!")
        return
    existing_orders = pd.DataFrame(mapping_order)
    for _,order in existing_orders.iterrows():
        logger.debug(f"order is {order}")
        po_refnum = order.get("reference_number")
        po_refnum = re.sub(r"PO:\s*", "", po_refnum) 
        if po_refnum == reference_number:
            error = print(f"Sales Order with reference number {reference_number} already exists.")
    terms_and_conditions = """
        All orders are final. Returns or exchanges are not accepted unless the item is damaged or defective upon receipt.
        Custom and made-to-order items cannot be cancelled or refunded once confirmed.
        Standard dispatch within 7–14 business days. Any delays will be duly communicated.
        Minor variations in color or finish are inherent to the handcrafted nature of our products and do not constitute defects.
        """
    
    mulx = fields["Price List"]
    desc = fields["description"]
    sku = fields["sku"]
    salesorder_payload = {
        "customer_id": fields["Vendor"],
        "date": format_date_for_api(fields["PO Date"]),
        "shipment_date": format_date_for_api(fields["PO Delivery Date"]),
        "reference_number": "PO: " + reference_number,
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
                    },
                    {
                        "index": 3,
                        "label": "OPC",
                        "api_name": "cf_opc",
                        "placeholder": "cf_opc",
                        "value": fields["OPC"]
                    }
                ],
        "line_items": [
            {
                "name": item_name if item_name else None,
                "item_id": int(item_id) if item_id else None,
                "description": f"PO: {reference_number}" if item_id else f"SKU: {sku} PO: {reference_number} {desc}",
                "quantity": int(fields["Quantity"]),
                "warehouse_id" : "1923531000001452123"
            }
        ],
        "is_inclusive_tax": True,
        "is_discount_before_tax": True,
        "discount_type": "entity_level",
        "discount": (round(((mrp_rate * mulx) - float(fields["Total"])) / 1.03, 2)
            if mrp_rate and fields.get("Total") else 0),
        "pricebook_id": fields["Pricebook_id"],
        "notes": f"Order Source : {os}",
        "terms": terms_and_conditions
    }
    
    print(f"Creating a new Sales Order with reference number {reference_number}...")
    result = post_record_to_zakya(
        zakya_config['base_url'],
        zakya_config['access_token'],  
        zakya_config['organization_id'],
        'salesorders',
        salesorder_payload
    )    
    return result


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