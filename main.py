import pandas as pd
import xlrd
import os
from io import BytesIO
import requests
import re
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


# Define a column rename map for the product master CSV
COLUMN_RENAME_MAP = {
    "Item ID": "Item_ID",
    "Item Name": "Item_Name",
    "Sales Description": "Sales_Description",
    "Selling Price": "Selling_Price",
    "Sales Account": "Sales_Account",
    "Is Returnable Item": "Is_Returnable_Item",
    "Brand": "Brand",
    "Manufacturer": "Manufacturer",
    "Package Weight": "Package_Weight",
    "Package Length": "Package_Length",
    "Package Width": "Package_Width",
    "Package Height": "Package_Height",
    "Is Receivable Service": "Is_Receivable_Service",
    "Taxable": "Taxable",
    "Exemption Reason": "Exemption_Reason",
    "Product Type": "Product_Type",
    "Source": "Source",
    "Reference ID": "Reference_ID",
    "Last Sync Time": "Last_Sync_Time",
    "Status": "Status",
    "Unit": "Unit",
    "SKU": "SKU",
    "HSN/SAC": "HSN_SAC",
    "UPC": "UPC",
    "EAN": "EAN",
    "ISBN": "ISBN",
    "Part Number": "Part_Number",
    "Purchase Price": "Purchase_Price",
    "Purchase Account": "Purchase_Account",
    "Purchase Description": "Purchase_Description",
    "MRP": "MRP",
    "Inventory Account": "Inventory_Account",
    "Track Batches": "Track_Batches",
    "Reorder Level": "Reorder_Level",
    "Preferred Vendor": "Preferred_Vendor",
    "Warehouse Name": "Warehouse_Name",
    "Opening Stock": "Opening_Stock",
    "Opening Stock Value": "Opening_Stock_Value",
    "Stock On Hand": "Stock_On_Hand",
    "Is Combo Product": "Is_Combo_Product",
    "Item Type": "Item_Type",
    "Category Name": "Category_Name",
    "Batch Reference#": "Batch_Reference_No",
    "Manufacturer Batch#": "Manufacturer_Batch_No",
    "Manufactured Date": "Manufactured_Date",
    "Expiry Date": "Expiry_Date",
    "Quantity In": "Quantity_In",
    "Taxability Type": "Taxability_Type",
    "Intra State Tax Name": "Intra_State_Tax_Name",
    "Intra State Tax Rate": "Intra_State_Tax_Rate",
    "Intra State Tax Type": "Intra_State_Tax_Type",
    "Inter State Tax Name": "Inter_State_Tax_Name",
    "Inter State Tax Rate": "Inter_State_Tax_Rate",
    "Inter State Tax Type": "Inter_State_Tax_Type",
    "CF.Collection": "CF_Collection",
    "CF.Serial Number": "CF_Serial_Number",
    "CF.Gender": "CF_Gender",
    "CF.Product Description": "CF_Product_Description",
    "CF.Components": "CF_Components",
    "CF.Work": "CF_Work",
    "CF.Finish": "CF_Finish",
    "CF.Finding": "CF_Finding",
    "CF.eCommerce Channel": "CF_eCommerce_Channel"
}

def load_and_rename_master(filepath="product_master.csv"):
    """Load the Product Master CSV from a known file and rename columns."""
    df = pd.read_csv(filepath)
    df.rename(columns=COLUMN_RENAME_MAP, inplace=True)
    return df

def filter_existing_products(df, category_name=None, components=None, work=None, finish=None, finding=None):
    """Filter the DataFrame based on user-selected criteria."""
    filtered_df = df.copy()
    if category_name:
        filtered_df = filtered_df[filtered_df["Category_Name"].str.lower() == category_name.lower()]
    if components:
        filtered_df = filtered_df[filtered_df["CF_Components"].str.lower() == components.lower()]
    if work:
        filtered_df = filtered_df[filtered_df["CF_Work"].str.lower() == work.lower()]
    if finish:
        filtered_df = filtered_df[filtered_df["CF_Finish"].str.lower() == finish.lower()]
    if finding:
        filtered_df = filtered_df[filtered_df["CF_Finding"].str.lower() == finding.lower()]
    return filtered_df

def process_excel(file):
    # Read the Excel file

    # file_data = file.read()
    # file_io = BytesIO(file_data)    
    workbook = xlrd.open_workbook(file, ignore_workbook_corruption=True)
    df = pd.read_excel(workbook,header=6) 
    # df = pd.read_excel(BytesIO(file), engine='openpyxl', header=6)
    # df = pd.read_excel(file_io, engine='xlrd')

    df.columns = ['Row Number', 'Image', 'Inventory Number', 'UPI', 'Color', 'Size', 'Price',
        'Quantity', 'Total Price']

    df.to_csv('order_prod.csv')   

    # Check if necessary columns exist and filter them
    required_columns = ["Row Number", "UPI", "Quantity", "Price"]
    if set(required_columns).issubset(df.columns):
        return df[required_columns]
    else:
        raise ValueError("Required columns are missing.")
    

def process_csv(file_content):
    # Read the CSV file into a DataFrame
    # print(file_content)
    df = pd.read_csv(file_content)
    
    # Filter necessary columns
    df = df[df['UPI'].notnull()]

    # Now go over the quantity and check where it has values like 5 ( out of stock : 4) - so actually quantity is 4
    # for price show in dollars and rupees find a way to find real time dollar to INR conversion and show that as well

    # Parse the Quantity column
    # Assuming the Quantity column can have values like "5 (out of stock:4)"
    # and we want to extract the actual quantity (4 in this example).
    # We'll use a regex to find the last number in the string if present.
    def parse_quantity(q):
        if pd.isnull(q):
            return q
        # Convert to string if not already
        q_str = str(q)
        
        # Try to find a number after 'out of stock:' pattern, else fallback to original number if present
        match = re.search(r'out of stock[:\s]*(\d+)', q_str, flags=re.IGNORECASE)
        if match:
            return int(match.group(1))
        
        # If no match for the "out of stock" pattern, try to just get the first integer in the string
        match = re.search(r'(\d+)', q_str)
        if match:
            return int(match.group(1))
        
        # If no integers found, return None or 0
        return None

    if 'Quantity' in df.columns:
        df['Quantity'] = df['Quantity'].apply(parse_quantity)  


    # Clean and convert Price from string (e.g. "$12.34") to float (12.34)
    if 'Price' in df.columns:
        
        def parse_price(p):
            # Remove dollar sign and strip whitespace
            p_str = str(p).replace('$', '').strip()
            # Convert to float
            try:
                return float(p_str)
            except ValueError:
                return None

        df['Price'] = df['Price'].apply(parse_price)         

    # Convert Price from USD to INR
    # Using an API key for ExchangeRate-API:
    api_key = os.getenv('EXCHANGE_RATE_API_KEY', None)
    if api_key is None:
        print("No API key found in environment variable. Please set EXCHANGE_RATE_API_KEY.")
        # Proceed without conversion if no API key
        return df

    # Construct the request URL using the API key
    url = f"https://v6.exchangerate-api.com/v6/{api_key}/latest/USD"

    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error if the request was not successful
        data = response.json()
        
        if data.get('result') == 'success':
            usd_to_inr = data['conversion_rates'].get('INR', None)
            if usd_to_inr and 'Price' in df.columns:
                df['Price_INR'] = df['Price'] * usd_to_inr
        else:
            print("API did not return a successful result. Proceeding without conversion.")
    except Exception as e:
        print(f"Error fetching exchange rate: {e}")
        # Proceed without adding the Price_INR column if the rate can't be fetched.     

    return df

# file = '/Users/shivamsoni/Documents/minaki-poc/orders_prod_20240928571537 (1).xls'
# df=process_excel(file)
# print(df.head())

# file = 'order_prod.csv'
# df=process_csv(file)
# print(df.head())