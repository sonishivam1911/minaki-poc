import pandas as pd
import xlrd
import os
from io import BytesIO
import requests
import re
from dotenv import load_dotenv
import io

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
    


def generate_csv_template():
    """
    Generates a CSV template for the user to download.
    """
    # Define the columns and optionally add a sample row
    template_df = pd.DataFrame({
        "Vendor Code(*)": [""],
        "Color(*)": [""], 
        "Size": [""],
        "Description": [""],
        "Quantity(*)": [""],
        "Cost Price(*)": [""],
        "Category(*)" : [""],
        "Lines" : [""],
        "Is Product Present (Y/N)(*)": [""],
        "SKU": [""],
        "Allow Backorder" : [""],
        "Multipler(*)" : [""]
    })
    
    # Write the DataFrame to a BytesIO buffer as CSV
    buffer = io.BytesIO()
    template_df.to_csv(buffer, index=False)
    buffer.seek(0)  # reset the pointer to the beginning of the buffer

    return buffer


def download_zakya_items_group_csv_template(df):

    unit_list = []
    product_name_list = []
    AttributeOption1_list = []
    AttributeOption2_list = []
    df["Brand"] = "MINAKI"
    df["AttributeName1"] = "Color"
    df["AttributeName2"] = "Size"
    df["AttributeName3"] = "Style"
    df["Item Type"] = "Inventory"
    df["Product Type"] = "goods"
    df["HSN/SAC"] = "711790"
    df["Intra State Tax Name"] = "Shopify Tax Group (SGST 1.5 CGST 1.5)"
    df["Intra State Tax Rate"] = "3"
    df["Intra State Tax Type"] = "Group"
    df["Inter State Tax Name"] = "IGST 3"
    df["Inter State Tax Rate"] = "3"
    df["Inter State Tax Type"] = "Simple"
    df["Gender"] = "Women"
    df["Opening Stock"] = 0
    df["Opening Stock Value"] = 0
    
    df[['Color', 'Size']] = df[['Color', 'Size']].fillna('')
    
    for _, row in df.iterrows():

        category = row.get('Category', '').strip()
        
        color = row.get('Color','').strip()
        AttributeOption1_list.append(color)
       

        size = row.get('Size','').strip()
        AttributeOption2_list.append(size)
        
        if 'Earrings' in category or "Earrings" in category:
            unit_list.append("pairs")
        else:
            unit_list.append("pcs")

        product_name_list.append(f"{category} {color}")

    df["Unit"] = unit_list
    df["Product Name"] = product_name_list
    df["AttributeOption1"] = AttributeOption1_list
    df["AttributeOption2"] = AttributeOption2_list
    df.rename(columns={
        'VendorCode' : 'Vendor Code',
        'AllowBackDoor' : 'Allow Backorder'
    },inplace=True)

    df = df[
        ["Product Name","Unit","Brand","AttributeName1","AttributeName2","AttributeName3","Item Type"
        ,"Product Type","Selling Price","Purchase Price","SKU","MRP","AttributeOption1","AttributeOption2"
        ,"Opening Stock","Opening Stock Value","HSN/SAC","Intra State Tax Name","Intra State Tax Rate"
        ,"Intra State Tax Type","Inter State Tax Name","Inter State Tax Rate","Inter State Tax Type"
        ,"Category","Collection","Allow Backorder","Gender","Vendor Code","Cost","Lines"]
    ]

    return df

def create_sku(df):
    # looping for each row we check what whether size if null or color is null of noth are not null then
    # SKU always start with M followed by X and then category type - Bracelet (B), Earrings (E), Ring(R) followed by 
    # going through entire product master and for all MXE/MXB/MXR find the most updated serial
    # so SKU will look like MXB0211 or MXE0234 the serial after alphabets can be of any length - ideally serial number
    # should be auto increment product_master_df = load_and_rename_master()

    # Define a mapping for Category_Name to SKU block suffix
    category_map = {
        'Bracelets': 'B',
        'Earrings': 'E',
        'Rings': 'R'
    }
    
    # Initialize a serial tracker for each SKU block
    serial_trackers = {'B': 0, 'E': 0, 'R': 0, 'X': 0}

    product_master_df = load_and_rename_master()
    
    # Extract existing SKUs and update serial trackers
    existing_skus = product_master_df['SKU'].dropna().astype(str)
    for sku in existing_skus:
        match = re.match(r"MX([BERX])(\d+)", sku)
        if match:
            suffix = match.group(1)
            serial_number = int(match.group(2))
            serial_trackers[suffix] = max(serial_trackers[suffix], serial_number)
    
    vendor_code_dict = {}
    # Generate new SKUs
    new_skus = []
    
    for index, row in df.iterrows():
        
        currentVendorCode = row.get('VendorCode', '').strip()

        category = row.get('Category', '').strip()


        if 'Earrings' in category or "Earrings" in category:
            category = "Earrings"
        suffix = category_map.get(category)  # Default to 'X' if no category match
        variant = []

        if not pd.isnull(row['Color']):
            variant.append(str(row['Color'])[:2])
        
        if not pd.isnull(row['Size']) and row['Size'] != "Free":
            variant.append(str(row['Size']))

        variant_sub_str = None
        if len(variant) == 2:
            variant_sub_str = "/".join(variant)
        else:
            variant_sub_str = variant[0]  

        if pd.isnull(row['SKU']) and currentVendorCode not in vendor_code_dict.keys():
            # Increment serial within the block
            serial_trackers[suffix] += 1
            new_sku = f"MX{suffix}{str(serial_trackers[suffix]).zfill(4)}/{variant_sub_str}"
            
            new_skus.append(new_sku)
            vendor_code_dict[currentVendorCode] = new_sku

        elif pd.isnull(row['SKU']) and currentVendorCode in vendor_code_dict.keys():
            parentSKU = vendor_code_dict[currentVendorCode].split("/")[0]
            new_skus.append(f"{parentSKU}/{variant_sub_str}")

        

    df['SKU'] = new_skus

    updated_df = download_zakya_items_group_csv_template(df)

    return updated_df 

def process_csv(file_content,exchange_rate):
    # Read the CSV file into a DataFrame
    # print(file_content)
    df = pd.read_csv(file_content)
    df.columns = ["VendorCode","Color", "Size", "Description","Quantity","CostPrice","Category","Collection","Lines","IsProductPresent","AllowBackDoor","SKU","Multipler","MRP"]
    
    # Filter necessary columns
    df = df[df['VendorCode'].notnull()]

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
    
    def parse_vendor_code(vc):
        if pd.isnull(vc):
            return vc
        
        return vc.split(" ")[0]

    df['Quantity'] = df['Quantity'].apply(parse_quantity)  
    df['VendorCode'] = df['VendorCode'].apply(parse_vendor_code)


    # Clean and convert Price from string (e.g. "$12.34") to float (12.34)
    def parse_price(p):
        # Remove dollar sign and strip whitespace
        p_str = str(p).replace('$', '').strip()
        # Convert to float
        try:
            return float(p_str)
        except ValueError:
            return None

    df['CostPrice'] = df['CostPrice'].apply(parse_price)   


    # Calculate MRP using CostPrice, Multipler, and exchange_rate
    def calculate_mrp(row):
        if pd.notnull(row['CostPrice']) and pd.notnull(row['Multipler']):
            return round(row['CostPrice'] * row['Multipler'] * exchange_rate, 2)
        return None

    df['MRP'] = df.apply(calculate_mrp, axis=1)

    df['VendorCode'] = df['VendorCode'].str.split(" ").str[0]


    # Add INR prefix to MRP column
    df['MRP'] = df['MRP'].apply(lambda x: f"INR {x:.2f}" if pd.notnull(x) else None)

    # Create additional columns Cost, Purchase Price, and Selling Price without INR prefix
    df['Cost'] = df['MRP'].str.replace('INR ', '', regex=False).astype(float)
    df['Purchase Price'] = df['Cost']
    df['Selling Price'] = df['Cost']    

    # Would like to give user option to use filtering to select for SKU's which can't be mapped
    # ExistingSKUDF = df[df["IsProductPresent"] == 'Y']
    # NewSKUDF = df[df["IsProductPresent"] == 'N'] 

    # updatedNewSKUDF = create_sku(NewSKUDF)

    # print(updatedNewSKUDF)
    # print(updatedNewSKUDF.columns)

    # Convert Price from USD to INR
    # Using an API key for ExchangeRate-API:
    # api_key = os.getenv('EXCHANGE_RATE_API_KEY', None)
    # if api_key is None:
    #     print("No API key found in environment variable. Please set EXCHANGE_RATE_API_KEY.")
    #     # Proceed without conversion if no API key
    #     return df

    # # Construct the request URL using the API key
    # url = f"https://v6.exchangerate-api.com/v6/{api_key}/latest/USD"

    # try:
    #     response = requests.get(url)
    #     response.raise_for_status()  # Raise an error if the request was not successful
    #     data = response.json()
        
    #     if data.get('result') == 'success':
    #         usd_to_inr = data['conversion_rates'].get('INR', None)
    #         if usd_to_inr and 'Price' in df.columns:
    #             df['Price_INR'] = df['Price'] * usd_to_inr
    #     else:
    #         print("API did not return a successful result. Proceeding without conversion.")
    # except Exception as e:
    #     print(f"Error fetching exchange rate: {e}")
        # Proceed without adding the Price_INR column if the rate can't be fetched.     

    return df




def map_existing_products(df):
    """
    Maps SKUs and processes Vendor Codes for existing products using master_output.xlsx.
    
    Args:
        df (pd.DataFrame): The uploaded DataFrame to be processed.
        master_output_path (str): Path to the master_output.xlsx file.
    
    Returns:
        pd.DataFrame: Updated DataFrame with SKUs mapped for existing products.
    """
    # Load the master_output.xlsx
    master_df = pd.read_excel('master_output.xlsx')

    # Exclude rows where SKU is null
    master_df = master_df[master_df['SKU'].notnull()]    

    # Process Vendor Code in the master_output.xlsx by removing anything after the first space
    master_df['Processed_Vendor_Code'] = master_df['Vendor Code'].str.split(" ").str[0]

    # Process Vendor Code in the uploaded DataFrame
    df['Processed_Vendor_Code'] = df['VendorCode'].str.split(" ").str[0]
    df['SKU'] = df['SKU'].astype(str)

    # Merge both DataFrames on the processed Vendor Code to map SKUs
    merged_df = df.merge(
        master_df[['Processed_Vendor_Code', 'SKU']], 
        left_on='Processed_Vendor_Code', 
        right_on='Processed_Vendor_Code', 
        how='inner'
    )

    # Update the SKU in the uploaded DataFrame for existing products
    merged_df['SKU'] = merged_df['SKU_y'].combine_first(merged_df['SKU_x'])

    # Drop unnecessary columns from the merge
    merged_df = merged_df.drop(columns=['SKU_x', 'SKU_y', 'Processed_Vendor_Code'])

    product_master_df = load_and_rename_master()
    # Merge with the product master
    final_df = merged_df.merge(
        product_master_df[['SKU', 'Item_Name', 'HSN_SAC']], 
        on='SKU', 
        how='inner'
    )   

    print(final_df)
    return final_df


def aggregated_df(new_sku_df, existing_sku_df):
    """
    Creates the aggregated DataFrame (Purchase Order DataFrame) without merging with the initial DataFrame.
    Handles `new_sku_df` and `existing_sku_df` independently and then combines them.

    Args:
        new_sku_df (pd.DataFrame): DataFrame with new SKUs.
        existing_sku_df (pd.DataFrame): DataFrame with existing SKUs.

    Returns:
        pd.DataFrame: Aggregated DataFrame with calculated and static fields.
    """
    # Process new SKUs
    new_sku_df = new_sku_df[new_sku_df['SKU'].notnull()]  # Exclude rows with null SKUs
    new_purchase_order_df = new_sku_df.copy()

    # Populate static and calculated fields for new SKUs
    new_purchase_order_df['Account'] = 'Inventory Asset'
    new_purchase_order_df['Item Type'] = 'goods'
    new_purchase_order_df['Attention'] = 'Head Office'
    new_purchase_order_df['HSN'] = '711790'
    new_purchase_order_df['Item Price'] = new_purchase_order_df['Selling Price']
    new_purchase_order_df['Item Name'] = new_purchase_order_df['Product Name']
    new_purchase_order_df['Item Total'] = new_purchase_order_df['Selling Price'] * new_purchase_order_df['Quantity']
    new_purchase_order_df['Usage unit'] = new_purchase_order_df['Category'].apply(
        lambda x: 'pairs' if 'earring' in str(x).lower() or 'kada' in str(x).lower() else 'pcs'
    )
    new_purchase_order_df['Total'] = new_purchase_order_df['Item Total'].sum()

    # Select relevant columns for new SKUs
    new_purchase_order_df = new_purchase_order_df[[
        'SKU', 'Account', 'Item Price', 'Item Name', 'Quantity', 'Usage unit',
        'Item Total', 'Total', 'Attention', 'HSN', 'Item Type'
    ]]

    # Process existing SKUs
    existing_sku_df = existing_sku_df[existing_sku_df['SKU'].notnull()]  # Exclude rows with null SKUs
    existing_purchase_order_df = existing_sku_df.copy()

    # Populate static and calculated fields for existing SKUs
    existing_purchase_order_df['Account'] = 'Inventory Asset'
    existing_purchase_order_df['Item Type'] = 'goods'
    existing_purchase_order_df['Attention'] = 'Head Office'
    existing_purchase_order_df['HSN'] = '711790'
    existing_purchase_order_df['Item Name'] = existing_purchase_order_df['Item_Name']
    existing_purchase_order_df['Item Price'] = existing_purchase_order_df['Selling Price']
    existing_purchase_order_df['Item Total'] = existing_purchase_order_df['Selling Price'] * existing_purchase_order_df['Quantity']
    existing_purchase_order_df['Usage unit'] = existing_purchase_order_df['Category'].apply(
        lambda x: 'pairs' if 'earring' in str(x).lower() or 'kada' in str(x).lower() else 'pcs'
    )
    existing_purchase_order_df['Total'] = existing_purchase_order_df['Item Total'].sum()

    # Select relevant columns for existing SKUs
    existing_purchase_order_df = existing_purchase_order_df[[
        'SKU', 'Account', 'Item Price', 'Item Name', 'Quantity', 'Usage unit',
        'Item Total', 'Total', 'Attention', 'HSN', 'Item Type'
    ]]

    # Combine the two DataFrames
    purchase_order_df = pd.concat([new_purchase_order_df, existing_purchase_order_df], ignore_index=True)

    # Return the final purchase order DataFrame
    return purchase_order_df


# file = '/Users/shivamsoni/Downloads/Vendor Order Template - Template.csv'
# df=process_csv(file,82.15)
# print(df.head())

# file = 'order_prod.csv'
# df=process_csv(file)
# print(df.head())