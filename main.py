import pandas as pd
import xlrd
import os
import logging
from io import BytesIO
import requests
import re
from dotenv import load_dotenv
import io

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("process_logs.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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
    try:
        logger.debug(f"Entering load_and_rename_master with filepath: {filepath}")
        df = pd.read_csv(filepath)
        logger.debug(f"Loaded product master with shape: {df.shape}")
        df.rename(columns=COLUMN_RENAME_MAP, inplace=True)
        logger.debug("Renamed columns in product master.")
        return df
    except Exception as e:
        logger.error(f"Error in load_and_rename_master: {e}")
        raise

def filter_existing_products(df, category_name=None, components=None, work=None, finish=None, finding=None):
    try:
        logger.debug("Entering filter_existing_products")
        logger.debug(f"Initial DataFrame shape: {df.shape}")
        filtered_df = df.copy()
        if category_name:
            filtered_df = filtered_df[filtered_df["Category_Name"].str.lower() == category_name.lower()]
            logger.debug(f"Filtered by category_name: {category_name}. Shape: {filtered_df.shape}")
        if components:
            filtered_df = filtered_df[filtered_df["CF_Components"].str.lower() == components.lower()]
            logger.debug(f"Filtered by components: {components}. Shape: {filtered_df.shape}")
        if work:
            filtered_df = filtered_df[filtered_df["CF_Work"].str.lower() == work.lower()]
            logger.debug(f"Filtered by work: {work}. Shape: {filtered_df.shape}")
        if finish:
            filtered_df = filtered_df[filtered_df["CF_Finish"].str.lower() == finish.lower()]
            logger.debug(f"Filtered by finish: {finish}. Shape: {filtered_df.shape}")
        if finding:
            filtered_df = filtered_df[filtered_df["CF_Finding"].str.lower() == finding.lower()]
            logger.debug(f"Filtered by finding: {finding}. Shape: {filtered_df.shape}")
        return filtered_df
    except Exception as e:
        logger.error(f"Error in filter_existing_products: {e}")
        raise

def process_excel(file):
    try:
        logger.debug(f"Entering process_excel with file: {file}")
        workbook = xlrd.open_workbook(file, ignore_workbook_corruption=True)
        df = pd.read_excel(workbook, header=6)
        logger.debug(f"Excel DataFrame shape: {df.shape}")
        df.columns = ['Row Number', 'Image', 'Inventory Number', 'UPI', 'Color', 'Size', 'Price',
                      'Quantity', 'Total Price']
        df.to_csv('order_prod.csv')
        required_columns = ["Row Number", "UPI", "Quantity", "Price"]
        if set(required_columns).issubset(df.columns):
            logger.debug("Required columns found in Excel file.")
            return df[required_columns]
        else:
            logger.error("Required columns are missing in Excel file.")
            raise ValueError("Required columns are missing.")
    except Exception as e:
        logger.error(f"Error in process_excel: {e}")
        raise
    


def generate_csv_template():
    try:
        logger.debug("Entering generate_csv_template")
        template_df = pd.DataFrame({
            "Vendor Code(*)": [""],
            "Color(*)": [""],
            "Size": [""],
            "Description": [""],
            "Quantity(*)": [""],
            "Cost Price(*)": [""],
            "Category(*)": [""],
            "Lines": [""],
            "Is Product Present (Y/N)(*)": [""],
            "SKU": [""],
            "Allow Backorder": [""],
            "Multipler(*)": [""]
        })
        buffer = io.BytesIO()
        template_df.to_csv(buffer, index=False)
        buffer.seek(0)
        logger.debug("Generated CSV template successfully.")
        return buffer
    except Exception as e:
        logger.error(f"Error in generate_csv_template: {e}")
        raise


def download_zakya_items_group_csv_template(df):
    try:
        logger.debug("Entering download_zakya_items_group_csv_template")
        logger.debug(f"Input DataFrame shape: {df.shape}")
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
            color = row.get('Color', '').strip()
            size = row.get('Size', '').strip()
            unit_list.append("pairs" if 'Earrings' in category else "pcs")
            product_name_list.append(f"{category} {color}")
            AttributeOption1_list.append(color)
            AttributeOption2_list.append(size)

        df["Unit"] = unit_list
        df["Product Name"] = product_name_list
        df["AttributeOption1"] = AttributeOption1_list
        df["AttributeOption2"] = AttributeOption2_list
        logger.debug("Populated additional columns for Zakya items group.")
        df.rename(columns={'VendorCode': 'Vendor Code', 'AllowBackDoor': 'Allow Backorder'}, inplace=True)
        df = df[[
            "Product Name", "Unit", "Brand", "AttributeName1", "AttributeName2", "AttributeName3", "Item Type",
            "Product Type", "Selling Price", "Purchase Price", "SKU", "MRP", "AttributeOption1", "AttributeOption2",
            "Opening Stock", "Opening Stock Value", "HSN/SAC", "Intra State Tax Name", "Intra State Tax Rate",
            "Intra State Tax Type", "Inter State Tax Name", "Inter State Tax Rate", "Inter State Tax Type",
            "Category", "Collection", "Allow Backorder", "Gender", "Vendor Code", "Cost", "Lines"
        ]]
        logger.debug(f"Final DataFrame shape for Zakya items group: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Error in download_zakya_items_group_csv_template: {e}")
        raise

def create_sku(df):
    """
    Creates SKUs for the given DataFrame based on categories, color, and size.

    Args:
        df (pd.DataFrame): Input DataFrame with VendorCode, Category, Color, Size, and existing SKU columns.

    Returns:
        pd.DataFrame: Updated DataFrame with generated SKUs and processed data.
    """
    try:
        logger.debug("Entering create_sku")
        logger.debug(f"Input DataFrame shape: {df.shape}")

        # Define a mapping for Category_Name to SKU block suffix
        category_map = {
            'Bracelets': 'B',
            'Earrings': 'E',
            'Rings': 'R'
        }

        # Initialize a serial tracker for each SKU block
        serial_trackers = {'B': 0, 'E': 0, 'R': 0, 'X': 0}
        logger.debug("Initialized serial trackers for SKU blocks.")

        # Load product master and extract existing SKUs
        product_master_df = load_and_rename_master()
        existing_skus = product_master_df['SKU'].dropna().astype(str)

        logger.debug(f"Loaded product master with shape: {product_master_df.shape}")
        logger.debug(f"Extracted {len(existing_skus)} existing SKUs for processing.")

        # Update serial trackers based on existing SKUs
        for sku in existing_skus:
            match = re.match(r"MX([BERX])(\d+)", sku)
            if match:
                suffix = match.group(1)
                serial_number = int(match.group(2))
                serial_trackers[suffix] = max(serial_trackers[suffix], serial_number)
        logger.debug(f"Updated serial trackers: {serial_trackers}")

        vendor_code_dict = {}
        new_skus = []

        # Iterate over the rows of the input DataFrame
        for index, row in df.iterrows():
            try:
                currentVendorCode = row.get('VendorCode', '').strip()
                category = row.get('Category', '').strip()

                # Determine SKU suffix based on category
                if 'Earrings' in category or "Earrings" in category:
                    category = "Earrings"
                suffix = category_map.get(category, 'X')  # Default to 'X' if no category match

                # Process variants based on Color and Size
                variant = []
                if not pd.isnull(row['Color']):
                    variant.append(str(row['Color'])[:2])
                if not pd.isnull(row['Size']) and row['Size'] != "Free":
                    variant.append(str(row['Size']))

                variant_sub_str = "/".join(variant) if len(variant) > 1 else variant[0] if variant else None

                # Generate SKU based on VendorCode and variants
                if pd.isnull(row['SKU']) and currentVendorCode not in vendor_code_dict.keys():
                    serial_trackers[suffix] += 1
                    new_sku = f"MX{suffix}{str(serial_trackers[suffix]).zfill(4)}"
                    if variant_sub_str:
                        new_sku += f"/{variant_sub_str}"
                    new_skus.append(new_sku)
                    vendor_code_dict[currentVendorCode] = new_sku
                    logger.debug(f"Generated new SKU: {new_sku} for VendorCode: {currentVendorCode}")
                elif pd.isnull(row['SKU']) and currentVendorCode in vendor_code_dict.keys():
                    parentSKU = vendor_code_dict[currentVendorCode].split("/")[0]
                    new_sku = f"{parentSKU}/{variant_sub_str}" if variant_sub_str else parentSKU
                    new_skus.append(new_sku)
                    logger.debug(f"Appended variant to existing SKU: {new_sku} for VendorCode: {currentVendorCode}")

            except Exception as e:
                logger.error(f"Error processing row {index}: {e}")

        df['SKU'] = new_skus
        logger.debug(f"Generated {len(new_skus)} new SKUs.")

        # Call download_zakya_items_group_csv_template for further processing
        updated_df = download_zakya_items_group_csv_template(df)
        logger.debug(f"Updated DataFrame shape after SKU generation: {updated_df.shape}")
        return updated_df

    except Exception as e:
        logger.error(f"Error in create_sku: {e}")
        raise
 

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

    Returns:
        pd.DataFrame: Updated DataFrame with SKUs mapped for existing products.
    """
    try:
        logger.debug("Entering map_existing_products")
        logger.debug(f"Input DataFrame shape: {df.shape}")

        # Load master_output.xlsx
        master_df = pd.read_excel('master_output.xlsx')
        logger.debug(f"Loaded master DataFrame with shape: {master_df.shape}")

        # Exclude rows with null SKUs in master DataFrame
        master_df = master_df[master_df['SKU'].notnull()]
        logger.debug(f"Filtered master DataFrame shape (non-null SKU): {master_df.shape}")

        # Process VendorCode
        master_df['Processed_Vendor_Code'] = master_df['Vendor Code'].str.split(" ").str[0]
        df['Processed_Vendor_Code'] = df['VendorCode'].str.split(" ").str[0]

        # Merge with master DataFrame
        merged_df = df.merge(
            master_df[['Processed_Vendor_Code', 'SKU']],
            on='Processed_Vendor_Code',
            how='inner'
        )
        logger.debug(f"Merged DataFrame shape: {merged_df.shape}")

        # Update SKU in merged DataFrame
        merged_df['SKU'] = merged_df['SKU_y'].combine_first(merged_df['SKU_x'])

        # Drop unnecessary columns
        merged_df = merged_df.drop(columns=['SKU_x', 'SKU_y', 'Processed_Vendor_Code'])

        # Load product master for additional information
        product_master_df = load_and_rename_master()
        logger.debug(f"Loaded product master DataFrame with shape: {product_master_df.shape}")

        # Merge with product master
        final_df = merged_df.merge(
            product_master_df[['SKU', 'Item_Name', 'HSN_SAC']],
            on='SKU',
            how='inner'
        )
        logger.debug(f"Final DataFrame shape after mapping existing products: {final_df.shape}")

        return final_df
    except Exception as e:
        logger.error(f"Error in map_existing_products: {e}")
        raise


def aggregated_df(new_sku_df, existing_sku_df):
    """
    Creates the aggregated DataFrame (Purchase Order DataFrame) by combining new and existing SKUs.

    Args:
        new_sku_df (pd.DataFrame): DataFrame with new SKUs.
        existing_sku_df (pd.DataFrame): DataFrame with existing SKUs.

    Returns:
        pd.DataFrame: Aggregated DataFrame with calculated and static fields.
    """
    try:
        logger.debug("Entering aggregated_df")
        logger.debug(f"New SKU DataFrame shape: {new_sku_df.shape}")
        logger.debug(f"Existing SKU DataFrame shape: {existing_sku_df.shape}")

        # Filter out rows with null SKUs
        new_sku_df = new_sku_df[new_sku_df['SKU'].notnull()]
        existing_sku_df = existing_sku_df[existing_sku_df['SKU'].notnull()]
        logger.debug(f"Filtered New SKU DataFrame shape: {new_sku_df.shape}")
        logger.debug(f"Filtered Existing SKU DataFrame shape: {existing_sku_df.shape}")

        # Create new purchase order DataFrame
        new_sku_df['Account'] = 'Inventory Asset'
        new_sku_df['Item Type'] = 'goods'
        new_sku_df['Attention'] = 'Head Office'
        new_sku_df['HSN'] = '711790'
        new_sku_df['Item Price'] = new_sku_df['Selling Price']
        new_sku_df['Item Total'] = new_sku_df['Selling Price'] * new_sku_df['Quantity']
        new_sku_df['Usage unit'] = new_sku_df['Category'].apply(
            lambda x: 'pairs' if 'earring' in str(x).lower() or 'kada' in str(x).lower() else 'pcs'
        )
        new_sku_df['Total'] = new_sku_df['Item Total'].sum()
        logger.debug("Processed New SKU DataFrame with static and calculated fields.")

        # Create existing purchase order DataFrame
        existing_sku_df['Account'] = 'Inventory Asset'
        existing_sku_df['Item Type'] = 'goods'
        existing_sku_df['Attention'] = 'Head Office'
        existing_sku_df['HSN'] = '711790'
        existing_sku_df['Item Total'] = existing_sku_df['Selling Price'] * existing_sku_df['Quantity']
        existing_sku_df['Usage unit'] = existing_sku_df['Category'].apply(
            lambda x: 'pairs' if 'earring' in str(x).lower() or 'kada' in str(x).lower() else 'pcs'
        )
        existing_sku_df['Total'] = existing_sku_df['Item Total'].sum()
        logger.debug("Processed Existing SKU DataFrame with static and calculated fields.")

        # Combine the two DataFrames
        purchase_order_df = pd.concat([new_sku_df, existing_sku_df], ignore_index=True)
        logger.debug(f"Final Aggregated DataFrame shape: {purchase_order_df.shape}")

        return purchase_order_df
    except Exception as e:
        logger.error(f"Error in aggregated_df: {e}")
        raise


# file = '/Users/shivamsoni/Downloads/Vendor Order Template - Template.csv'
# df=process_csv(file,82.15)
# print(df.head())

# file = 'order_prod.csv'
# df=process_csv(file)
# print(df.head())