import pandas as pd
import xlrd
from io import BytesIO

def process_excel(file):
    # Read the Excel file
    workbook = xlrd.open_workbook(file, ignore_workbook_corruption=True)
    # df = pd.read_excel(workbook,header=6) 
    df = pd.read_excel(BytesIO(file), engine='openpyxl', header=6)

    # df.columns = ['Row Number', 'Image', 'Inventory Number', 'UPI', 'Color', 'Size', 'Price',
    #    'Quantity', 'Remark']

    # df.to_csv('order_prod.csv')   

    # Check if necessary columns exist and filter them
    required_columns = ["Row Number", "UPI", "Quantity", "Price"]
    if set(required_columns).issubset(df.columns):
        return df[required_columns]
    else:
        raise ValueError("Required columns are missing.")
    
def process_csv(file_content):
    # Read the CSV file into a DataFrame
    print(file_content)
    df = pd.read_csv(file_content)
    
    # # Rename columns if necessary (example based on your image)
    # df.columns = ['Row Number', 'Image', 'Inventory Number', 'UPI', 'Color', 'Size', 'Price',
    #               'Quantity', 'Remark']
    
    # Filter necessary columns
    df = df[df['UPI'].notnull()]
    required_columns = ["Row Number", "UPI", "Quantity", "Price"]
    if set(required_columns).issubset(df.columns):

        return df[required_columns]
    else:
        raise ValueError("Required columns are missing.")


# file = '/Users/shivamsoni/Documents/minaki-poc/orders_prod_20240928571537 (1).xls'
# df=process_excel(file)
# print(df.head())