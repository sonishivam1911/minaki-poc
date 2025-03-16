import streamlit as st
from config.logger import logger
import pandas as pd
from utils.zakya_api import (fetch_object_for_each_id
    ,fetch_records_from_zakya)

from utils.postgres_connector import crud

def extract_record_list(input_data,key):
    records = []
    for record in input_data:
        records.extend(record[f'{key}'])
    return records

def fetch_records_from_zakya_in_df_format(endpoint):
    object_data = fetch_records_from_zakya(
        st.session_state['api_domain'],
        st.session_state['access_token'],
        st.session_state['organization_id'],
        f'/{endpoint}'                  
)
    object_data = extract_record_list(object_data,f"{endpoint}")
    object_data = pd.DataFrame.from_records(object_data)
    return object_data


def create_invoice_mapping():
    
    invoice_df = fetch_records_from_zakya_in_df_format("invoices")
    mapping_data = []
    for index,row in invoice_df.iterrows():
        invoice_id = row.get('invoice_id')
        logger.debug(f"Invoice Id is {row.get('invoice_id')}")

        # if not invoice_id:
        #     # logger.debug(f"row for invoice is : {row}")
        #     continue

        invoice_details = fetch_object_for_each_id(
        st.session_state['api_domain'],
        st.session_state['access_token'],
        st.session_state['organization_id'],
        f'invoices/{invoice_id}'          
    )

    # Extract line items from invoice
        if 'invoice' in invoice_details and 'line_items' in invoice_details['invoice']:
            line_items = invoice_details['invoice']['line_items']
        elif 'line_items' in invoice_details:
            line_items = invoice_details['line_items']
        else:
        # Skip if no line items are found
            logger.debug(f"invoice_details is {invoice_details}")
            continue    

    # Process each line item in the invoice
        for line_item in line_items:
            mapping_data.append({
            'invoice_id': invoice_id,
            'invoice_number': invoice_details.get('invoice', {}).get('invoice_number', ''),
            'line_item_id': line_item.get('line_item_id', ''),
            'item_id': line_item.get('item_id', ''),
            'item_name': line_item.get('name', ''),
            'quantity': line_item.get('quantity', 0),
            'rate': line_item.get('rate', 0),
            'amount': line_item.get('item_total', 0),
        })    

    invoicing_mapping_df = pd.DataFrame.from_records(mapping_data)
    crud.create_table('zakya_invoice_line_item_mapping',invoicing_mapping_df)
    return invoicing_mapping_df



def create_salesorder_mapping():
    salesorder_df = fetch_records_from_zakya_in_df_format("salesorders")
    mapping_data = []
    for index,row in salesorder_df.iterrows():
        salesorder_id = row.get(row['salesorder_id'])

        salesorder_details = fetch_object_for_each_id(
                st.session_state['api_domain'],
                st.session_state['access_token'],
                st.session_state['organization_id'],
                f'salesorders/{salesorder_id}'          
            )

        # Extract line items from invoice
        if 'sales_order' in salesorder_details and 'line_items' in salesorder_details['sales_order']:
            line_items = salesorder_details['sales_order']['line_items']
        elif 'line_items' in salesorder_details:
            line_items = salesorder_details['line_items']
        else:
            # Skip if no line items are found
            logger.debug(f"salesorder_details is {salesorder_details}")
            continue    

        # Process each line item in the sales order
        for line_item in line_items:
            mapping_data.append({
            'salesorder_id': salesorder_id,
            'salesorder_number': salesorder_details.get('sales_order', {}).get('salesorder_number', ''),
            'line_item_id': line_item.get('line_item_id', ''),
            'item_id': line_item.get('item_id', ''),
            'item_name': line_item.get('name', ''),
            'quantity': line_item.get('quantity', 0),
            'rate': line_item.get('rate', 0),
            'amount': line_item.get('item_total', 0),
        })   

    salesorder_mapping_df = pd.DataFrame.from_records(mapping_data)
    crud.create_table('zakya_salesorder_line_item_mapping',salesorder_mapping_df)

