import streamlit as st
import pandas as pd
from utils.zakya_api import (get_authorization_url
    ,fetch_object_for_each_id
    ,fetch_records_from_zakya)

from utils.postgres_connector import crud


def handle_on_click_button(df,table_name):
    crud.create_table(table_name, df)
    return 

def fetch_zakya_code():
    auth_url = get_authorization_url()
    st.markdown(f"[Login with Zakya]({auth_url})")


def zakya_integration_function():
    st.title("MINAKI Intell")
    # Check if authorization code is present in the URL
    if st.button("Show Contacts"):
        with st.container():
            st.header("Contacts")
            contact_data = fetch_records_from_zakya(
                    st.session_state['api_domain'],
                    st.session_state['access_token'],
                    st.session_state['organization_id'],
                    '/contacts'
                )
            contacts_record = extract_record_list(contact_data,"contacts")
            show_preview = st.checkbox("Show/Hide Contacts",value=True)
            if show_preview:
                contacts_df = pd.DataFrame.from_records(contacts_record)
                st.dataframe(contacts_df)
                if st.button("Save to Database",on_click=handle_on_click_button, args=(contacts_df,'zakya_contacts')):
                    st.success("Contacts saved to database successfully!")


    if st.button("Show Item Groups"):
        with st.container():
            st.header("Item Groups")
            item_groups_data = fetch_records_from_zakya(
                    st.session_state['api_domain'],
                    st.session_state['access_token'],
                    st.session_state['organization_id'],
                    '/itemgroups'
            )
            item_groups_record = extract_record_list(item_groups_data,"itemgroups")
            show_preview = st.checkbox("Show/Hide Item Groups",value=True)
            if show_preview:                
                itemgroups_df = pd.DataFrame.from_records(item_groups_record)
                st.dataframe(itemgroups_df)
                if st.button("Save to Database",on_click=handle_on_click_button, args=(itemgroups_df,'zakya_item_groups')):
                    st.success("Item groups saved to database successfully!")

    if st.button("Show Items"):
        with st.container():
            st.header("Items")
            items_data = fetch_records_from_zakya(
                    st.session_state['api_domain'],
                    st.session_state['access_token'],
                    st.session_state['organization_id'],
                    '/items'                
            )
            items_record = extract_record_list(items_data,"items")
            show_preview = st.checkbox("Show/Hide Products",value=True)
            if show_preview:                 
                product_df = pd.DataFrame.from_records(items_record)                    
                st.dataframe(product_df)
                if st.button("Save to Database",on_click=handle_on_click_button, args=(product_df,'zakya_products')):
                    st.success("Items saved to database successfully!")

    if st.button("Show Sales Order"):
        with st.container():
            st.header("Sales Order")
            sales_order_data = fetch_records_from_zakya(
                    st.session_state['api_domain'],
                    st.session_state['access_token'],
                    st.session_state['organization_id'],
                    '/salesorders'                  
            )
            sales_order_record = extract_record_list(sales_order_data,"salesorders")
            show_preview = st.checkbox("Show/Hide Sales Order",value=True)
            if show_preview:                 
                sales_order_df = pd.DataFrame.from_records(sales_order_record)
                st.dataframe(sales_order_df)
                if st.button("Save to Database",on_click=handle_on_click_button, args=(sales_order_df,'zakya_sales_order')):
                    crud.create_table('zakya_sales_order',sales_order_df)    
                    st.success("zakya_sales_order saved to database successfully!") 


    if st.button("Show Transfer Order"):
        with st.container():
            st.header("Transfer Order")
            transfer_order_data = fetch_records_from_zakya(
                    st.session_state['api_domain'],
                    st.session_state['access_token'],
                    st.session_state['organization_id'],
                    '/transferorders'                  
            )
            print(f"Output is : {transfer_order_data}")
            transfer_order_records = extract_record_list(transfer_order_data,"transfer_orders")
            show_preview = st.checkbox("Show/Hide Transfer Order",value=True)
            if show_preview:                 
                transfer_order_data = pd.DataFrame.from_records(transfer_order_records)
                st.dataframe(transfer_order_data)
                if st.button("Save to Database",on_click=handle_on_click_button, args=(transfer_order_data,'zakya_transfer_orders')):
                    crud.create_table('zakya_transfer_orders',transfer_order_data)    
                    st.success("zakya_transfer_orders saved to database successfully!") 

    if st.button("Show Invoices"):
        with st.container():
            st.header("Invoices")
            invoices_data = fetch_records_from_zakya(
                    st.session_state['api_domain'],
                    st.session_state['access_token'],
                    st.session_state['organization_id'],
                    '/invoices'                  
            )
            invoices_record = extract_record_list(invoices_data,"invoices")
            show_preview = st.checkbox("Show/Hide Invoices",value=True)
            if show_preview:                 
                invoices_data = pd.DataFrame.from_records(invoices_record)
                st.dataframe(invoices_data)                   
                if st.button("Save to Database",on_click=handle_on_click_button, args=(invoices_data,'zakya_invoices')): 
                    st.success("zakya_invoices saved to database successfully!") 


    if st.button("Show Price Books"):
        with st.container():
            st.header("Price Books")
            price_books_data = fetch_records_from_zakya(
                    st.session_state['api_domain'],
                    st.session_state['access_token'],
                    st.session_state['organization_id'],
                    '/pricebooks'                  
            )
            pricebook_record = extract_record_list(price_books_data,"pricebooks")
            show_preview = st.checkbox("Show/Hide Price Books",value=True)
            if show_preview:                 
                price_books_data = pd.DataFrame.from_records(pricebook_record)
                st.dataframe(price_books_data)
                if st.button("Save to Database",on_click=handle_on_click_button, args=(price_books_data,'zakya_pricebooks')):
                    st.success("zakya_pricebooks saved to database successfully!") 

    if st.button("Show Tax Codes"):
        with st.container():
            st.header("Tax Codes")
            tax_data = fetch_records_from_zakya(
                    st.session_state['api_domain'],
                    st.session_state['access_token'],
                    st.session_state['organization_id'],
                    '/settings/taxes'                  
            )
            tax_record = extract_record_list(tax_data,"taxes")
            show_preview = st.checkbox("Show/Hide Price Books",value=True)
            if show_preview:                 
                tax_data = pd.DataFrame.from_records(tax_record)
                st.dataframe(tax_data)
                if st.button("Save to Database",on_click=handle_on_click_button,args=(tax_data,'zakya_taxes')):
                    st.success("zakya_taxes saved to database successfully!") 



def display_each_selected_row(invoice_id):
    details = fetch_object_for_each_id(
                                api_domain=st.session_state['api_domain'],
                                access_token=st.session_state['access_token'],
                                organization_id=st.session_state['organization_id'],
                                endpoint=f"/invoices/{invoice_id}"
                            )
                            

                            # 5. Display the details in an expander (simulating a popup)
    with st.expander(f"Invoice {invoice_id} Details",expanded=True):
                                # You can display raw JSON or form a DataFrame if needed
        st.json(details)

def extract_record_list(input_data,key):
    records = []
    for record in input_data:
        records.extend(record[f'{key}'])
    return records


zakya_integration_function()