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


def check_invoices_from_csv():
    """Function to handle CSV upload and check invoice existence"""
    st.header("Map Invoices from CSV")
    
    # File uploader
    uploaded_file = st.file_uploader("Upload CSV file with invoice numbers", type=['csv'])
    
    if uploaded_file is not None:
        try:
            # Read the CSV file
            df = pd.read_csv(uploaded_file)
            
            # Display the uploaded data
            st.subheader("Uploaded Invoice Numbers")
            st.dataframe(df)
            
            # Let user select the column containing invoice numbers
            invoice_column = st.selectbox(
                "Select the column containing invoice numbers:",
                df.columns.tolist()
            )
            
            if st.button("Check Invoice Existence"):
                # Extract invoice numbers from selected column
                invoice_numbers = df[invoice_column].dropna().tolist()
                
                if not invoice_numbers:
                    st.error("No invoice numbers found in the selected column.")
                    return
                
                # Fetch all invoices from Zakya
                with st.spinner("Fetching invoices from Zakya..."):
                    invoices_data = fetch_records_from_zakya(
                        st.session_state['api_domain'],
                        st.session_state['access_token'],
                        st.session_state['organization_id'],
                        '/invoices'
                    )
                    
                    # Extract invoice records
                    invoices_record = extract_record_list(invoices_data, "invoices")
                    zakya_invoices_df = pd.DataFrame.from_records(invoices_record)
                
                # Check which invoices exist
                results = []
                
                for invoice_num in invoice_numbers:
                    # Convert to string for comparison
                    invoice_num_str = str(invoice_num)
                    
                    # Check if invoice exists in Zakya data
                    invoice_exists = False
                    matched_invoice = None
                    
                    # Check multiple possible column names for invoice numbers
                    possible_columns = ['invoice_number', 'number', 'invoice_id', 'id', 'document_number']
                    
                    for col in possible_columns:
                        if col in zakya_invoices_df.columns:
                            matches = zakya_invoices_df[zakya_invoices_df[col].astype(str) == invoice_num_str]
                            if not matches.empty:
                                invoice_exists = True
                                matched_invoice = matches.iloc[0].to_dict()
                                break
                    
                    results.append({
                        'invoice_number': invoice_num,
                        'exists_in_zakya': invoice_exists,
                        'matched_data': matched_invoice
                    })
                
                # Display results
                st.subheader("Invoice Check Results")
                
                # Create summary
                total_invoices = len(invoice_numbers)
                found_invoices = sum(1 for r in results if r['exists_in_zakya'])
                not_found_invoices = total_invoices - found_invoices
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Invoices", total_invoices)
                with col2:
                    st.metric("Found in Zakya", found_invoices)
                with col3:
                    st.metric("Not Found", not_found_invoices)
                
                # Create detailed results DataFrame with invoice information
                detailed_results = []
                for r in results:
                    row = {
                        'Invoice Number': r['invoice_number'],
                        'Exists in Zakya': r['exists_in_zakya'],
                        'Status': 'Found' if r['exists_in_zakya'] else 'Not Found'
                    }
                    
                    # Add invoice details if found
                    if r['exists_in_zakya'] and r['matched_data']:
                        matched = r['matched_data']
                        row.update({
                            'Customer Name': matched.get('customer_name', 'N/A'),
                            'Customer ID': matched.get('customer_id', 'N/A'),
                            'Invoice Date': matched.get('date', 'N/A'),
                            'Due Date': matched.get('due_date', 'N/A'),
                            'Total Amount': matched.get('total', 'N/A'),
                            'Balance': matched.get('balance', 'N/A'),
                            'Currency': matched.get('currency_code', 'N/A'),
                            'Invoice Status': matched.get('status', 'N/A'),
                            'Reference Number': matched.get('reference_number', 'N/A'),
                            'Payment Terms': matched.get('payment_terms_label', 'N/A'),
                            'Created Time': matched.get('created_time', 'N/A'),
                            'Last Modified': matched.get('last_modified_time', 'N/A')
                        })
                    else:
                        # Add empty fields for not found invoices
                        row.update({
                            'Customer Name': 'N/A',
                            'Customer ID': 'N/A',
                            'Invoice Date': 'N/A',
                            'Due Date': 'N/A',
                            'Total Amount': 'N/A',
                            'Balance': 'N/A',
                            'Currency': 'N/A',
                            'Invoice Status': 'N/A',
                            'Reference Number': 'N/A',
                            'Payment Terms': 'N/A',
                            'Created Time': 'N/A',
                            'Last Modified': 'N/A'
                        })
                    
                    detailed_results.append(row)
                
                results_df = pd.DataFrame(detailed_results)
                
                # Display the results table
                st.dataframe(results_df, use_container_width=True)
                
                # Option to download results
                if st.button("Download Results as CSV"):
                    # Convert to CSV
                    csv = results_df.to_csv(index=False)
                    st.download_button(
                        label="Download CSV",
                        data=csv,
                        file_name='invoice_mapping_results.csv',
                        mime='text/csv'
                    )
                
        except Exception as e:
            st.error(f"Error processing CSV file: {str(e)}")


def zakya_integration_function():
    st.title("MINAKI Intell")
    
    # Create tabs for different functionalities
    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9, tab10, tab11 = st.tabs([
        "Invoice Mapping", 
        "Contacts", 
        "Item Groups", 
        "Items", 
        "Sales Order", 
        "Transfer Order", 
        "Invoices", 
        "Bills", 
        "Price Books", 
        "Tax Codes",
        "Login"
    ])
    
    # Tab 1: Invoice Mapping from CSV
    with tab1:
        check_invoices_from_csv()
    
    # Tab 2: Contacts
    with tab2:
        st.header("Contacts")
        if st.button("Fetch Contacts"):
            contact_data = fetch_records_from_zakya(
                st.session_state['api_domain'],
                st.session_state['access_token'],
                st.session_state['organization_id'],
                '/contacts'
            )
            contacts_record = extract_record_list(contact_data,"contacts")
            show_preview = st.checkbox("Show/Hide Contacts",value=True, key="contacts_checkbox")
            if show_preview:
                contacts_df = pd.DataFrame.from_records(contacts_record)
                st.dataframe(contacts_df)
                if st.button("Save to Database", key="save_contacts",on_click=handle_on_click_button,args=(contacts_df,'zakya_contacts')):
                    st.success("Contacts saved to database successfully!")
    
    # Tab 3: Item Groups
    with tab3:
        st.header("Item Groups")
        if st.button("Fetch Item Groups"):
            item_groups_data = fetch_records_from_zakya(
                st.session_state['api_domain'],
                st.session_state['access_token'],
                st.session_state['organization_id'],
                '/itemgroups'
            )
            item_groups_record = extract_record_list(item_groups_data,"itemgroups")
            show_preview = st.checkbox("Show/Hide Item Groups",value=True, key="itemgroups_checkbox")
            if show_preview:                
                itemgroups_df = pd.DataFrame.from_records(item_groups_record)
                st.dataframe(itemgroups_df)
                if st.button("Save to Database", key="save_itemgroups",on_click=handle_on_click_button,args=(itemgroups_df,'zakya_item_groups')):
                    st.success("Item groups saved to database successfully!")
    
    # Tab 4: Items
    with tab4:
        st.header("Items")
        if st.button("Fetch Items"):
            items_data = fetch_records_from_zakya(
                st.session_state['api_domain'],
                st.session_state['access_token'],
                st.session_state['organization_id'],
                '/items'                
            )
            items_record = extract_record_list(items_data,"items")
            show_preview = st.checkbox("Show/Hide Products",value=True, key="items_checkbox")
            if show_preview:                 
                product_df = pd.DataFrame.from_records(items_record)                    
                st.dataframe(product_df)
                if st.button("Save to Database", key="save_items",on_click=handle_on_click_button,args=(product_df,'zakya_products')):
                    st.success("Items saved to database successfully!")
    
    # Tab 5: Sales Order
    with tab5:
        st.header("Sales Order")
        if st.button("Fetch Sales Orders"):
            sales_order_data = fetch_records_from_zakya(
                st.session_state['api_domain'],
                st.session_state['access_token'],
                st.session_state['organization_id'],
                '/salesorders'                  
            )
            sales_order_record = extract_record_list(sales_order_data,"salesorders")
            show_preview = st.checkbox("Show/Hide Sales Order",value=True, key="salesorder_checkbox")
            if show_preview:                 
                sales_order_df = pd.DataFrame.from_records(sales_order_record)
                st.dataframe(sales_order_df)
                if st.button("Save to Database", key="save_salesorder",on_click=handle_on_click_button, args=(sales_order_df,'zakya_sales_order')):
                    st.success("zakya_sales_order saved to database successfully!")
    
    # Tab 6: Transfer Order
    with tab6:
        st.header("Transfer Order")
        if st.button("Fetch Transfer Orders"):
            transfer_order_data = fetch_records_from_zakya(
                st.session_state['api_domain'],
                st.session_state['access_token'],
                st.session_state['organization_id'],
                '/transferorders'                  
            )
            print(f"Output is : {transfer_order_data}")
            transfer_order_records = extract_record_list(transfer_order_data,"transfer_orders")
            show_preview = st.checkbox("Show/Hide Transfer Order",value=True, key="transferorder_checkbox")
            if show_preview:                 
                transfer_order_df = pd.DataFrame.from_records(transfer_order_records)
                st.dataframe(transfer_order_df)
                if st.button("Save to Database", key="save_transferorder",on_click=handle_on_click_button, args=(transfer_order_df,'zakya_transfer_orders')):                    
                    st.success("zakya_transfer_orders saved to database successfully!")
    
    # Tab 7: Invoices
    with tab7:
        st.header("Invoices")
        if st.button("Fetch Invoices"):
            invoices_data = fetch_records_from_zakya(
                st.session_state['api_domain'],
                st.session_state['access_token'],
                st.session_state['organization_id'],
                '/invoices'                  
            )
            invoices_record = extract_record_list(invoices_data,"invoices")
            show_preview = st.checkbox("Show/Hide Invoices",value=True, key="invoices_checkbox")
            if show_preview:                 
                invoices_df = pd.DataFrame.from_records(invoices_record)
                st.dataframe(invoices_df)                   
                if st.button("Save to Database", key="save_invoices",on_click=handle_on_click_button, args=(invoices_df,'zakya_invoices')):
                    st.success("zakya_invoices saved to database successfully!")
    
    # Tab 8: Bills
    with tab8:
        st.header("Bills")
        if st.button("Fetch Bills"):
            bills_data = fetch_records_from_zakya(
                st.session_state['api_domain'],
                st.session_state['access_token'],
                st.session_state['organization_id'],
                '/bills'                  
            )
            bills_record = extract_record_list(bills_data,"bills")
            show_preview = st.checkbox("Show/Hide Bills",value=True, key="bills_checkbox")
            if show_preview:                 
                bills_df = pd.DataFrame.from_records(bills_record)
                st.dataframe(bills_df)                   
                if st.button("Save to Database", key="save_bills",on_click=handle_on_click_button,args=(bills_df,'zakya_bills')):                    
                    st.success("zakya_bills saved to database successfully!")
    
    # Tab 9: Price Books
    with tab9:
        st.header("Price Books")
        if st.button("Fetch Price Books"):
            price_books_data = fetch_records_from_zakya(
                st.session_state['api_domain'],
                st.session_state['access_token'],
                st.session_state['organization_id'],
                '/pricebooks'                  
            )
            pricebook_record = extract_record_list(price_books_data,"pricebooks")
            show_preview = st.checkbox("Show/Hide Price Books",value=True, key="pricebooks_checkbox")
            if show_preview:                 
                price_books_df = pd.DataFrame.from_records(pricebook_record)
                st.dataframe(price_books_df)
                if st.button("Save to Database", key="save_pricebooks",on_click=handle_on_click_button, args=(price_books_df,'zakya_pricebooks')):                    
                    st.success("zakya_pricebooks saved to database successfully!")
    
    # Tab 10: Tax Codes
    with tab10:
        st.header("Tax Codes")
        if st.button("Fetch Tax Codes"):
            tax_data = fetch_records_from_zakya(
                st.session_state['api_domain'],
                st.session_state['access_token'],
                st.session_state['organization_id'],
                '/settings/taxes'                  
            )
            tax_record = extract_record_list(tax_data,"taxes")
            show_preview = st.checkbox("Show/Hide Tax Codes",value=True, key="taxes_checkbox")
            if show_preview:                 
                tax_df = pd.DataFrame.from_records(tax_record)
                st.dataframe(tax_df)
                if st.button("Save to Database", key="save_taxes",on_click=handle_on_click_button,args=(tax_df,'zakya_taxes')):                    
                    st.success("zakya_taxes saved to database successfully!")
    
    # Tab 11: Login
    with tab11:
        st.header("Zakya Authentication")
        fetch_zakya_code()


def display_each_selected_row(invoice_id):
    details = fetch_object_for_each_id(
        api_domain=st.session_state['api_domain'],
        access_token=st.session_state['access_token'],
        organization_id=st.session_state['organization_id'],
        endpoint=f"/invoices/{invoice_id}"
    )
    
    # Display the details in an expander (simulating a popup)
    with st.expander(f"Invoice {invoice_id} Details",expanded=True):
        st.json(details)


def extract_record_list(input_data,key):
    records = []
    for record in input_data:
        records.extend(record[f'{key}'])
    return records


# Run the application
zakya_integration_function()