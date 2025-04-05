import streamlit as st
import pandas as pd
from datetime import datetime
from frontend_components.pernia.utils.state_manager import get_zakya_connection
from server.invoice.route import process_aza_sales
from frontend_components.invoice.create_salesorder_in_zakya import create_missing_salesorder_component

def aza_invoice_tab():
    """Display the invoice tab content for Aza."""
    st.subheader("Generate Invoices")
    
    # Verify all requirements are met
    if not st.session_state.get('all_items_mapped', False):
        st.warning("All items must be mapped to sales orders before generating invoices.")
        return
    
    if st.session_state.get('aza_orders') is None:
        st.warning("No Aza orders loaded. Cannot generate invoices.")
        return
    
    # Display summary of orders ready for invoicing
    aza_orders = st.session_state['aza_orders']
    sales_orders = st.session_state.get('aza_sales_orders')
    #logger.debug(f"Missing Sales Order : {missing_orders}")
    missing_orders = st.session_state['aza_missing_sales_orders']
    present_orders = st.session_state['present_orders'] 
    missng_salesorder_reference_number_mapping = st.session_state.get('missng_salesorder_reference_number_mapping',{})
    output = []

    for _,row in aza_orders.iterrows():

        reference_number = str(row.get('PO Number'))
        # logger.debug(f"Pernia row to dict is : {row.to_dict()}")
        aza_row_dict=row.to_dict()
        if reference_number in missng_salesorder_reference_number_mapping:
            aza_row_dict['Mapped Salesorder ID']  = missng_salesorder_reference_number_mapping[reference_number]
        else:
            sales_orders_dict=sales_orders[sales_orders['Mapped POs'] == reference_number].to_dict('records')[0]
            aza_row_dict['Mapped Salesorder ID'] = sales_orders_dict['Mapped Salesorder ID']

        output.append(aza_row_dict)
    
    
    mapped_salesorder_pernia_order_df=pd.DataFrame.from_records(output)
    with st.container():
        st.subheader("Mapped Salesorder IDs with pernia orders")
        st.dataframe(mapped_salesorder_pernia_order_df,use_container_width=True)

    
    # Invoice date selection
    invoice_date = st.date_input(
        "Invoice Date", 
        value=datetime.now().date(),
        help="Select the date to use for the generated invoices"
    )
    
    # Get customer name
    customer_name = st.session_state.get('selected_customer')
    if customer_name:
        st.write(f"Customer: {customer_name}")
    else:
        st.error("Customer not selected. Please check the file selection section.")
        return
    
    # Invoice generation button
    if st.button("Generate Invoices", type="primary"):
        generate_aza_invoices(aza_orders, invoice_date, customer_name)

def generate_aza_invoices(df, invoice_date, customer_name):
    """Generate invoices for Aza orders."""
    with st.spinner("Generating invoices..."):
        # Get connection details for Zakya
        zakya_connection = get_zakya_connection()
        
        # Process the sales data and generate invoice
        result_df = process_aza_sales(
            df, 
            invoice_date,
            customer_name,
            zakya_connection
        )

        # Store the invoice results
        st.session_state['aza_invoices'] = result_df