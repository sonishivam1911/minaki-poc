import streamlit as st
import pandas as pd
from datetime import datetime
from frontend_components.pernia.utils.state_manager import get_zakya_connection
from server.ppus_invoice_service import create_invoice_from_sales_orders
from config.logger import logger

def invoice_tab():
    """Display the invoice tab content."""
    st.subheader("Generate Invoices")
    
    # Verify all requirements are met
    if not st.session_state.get('all_items_mapped', False):
        st.warning("All items must be mapped to sales orders before generating invoices.")
        return
    
    if st.session_state.get('pernia_orders') is None:
        st.warning("No Pernia orders loaded. Cannot generate invoices.")
        return
    
    # Display summary of orders ready for invoicing
    pernia_orders = st.session_state.get('pernia_orders')
    sales_orders = st.session_state.get('sales_orders')
    present_orders = st.session_state.get('present_orders')
    missing_orders = st.session_state.get('missing_sales_orders')
    missng_salesorder_reference_number_mapping = st.session_state.get('missng_salesorder_reference_number_mapping',{})


    st.write(f"Ready to generate invoices for {len(pernia_orders)} Pernia orders.")

    output = []
    for _,row in pernia_orders.iterrows():

        reference_number = str(row.get('PO Number'))
        # logger.debug(f"Pernia row to dict is : {row.to_dict()}")
        pernia_row_dict=row.to_dict()
        if reference_number in missng_salesorder_reference_number_mapping:
            pernia_row_dict['Mapped Salesorder ID']  = missng_salesorder_reference_number_mapping[reference_number]
        else:
            sales_orders_dict=sales_orders[sales_orders['Mapped POs'] == reference_number].to_dict('records')[0]
            pernia_row_dict['Mapped Salesorder ID'] = sales_orders_dict['Mapped Salesorder ID']

        output.append(pernia_row_dict)

    mapped_salesorder_pernia_order_df=pd.DataFrame.from_records(output)
    with st.container():
        st.subheader("Mapped Salesorder IDs with pernia orders")
        st.dataframe(mapped_salesorder_pernia_order_df,use_container_width=True)


    # with st.container():
    #     st.subheader("Sales Order Dataframe")
    #     st.dataframe(sales_orders,use_container_width=True)

    
    # Invoice date selection
    invoice_date = st.date_input(
        "Invoice Date", 
        value=datetime.now().date(),
        help="Select the date to use for the generated invoices"
    )
    
    # Invoice generation button
    if st.button("Generate Invoices", type="primary"):
        generate_invoices(pernia_orders, invoice_date,present_orders,missing_orders,missng_salesorder_reference_number_mapping)

def generate_invoices(df, invoice_date,present_orders,missing_orders,missng_salesorder_reference_number_mapping):
    """Generate invoices for Pernia orders."""
    with st.spinner("Generating invoices..."):
        # Get connection details for Zakya
        zakya_connection = get_zakya_connection()
        
        # Process the sales data and generate invoice
        customer_name = st.session_state['selected_customer']
        result=create_invoice_from_sales_orders(
            sales_orders_df=present_orders,
            missing_salesorder_df=missing_orders,
            missng_salesorder_reference_number_mapping=missng_salesorder_reference_number_mapping,
            zakya_connection=zakya_connection,
            customer_name=customer_name,
            invoice_date=invoice_date

        )

        with st.container():
            st.subheader("Created Invoice Summary")
            st.dataframe(result['invoice_df'],use_container_width=True)


        if not result['adjustment_df'].empty:
            with st.container():
                st.subheader("Adjustment Items Summary")
                st.dataframe(result['adjustment_df'],use_container_width=True)
        
            # Store the invoice results


        st.session_state['invoices'] = result['invoice_df']
        st.session_state['adjustment_df'] = result['adjustment_df']