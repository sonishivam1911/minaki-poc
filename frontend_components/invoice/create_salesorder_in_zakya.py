import streamlit as st
import pandas as pd
from datetime import datetime
from config.logger import logger
from server.ppus_invoice_service import process_sales_orders

def create_missing_salesorder_component(config):
    """
    Component to create missing sales orders in Zakya.
    
    Args:
        config (dict): Configuration containing:
            - orders: DataFrame with order data
            - base_url: Zakya API base URL
            - access_token: Zakya API access token
            - organization_id: Zakya organization ID
            - Other optional config parameters
    """
    st.subheader("Create Missing Sales Orders")
    
    # Check if orders data is provided
    if 'orders' not in config or config['orders'] is None or config['orders'].empty:
        st.warning("No orders data provided. Please load orders first.")
        return
    
    orders_df = config['orders']
    
    # Display summary of orders to be processed
    st.write(f"Found {len(orders_df)} orders that need to be created as sales orders in Zakya.")
    
    # Group orders by PO Number to show summary
    if 'PO Number' in orders_df.columns:
        po_groups = orders_df.groupby('PO Number')
        po_summary = []
        
        for po_number, group in po_groups:
            po_summary.append({
                'PO Number': po_number,
                'Items Count': len(group),
                'Total Value': group['PO Value'].sum() if 'PO Value' in group.columns else 'N/A'
            })
        
        summary_df = pd.DataFrame(po_summary)
        st.write("Summary by PO Number:")
        st.dataframe(summary_df, use_container_width=True)
    
    # Options for sales order creation
    st.write("### Creation Options")
    
    col1, col2 = st.columns(2)
    
    with col1:
        quantity_value = st.number_input(
            "Default Quantity", 
            min_value=1,
            value=1,
            help="Default quantity for each line item"
        )
    
    with col2:
        order_source = st.text_input(
            "Order Source",
            value="Pernia",
            help="Source of the orders"
        )
    
    # Create button
    if st.button("Create Sales Orders in Zakya", type="primary"):
        with st.spinner("Creating sales orders..."):
            # Check if customer_id is available
            if 'customer_id' not in config or not config['customer_id']:
                st.error("Customer ID is required but not provided.")
                return
            
            # Prepare Zakya connection
            zakya_connection = {
                'base_url': config['base_url'],
                'access_token': config['access_token'],
                'organization_id': config['organization_id']
            }
            
            # Prepare options
            options = {
                'ref_field': 'PO Number',
                'date_field': 'PO Date',
                'delivery_date_field': 'Delivery Date',
                'price_field': 'PO Value',
                'sku_field': 'Vendor Code',
                'partner_sku_field': 'SKU Code',
                'description_field': 'Designer Name',
                'quantity_value': quantity_value,
                'order_source': order_source
            }
            
            # Call the backend function to create sales orders
            results = process_sales_orders(
                orders_df,
                config['customer_id'], 
                zakya_connection,
                options
            )
            
            # Display results
            if results['success']:
                st.success(f"Successfully created {results['created_count']} sales orders!")
                
                # Show detailed results
                if results['details']:
                    details_df = pd.DataFrame(results['details'])
                    st.write("Creation Details:")
                    st.dataframe(details_df, use_container_width=True)
                    
                    # Offer download of results
                    csv = details_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Download Results as CSV",
                        data=csv,
                        file_name=f"salesorder_creation_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                    )
            else:
                st.error("Failed to create some or all sales orders.")
                
                # Show errors
                if results['errors']:
                    st.write("Errors encountered:")
                    for error in results['errors']:
                        st.error(error)
                
                # Show details for partial success/failure
                if results['details']:
                    details_df = pd.DataFrame(results['details'])
                    st.write("Creation Details:")
                    st.dataframe(details_df, use_container_width=True)