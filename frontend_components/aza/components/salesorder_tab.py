import streamlit as st
import pandas as pd
from datetime import datetime
from config.logger import logger
from frontend_components.pernia.utils.state_manager import get_zakya_connection
from frontend_components.aza.utils.state_manager import update_aza_product_mapping_status
from server.ppus_invoice_service import (
    create_missing_salesorders, 
    fetch_salesorders_by_customer_service,
    analyze_missing_salesorders,
    fetch_inventory_data
)

def aza_sales_orders_tab():
    """Display the sales orders tab for Aza with two containers."""
    # Only proceed if we have Aza orders
    if st.session_state.get('aza_orders') is None:
        st.info("No Aza orders loaded. Please upload an Aza sales file first.")
        return
    
    # Check if product mapping analysis has been done
    if st.session_state.get('aza_mapped_products') is None and st.session_state.get('aza_unmapped_products') is None:
        st.warning("Please click 'Analyze Product Mapping' in the Aza Orders section first.")
        return
    
    # Fetch sales orders if not already done
    if st.session_state.get('aza_sales_orders') is None:
        if st.button("Fetch Existing Sales Orders", key="aza_fetch_sales_btn"):
            with st.spinner("Fetching sales orders and inventory data..."):
                # Get Zakya connection details
                zakya_connection = get_zakya_connection()
                customer_id = st.session_state.get('customer_id')
                
                if not customer_id:
                    st.error("Customer ID not found. Please select a customer first.")
                    return
                
                # Prepare config for API call
                config = {
                    'base_url': zakya_connection.get('base_url'),
                    'access_token': zakya_connection.get('access_token'),
                    'organization_id': zakya_connection.get('organization_id'),
                    'customer_id': customer_id,
                    'include_inventory': True  # Request inventory data
                }
                
                # Fetch sales orders
                sales_orders = fetch_salesorders_by_customer_service(config)
                
                # Fetch inventory data separately for all mapped products
                inventory_data = fetch_inventory_data(
                    zakya_connection, 
                    st.session_state.get('aza_product_mapping', {})
                )
                
                # Store in session state
                st.session_state['aza_sales_orders'] = sales_orders
                st.session_state['aza_inventory_data'] = inventory_data
                
                # Now analyze which items need sales orders
                if sales_orders is not None and not isinstance(sales_orders, str):
                    # Check if aza_orders exists and is not None
                    aza_orders = st.session_state.get('aza_orders')
                    if aza_orders is not None:
                        # Check if product_mapping exists and is not None
                        product_mapping = st.session_state.get('aza_product_mapping')
                        if product_mapping is not None:
                            # Analyze missing sales orders
                            missing_orders = analyze_missing_salesorders(
                                aza_orders,
                                product_mapping,
                                sales_orders,
                                sku_field="SKU"
                            )
                            
                            logger.debug(f"Missing Sales Order : {missing_orders}")
                            st.session_state['aza_missing_sales_orders'] = missing_orders
                            
                            # Update mapping status
                            update_aza_product_mapping_status()
                        else:
                            st.error("Product mapping is missing. Please analyze product mapping first.")
                    else:
                        st.error("Aza orders not found. Please upload an Aza file first.")
                else:
                    st.error("Failed to fetch sales orders.")
    
    # Display the two containers for sales orders
    st.markdown("### Existing Sales Orders")
    aza_existing_sales_orders_container()
    
    st.markdown("### Missing Sales Orders")
    aza_missing_sales_orders_container()

def aza_existing_sales_orders_container():
    """Container 1: Display existing sales orders."""
    if st.session_state.get('aza_sales_orders') is None:
        st.info("Click 'Fetch Existing Sales Orders' to see sales orders mapped to these products.")
        return
    
    sales_orders = st.session_state['aza_sales_orders']
    
    # Check if sales_orders is a valid DataFrame
    if not isinstance(sales_orders, pd.DataFrame):
        st.error(f"Invalid sales orders data: {type(sales_orders)}")
        return
    
    if sales_orders.empty:
        st.warning("No existing sales orders found for these Aza products.")
        return
    
    # Add inventory data to the display if available
    if st.session_state.get('aza_inventory_data'):
        # Create a function to map item_id to inventory data for display
        def add_inventory_data(row):
            item_id = row.get('item_id')
            if item_id and item_id in st.session_state['aza_inventory_data']:
                inv_data = st.session_state['aza_inventory_data'][item_id]
                row['Available Stock'] = inv_data.get('available_stock', 'N/A')
                row['Stock on Hand'] = inv_data.get('stock_on_hand', 'N/A')
            else:
                row['Available Stock'] = 'N/A'
                row['Stock on Hand'] = 'N/A'
            return row
        
        # Apply the function to each row
        sales_orders = sales_orders.apply(add_inventory_data, axis=1)
    
    # Display sales orders with enhanced columns
    st.dataframe(sales_orders, use_container_width=True)
    
    # Download button
    csv = sales_orders.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Download Sales Orders as CSV",
        data=csv,
        file_name=f"aza_existing_sales_orders_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )

def aza_missing_sales_orders_container():
    """Container 2: Display missing sales orders and provide creation button."""
    # Check if we've analyzed missing sales orders
    if st.session_state.get('aza_missing_sales_orders') is None:
        if st.session_state.get('aza_sales_orders') is not None:
            st.info("Analyzing missing sales orders...")
            
            # Get key data needed for analysis
            aza_orders = st.session_state.get('aza_orders')
            product_mapping = st.session_state.get('aza_product_mapping', {})
            sales_orders = st.session_state.get('aza_sales_orders')
            
            # Check if we have all required data
            if aza_orders is not None and product_mapping and isinstance(sales_orders, pd.DataFrame):
                # Analyze missing sales orders
                missing_orders = analyze_missing_salesorders(
                    aza_orders,
                    product_mapping,
                    sales_orders,
                    sku_field="SKU"
                )
                st.session_state['aza_missing_sales_orders'] = missing_orders
                
                # Update the all items mapped flag
                update_aza_product_mapping_status()
            else:
                st.warning("Missing required data for sales order analysis")
                return
        else:
            st.info("Fetch existing sales orders first to identify missing ones.")
            return
    
    missing_orders = st.session_state.get('aza_missing_sales_orders', pd.DataFrame())
    
    # Handle the case where missing_orders is not a DataFrame
    if not isinstance(missing_orders, pd.DataFrame):
        st.error(f"Invalid missing orders data: {type(missing_orders)}")
        st.session_state['aza_missing_sales_orders'] = pd.DataFrame()  # Reset to empty DataFrame
        return
    
    if missing_orders.empty:
        st.success("All Aza products have valid sales orders! 🎉")
        # Update the all items mapped flag
        st.session_state['all_items_mapped'] = True
        return
    
    # Display missing orders
    st.warning(f"Found {len(missing_orders)} products without valid sales orders")
    
    # Create tabs for mapped and unmapped products
    tab1, tab2 = st.tabs(["Mapped in Zakya", "Not Mapped in Zakya"])
    
    # Ensure 'is_mapped' column exists
    if 'is_mapped' not in missing_orders.columns:
        st.error("Missing required 'is_mapped' column in data")
        return
    
    # Display mapped products needing sales orders
    with tab1:
        mapped_missing = missing_orders[missing_orders['is_mapped'] == True]
        if not mapped_missing.empty:
            st.write(f"{len(mapped_missing)} products are mapped in Zakya but need sales orders:")
            
            # Add inventory data if available
            if st.session_state.get('aza_inventory_data'):
                # Add inventory data to mapped products
                for idx, row in mapped_missing.iterrows():
                    item_id = row.get('item_id')
                    if item_id and item_id in st.session_state['aza_inventory_data']:
                        inv_data = st.session_state['aza_inventory_data'][item_id]
                        mapped_missing.at[idx, 'Available Stock'] = inv_data.get('available_stock', 'N/A')
                        mapped_missing.at[idx, 'Stock on Hand'] = inv_data.get('stock_on_hand', 'N/A')
            
            st.dataframe(mapped_missing, use_container_width=True)
        else:
            st.success("All mapped products have valid sales orders!")
    
    # Display unmapped products
    with tab2:
        unmapped_missing = missing_orders[missing_orders['is_mapped'] == False]
        if not unmapped_missing.empty:
            st.write(f"{len(unmapped_missing)} products are not mapped in Zakya:")
            st.dataframe(unmapped_missing, use_container_width=True)
        else:
            st.success("All products are mapped in Zakya!")
    
    # Button to create missing sales orders
    if st.button("Create Missing Sales Orders", key="aza_create_missing_btn"):
        with st.spinner("Creating sales orders..."):
            # Group by designer and create sales orders
            zakya_connection = get_zakya_connection()
            customer_id = st.session_state['customer_id']
            
            results = create_missing_salesorders(
                missing_orders, 
                zakya_connection, 
                customer_id,
                sku_field="SKU"
            )
            
            # Display results
            if results.get('success'):
                st.success(f"Successfully created {results.get('created_count', 0)} sales orders!")
                
                # Refresh sales order data
                st.session_state['aza_sales_orders'] = None
                st.session_state['aza_missing_sales_orders'] = None
                
                # Force refresh
                st.rerun()
            else:
                st.error(f"Error creating sales orders: {results.get('error', 'Unknown error')}")
                
                # Show detailed errors if available
                if results.get('details'):
                    with st.expander("Error Details"):
                        for i, detail in enumerate(results.get('details')):
                            if detail.get('status') == 'Failed':
                                st.write(f"Error with {detail.get('reference_number')}: {detail.get('error')}")