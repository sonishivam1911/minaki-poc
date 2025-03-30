import streamlit as st
import pandas as pd
from datetime import datetime
from config.logger import logger
from frontend_components.pernia.utils.state_manager import get_zakya_connection, update_product_mapping_status
from server.ppus_invoice_service import (
    create_missing_salesorders, 
    fetch_salesorders_by_customer_service,
    analyze_missing_salesorders,
    fetch_inventory_data
)

def sales_orders_tab():
    """Display the sales orders tab with two containers."""
    # Only proceed if we have Pernia orders
    if st.session_state.get('pernia_orders') is None:
        st.info("No Pernia orders loaded. Please select a customer and load orders first.")
        return
    
    # Check if product mapping analysis has been done
    if st.session_state.get('mapped_products') is None and st.session_state.get('unmapped_products') is None:
        st.warning("Please click 'Analyze Product Mapping' in the Pernia Orders section first.")
        return
    
    # Fetch sales orders if not already done
    if st.session_state.get('sales_orders') is None:
        if st.button("Fetch Existing Sales Orders", key="fetch_sales_btn"):
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
                    'include_inventory': True,
                    'pernia_orders' : st.session_state.get('pernia_orders'),
                    'start_date': st.session_state.get('start_date'),  # Add start date
                     'end_date': st.session_state.get('end_date'),
                    'date_filter_days': 45  # Add days to filter                    
                    }
                
                st.info(f"Start date is : {st.session_state.get('start_date')}")
                st.info(f"End date is : {st.session_state.get('end_date')}")
                # Fetch sales orders
                sales_orders = fetch_salesorders_by_customer_service(config)
                logger.debug(f"Sales order fetched are : {sales_orders}")
                
                # Fetch inventory data separately for all mapped products
                inventory_data = fetch_inventory_data(
                    zakya_connection, 
                    st.session_state.get('product_mapping', {})
                )
                
                # Store in session state
                st.session_state['sales_orders'] = sales_orders
                st.session_state['inventory_data'] = inventory_data
                
                # Now analyze which items need sales orders
                if sales_orders is not None and not isinstance(sales_orders, str):
                    # Check if pernia_orders exists and is not None
                    pernia_orders = st.session_state.get('pernia_orders')
                    if pernia_orders is not None:
                        # Check if product_mapping exists and is not None
                        product_mapping = st.session_state.get('product_mapping')
                        # st.info(f"Product Mapping : {product_mapping}")
                        if product_mapping is not None:
                            # Analyze missing sales orders
                            missing_orders,present_orders = analyze_missing_salesorders(
                                pernia_orders,
                                product_mapping,
                                sales_orders
                            )
                            
                            #logger.debug(f"Mising Sales Order : {missing_orders}")
                            st.session_state['missing_sales_orders'] = missing_orders
                            st.session_state['present_orders'] = present_orders
                            
                            # Update mapping status
                            update_product_mapping_status()
                        else:
                            st.error("Product mapping is missing. Please analyze product mapping first.")
                    else:
                        st.error("Pernia orders not found. Please load orders first.")
                else:
                    st.error("Failed to fetch sales orders.")
    
    # Display the two containers for sales orders
    st.markdown("### Existing Sales Orders")
    existing_sales_orders_container()
    
    st.markdown("### Missing Sales Orders")
    missing_sales_orders_container()

def existing_sales_orders_container():
    """Container 1: Display existing sales orders with item mapping details."""
    if st.session_state.get('sales_orders') is None:
        st.info("Click 'Fetch Existing Sales Orders' to see sales orders mapped to these products.")
        return
    
    sales_orders = st.session_state['sales_orders']
    
    # Check if sales_orders is a valid DataFrame
    if not isinstance(sales_orders, pd.DataFrame):
        st.error(f"Invalid sales orders data: {type(sales_orders)}")
        return
    
    if sales_orders.empty:
        st.warning("No existing sales orders found for these Pernia products.")
        return
    
    # Add inventory data to the display if available
    if st.session_state.get('inventory_data'):
        # Create a function to map item_id to inventory data for display
        def add_inventory_data(row):
            item_id = row.get('item_id')
            if item_id and item_id in st.session_state['inventory_data']:
                inv_data = st.session_state['inventory_data'][item_id]
                row['Available Stock'] = inv_data.get('available_stock', 'N/A')
                row['Stock on Hand'] = inv_data.get('stock_on_hand', 'N/A')
            else:
                row['Available Stock'] = 'N/A'
                row['Stock on Hand'] = 'N/A'
            return row
        
        # Apply the function to each row
        sales_orders = sales_orders.apply(add_inventory_data, axis=1)
    
    # Filter for products with "Received and QC Pass" status
    if 'Product Status' in sales_orders.columns:
        sales_orders = sales_orders[sales_orders['Product Status'] == 'Received and QC Pass']
        if sales_orders.empty:
            st.warning("No sales orders found with 'Received and QC Pass' status.")
            return
    
    # Enhance display to show sales order to item mapping
    # Group by relevant columns to show item-to-sales-order mapping
    display_columns = ['Order Number', 'Item Name', 'item_id', 'Order Date', 
                      'Total Quantity', 'Average Rate', 'Total Amount',
                      'Available Stock', 'Stock on Hand', 'Invoice Status']
    
    # Ensure all required columns exist
    existing_columns = set(sales_orders.columns)
    valid_columns = [col for col in display_columns if col in existing_columns]
    
    # Display the filtered data
    st.dataframe(st.session_state['present_orders'], use_container_width=True)
    
    # Add explanatory text
    st.caption("This table shows all items with 'Received and QC Pass' status that are mapped to sales orders.")
    
    # Download button
    csv = sales_orders.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Download Sales Orders as CSV",
        data=csv,
        file_name=f"existing_sales_orders_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )

def missing_sales_orders_container():
    """Container 2: Display missing sales orders and provide creation button."""
    # Check if we've analyzed missing sales orders
    if st.session_state.get('missing_sales_orders') is None:
        if st.session_state.get('sales_orders') is not None:
            st.info("Analyzing missing sales orders...")
            
            # Get key data needed for analysis
            pernia_orders = st.session_state.get('pernia_orders')
            product_mapping = st.session_state.get('product_mapping', {})
            sales_orders = st.session_state.get('sales_orders')

            #logger.debug(f" Product mapping from front end is : {product_mapping}")
            
            # Check if we have all required data
            if pernia_orders is not None and product_mapping and isinstance(sales_orders, pd.DataFrame):
                # Analyze missing sales orders
                missing_orders,present_orders = analyze_missing_salesorders(
                    pernia_orders,
                    product_mapping,
                    sales_orders
                )
                st.session_state['missing_sales_orders'] = missing_orders
                st.session_state['present_orders'] = present_orders
                
                # Update the all items mapped flag
                update_product_mapping_status()
            else:
                st.warning("Missing required data for sales order analysis")
                return
        else:
            st.info("Fetch existing sales orders first to identify missing ones.")
            return
    
    missing_orders = st.session_state.get('missing_sales_orders', pd.DataFrame())
    
    # Handle the case where missing_orders is not a DataFrame
    if not isinstance(missing_orders, pd.DataFrame):
        st.error(f"Invalid missing orders data: {type(missing_orders)}")
        st.session_state['missing_sales_orders'] = pd.DataFrame()  # Reset to empty DataFrame
        return
    
    if missing_orders.empty:
        st.success("All Pernia products have valid sales orders! 🎉")
        # Update the all items mapped flag
        st.session_state['all_items_mapped'] = True
        return
    
    # Display missing orders grouped by PO Number
    st.warning(f"Found {len(missing_orders)} products without valid sales orders")
    
    # Create tabs for mapped and unmapped products
    tab1, tab2 = st.tabs(["Mapped in Zakya", "Not Mapped in Zakya"])
    
    # Ensure 'is_mapped' column exists
    if 'is_mapped' not in missing_orders.columns:
        st.error("Missing required 'is_mapped' column in data")
        return
    
    # Display mapped products needing sales orders
    with tab1:
        mapped_missing = missing_orders[(missing_orders['is_mapped'] == True) & (missing_orders['Product Status'] == 'Received and QC Pass')]
        if not mapped_missing.empty:
            st.write(f"{len(mapped_missing)} products are mapped in Zakya but need sales orders:")
            
            # Add inventory data if available
            if st.session_state.get('inventory_data'):
                # Add inventory data to mapped products
                for idx, row in mapped_missing.iterrows():
                    item_id = row.get('item_id')
                    if item_id and item_id in st.session_state['inventory_data']:
                        inv_data = st.session_state['inventory_data'][item_id]
                        mapped_missing.at[idx, 'Available Stock'] = inv_data.get('available_stock', 'N/A')
                        mapped_missing.at[idx, 'Stock on Hand'] = inv_data.get('stock_on_hand', 'N/A')
            
            st.dataframe(mapped_missing, use_container_width=True)
        else:
            st.success("All mapped products have valid sales orders!")
    
    # Display unmapped products
    with tab2:
        unmapped_missing = missing_orders[(missing_orders['is_mapped'] == False) & (missing_orders['Product Status'] == 'Received and QC Pass')]
        if not unmapped_missing.empty:
            st.write(f"{len(unmapped_missing)} products are not mapped in Zakya:")
            st.dataframe(unmapped_missing, use_container_width=True)
        else:
            st.success("All products are mapped in Zakya!")
    
    # Button to create missing sales orders
    if st.button("Create Missing Sales Orders", key="create_missing_btn"):
        with st.spinner("Creating sales orders..."):
            # Group by PO Number and create sales orders
            zakya_connection = get_zakya_connection()
            customer_id = st.session_state['customer_id']
            
            results = create_missing_salesorders(
                missing_orders, 
                zakya_connection, 
                customer_id
            )
            
            # Display results
            if results.get('success'):
                st.success(f"Successfully created {results.get('created_count', 0)} sales orders!")
                st.json(results.get('details'))
                # map missing salesorder to pernia table okay ?
                missng_salesorder_reference_number_mapping = {}
                for obj in results.get('details'):
                    # remove PO
                    reference_number = obj.get('reference_number','').split(":")[-1].strip()
                    salesorder_id = obj.get('salesorder_id','')
                    missng_salesorder_reference_number_mapping[reference_number] = salesorder_id


                #update state 
                st.session_state['all_items_mapped'] = True
                st.session_state['missng_salesorder_reference_number_mapping'] = missng_salesorder_reference_number_mapping
                st.info("Please create invoices now")    
                
                # Force refresh
                # st.rerun()
            else:
                st.error(f"Error creating sales orders: {results.get('error', 'Unknown error')}")
                
                # Show detailed errors if available
                if results.get('details'):
                    with st.expander("Error Details"):
                        for i, detail in enumerate(results.get('details')):
                            if detail.get('status') == 'Failed':
                                st.write(f"Error with {detail.get('reference_number')}: {detail.get('error')}")