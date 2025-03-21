import streamlit as st
from server.create_shiprocket_for_sales_orders import sales_order_id_number_mapping_dict
from frontend_components.set_session_variable_for_salesorder_by_id import set_session_variable_for_salesorder_by_id

def select_salesorder_and_display_details_by_id():
    sales_order_mapping = sales_order_id_number_mapping_dict()
    
    # First section: Sales Order Selection
    st.subheader("1. Select Sales Order")
    selected_so_number = st.selectbox(
        "Sales Order",
        options=list(sales_order_mapping.values()),
        format_func=lambda x: f"SO-{x}"
    )
    
    # Get the selected sales order ID from the mapping
    selected_so_id = [k for k, v in sales_order_mapping.items() if v == selected_so_number][0]
    
    # Fetch and display sales order details
    if st.button("View Sales Order Details"):
        set_session_variable_for_salesorder_by_id(selected_so_id)
                #logger.error(f"Error fetching sales order details: {str(e)}")
    
    # Display sales order details if available
    if st.session_state.get('sales_order_details'):
        so_details = st.session_state['sales_order_details']
        
        with st.expander("Sales Order Details", expanded=True):
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Basic Information**")
                st.write(f"**SO Number:** {so_details.get('salesorder_number', 'N/A')}")
                st.write(f"**Reference:** {so_details.get('reference_number', 'N/A')}")
                st.write(f"**Date:** {so_details.get('date', 'N/A')}")
                st.write(f"**Status:** {so_details.get('status', 'N/A')}")
                st.write(f"**Customer:** {so_details.get('customer_name', 'N/A')}")
            
            with col2:
                st.markdown("**Shipping Information**")
                if 'shipping_address' in so_details:
                    shipping = so_details['shipping_address']
                    address_parts = []
                    if shipping.get('address'):
                        address_parts.append(shipping.get('address'))
                    if shipping.get('city'):
                        address_parts.append(shipping.get('city'))
                    if shipping.get('state'):
                        address_parts.append(shipping.get('state'))
                    if shipping.get('zip'):
                        address_parts.append(shipping.get('zip'))
                    if shipping.get('country'):
                        address_parts.append(shipping.get('country'))
                    
                    st.write(f"**Ship To:** {', '.join(address_parts)}")
                    st.write(f"**Shipment Date:** {so_details.get('shipment_date', 'N/A')}")
                else:
                    st.write("No shipping address available")
            
            # Display line items if available
            if 'line_items' in so_details and so_details['line_items']:
                st.markdown("**Products**")
                for i, item in enumerate(so_details['line_items']):
                    st.write(f"**{i+1}.** {item.get('name', 'N/A')} - Qty: {item.get('quantity', 'N/A')}")
            
            # Display total amount
            st.markdown(f"**Total Amount:** {so_details.get('currency_code', '')} {so_details.get('total', 'N/A')}")
            return selected_so_id