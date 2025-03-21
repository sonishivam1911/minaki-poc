import streamlit as st
from utils.zakya_api import fetch_object_for_each_id

def set_session_variable_for_salesorder_by_id(selected_so_id):
    with st.spinner('Fetching sales order details...'):
        try:
            # Fetch sales order details
            sales_order_response = fetch_object_for_each_id(
                st.session_state['api_domain'],
                st.session_state['access_token'],
                st.session_state['organization_id'],
                f'/salesorders/{selected_so_id}'
            )
            
            if 'salesorder' in sales_order_response:
                sales_order_details = sales_order_response['salesorder']
                st.session_state['sales_order_details'] = sales_order_details
            else:
                st.error("Failed to fetch sales order details")
        except Exception as e:
            st.error(f"Error fetching sales order details: {str(e)}")    