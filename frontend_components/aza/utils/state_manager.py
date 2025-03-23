import streamlit as st

def initialize_aza_session_state():
    """Initialize all session state variables needed for the Aza dashboard."""
    # Customer and order data
    if 'aza_designer_name' not in st.session_state:
        st.session_state['aza_designer_name'] = None
    if 'selected_customer' not in st.session_state:
        st.session_state['selected_customer'] = None
    if 'customer_id' not in st.session_state:
        st.session_state['customer_id'] = None
    if 'aza_orders' not in st.session_state:
        st.session_state['aza_orders'] = None
    
    # Product mapping data
    if 'aza_mapped_products' not in st.session_state:
        st.session_state['aza_mapped_products'] = None
    if 'aza_unmapped_products' not in st.session_state:
        st.session_state['aza_unmapped_products'] = None
    if 'aza_product_mapping' not in st.session_state:
        st.session_state['aza_product_mapping'] = {}  # SKU to item_id mapping
    
    # Sales order data
    if 'aza_sales_orders' not in st.session_state:
        st.session_state['aza_sales_orders'] = None
    if 'aza_missing_sales_orders' not in st.session_state:
        st.session_state['aza_missing_sales_orders'] = None
    if 'all_items_mapped' not in st.session_state:
        st.session_state['all_items_mapped'] = False
    
    # Inventory data
    if 'aza_inventory_data' not in st.session_state:
        st.session_state['aza_inventory_data'] = {}  # item_id to inventory details mapping
    
    # Invoice data
    if 'aza_invoices' not in st.session_state:
        st.session_state['aza_invoices'] = None

def update_aza_product_mapping_status():
    """Update the status of product mapping and related flags."""
    # Calculate whether all products are mapped to sales orders
    if (st.session_state.get('aza_missing_sales_orders') is not None and 
        len(st.session_state['aza_missing_sales_orders']) == 0):
        st.session_state['all_items_mapped'] = True
    else:
        st.session_state['all_items_mapped'] = False