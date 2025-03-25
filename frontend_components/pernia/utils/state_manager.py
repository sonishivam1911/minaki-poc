import streamlit as st

def initialize_session_state():
    """Initialize all session state variables needed for the Pernia dashboard."""
    # Customer and order data
    if 'customer_master_df' not in st.session_state:
        st.session_state['customer_master_df'] = None
    if 'selected_customer' not in st.session_state:
        st.session_state['selected_customer'] = None
    if 'customer_id' not in st.session_state:
        st.session_state['customer_id'] = None
    if 'pernia_orders' not in st.session_state:
        st.session_state['pernia_orders'] = None

    if 'start_end' not in st.session_state:
        st.session_state['start_end'] = None
    
    # Product mapping data
    if 'mapped_products' not in st.session_state:
        st.session_state['mapped_products'] = None
    if 'unmapped_products' not in st.session_state:
        st.session_state['unmapped_products'] = None
    if 'product_mapping' not in st.session_state:
        st.session_state['product_mapping'] = {}  # SKU to item_id mapping
    
    # Sales order data
    if 'sales_orders' not in st.session_state:
        st.session_state['sales_orders'] = None
    if 'missing_sales_orders' not in st.session_state:
        st.session_state['missing_sales_orders'] = None
    if 'all_items_mapped' not in st.session_state:
        st.session_state['all_items_mapped'] = False
    
    # Inventory data
    if 'inventory_data' not in st.session_state:
        st.session_state['inventory_data'] = {}  # item_id to inventory details mapping
    
    # Invoice data
    if 'invoices' not in st.session_state:
        st.session_state['invoices'] = None

def get_zakya_connection():
    """Return Zakya connection details from session state."""
    return {
        'base_url': st.session_state['api_domain'],
        'access_token': st.session_state['access_token'],
        'organization_id': st.session_state['organization_id']
    }

def update_product_mapping_status():
    """Update the status of product mapping and related flags."""
    # Calculate whether all products are mapped to sales orders
    if (st.session_state.get('missing_sales_orders') is not None and 
        len(st.session_state['missing_sales_orders']) == 0):
        st.session_state['all_items_mapped'] = True
    else:
        st.session_state['all_items_mapped'] = False