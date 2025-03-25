import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from schema.zakya_schemas.schema import ZakyaContacts
from config.constants import customer_mapping_zakya_contacts
from server.ppus_invoice_service import fetch_pernia_data_from_database
from main import load_customer_data, fetch_customer_data
from config.logger import logger

def customer_selection_section():
    """Create the customer selection section UI."""
    st.subheader("Customer Selection")
    
    # Load customer data if not already loaded
    if st.session_state['customer_master_df'] is None:
        with st.spinner("Loading customer data..."):
            st.session_state['customer_master_df'] = load_customer_data()
            if st.session_state['customer_master_df'] is None:
                st.error("Failed to load customer data")
                return
    
    # Filter for Pernia customers
    customer_df = st.session_state['customer_master_df']
    pernia_customers = customer_df[customer_df['contact_name'].str.contains('Pernia', case=False, na=False)]
    
    if len(pernia_customers) == 0:
        st.warning("No Pernia customers found in the customer master data")
        return
        
    # Get a list of Pernia customer names
    pernia_customer_names = pernia_customers['contact_name'].unique().tolist()
    
    # Pre-select "Pernia Delhi" if it exists in the list
    default_idx = pernia_customer_names.index("Pernia Delhi") if "Pernia Delhi" in pernia_customer_names else 0
    
    # Create columns for customer selection and date range
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        # Customer selection dropdown (pre-filled)
        selected_customer = st.selectbox(
            "Select Pernia Customer", 
            pernia_customer_names,
            index=default_idx
        )
    
    # Date selection
    with col2:
        # Default to last month
        default_end_date = datetime.now()
        default_start_date = default_end_date - timedelta(days=30)
        start_date = st.date_input("Start Date", default_start_date)
        st.session_state['start_end'] = start_date
    
    with col3:
        end_date = st.date_input("End Date", default_end_date)
    
    # Update customer selection in session state
    if selected_customer != st.session_state.get('selected_customer'):
        update_customer_selection(selected_customer)
    
    # Fetch button
    if st.button("Get Pernia Orders", type="primary"):
        fetch_pernia_orders(start_date, end_date)

def update_customer_selection(selected_customer):
    """Update session state when customer selection changes."""
    st.session_state['selected_customer'] = selected_customer
            
    customer_data = fetch_customer_data(ZakyaContacts, {
        customer_mapping_zakya_contacts['branch_name']: {
            'op': 'eq', 'value': selected_customer
        }
    })
    
    if customer_data and len(customer_data) > 0:
        customer_data = customer_data[0]
        logger.debug(f"Customer data after filtering is {customer_data}")
        customer_id = customer_data.get('contact_id')
        st.session_state['customer_id'] = customer_id
        
        # Display customer details
        st.info(f"Selected Customer: {selected_customer} (ID: {customer_id})")
    else:
        st.error(f"Could not fetch details for customer {selected_customer}")
        st.session_state['customer_id'] = None

def fetch_pernia_orders(start_date, end_date):
    """Fetch Pernia orders based on date range."""
    if not st.session_state.get('customer_id'):
        st.error("Please select a valid customer to continue")
        return
    
    # Convert dates to string format
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    st.session_state['start_date'] = start_date_str
    st.session_state['end_date'] = end_date_str
    input_params = {
        'start_date': start_date_str,
        'end_date': end_date_str
    }
    
    with st.spinner("Fetching Pernia orders..."):
        orders = fetch_pernia_data_from_database(input_params)

        
        if not orders or len(orders) == 0:
            st.warning("No Pernia orders found in the selected date range.")
            st.session_state['pernia_orders'] = None
        else:
            # Convert to dataframe and store in session state
            df = pd.DataFrame(orders)
            st.session_state['pernia_orders'] = df
            
            # Reset other related state variables when orders change
            st.session_state['mapped_products'] = None
            st.session_state['unmapped_products'] = None
            st.session_state['product_mapping'] = {}
            st.session_state['sales_orders'] = None
            st.session_state['missing_sales_orders'] = None
            st.session_state['all_items_mapped'] = False
            
            st.success(f"Found {len(orders)} Pernia orders between {start_date} and {end_date}")