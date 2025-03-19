import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from schema.zakya_schemas.schema import ZakyaContacts
from config.constants import customer_mapping_zakya_contacts
from server.ppus_invoice_service import fetch_pernia_data_from_database, fetch_salesorders_by_customer
from server.invoice.route import process_pernia_sales
from main import load_customer_data, fetch_customer_data
from config.logger import logger

# Page configuration
st.set_page_config(
    page_title="Pernia Orders Dashboard",
    page_icon="📊",
    layout="wide"  # Using wide layout for better container spacing
)

def get_zakya_connection():
    """Return Zakya connection details from session state."""
    return {
        'base_url': st.session_state['api_domain'],
        'access_token': st.session_state['access_token'],
        'organization_id': st.session_state['organization_id']
    }

# Main app
def main():
    st.title("Pernia Orders Dashboard")
    
    # Initialize session state
    initialize_cache()
    
    # Load customer data if not already loaded
    if st.session_state['customer_master_df'] is None:
        with st.spinner("Loading customer data..."):
            st.session_state['customer_master_df'] = load_customer_data()
            if st.session_state['customer_master_df'] is not None:
                st.success("Customer data loaded successfully")
            else:
                st.error("Failed to load customer data")
                return
    
    # CONTAINER 1: Customer Selection
    with st.container():
        st.subheader("1️⃣ Customer Selection")
        
        # Filter for Pernia customers
        customer_df = st.session_state['customer_master_df']
        pernia_customers = customer_df[customer_df['contact_name'].str.contains('Pernia', case=False, na=False)]
        
        if len(pernia_customers) > 0:
            # Get a list of Pernia customer names
            pernia_customer_names = pernia_customers['contact_name'].unique().tolist()
            
            # Pre-select "Pernia Delhi" if it exists in the list
            default_idx = pernia_customer_names.index("Pernia Delhi") if "Pernia Delhi" in pernia_customer_names else 0
            
            # Create two columns for customer selection and date range
            col1, col2, col3 = st.columns([1, 1, 1])
            
            with col1:
                # Customer selection dropdown (pre-filled)
                selected_customer = st.selectbox(
                    "Select Pernia Customer", 
                    pernia_customer_names,
                    index=default_idx
                )
            
            # Date selection in the same container
            with col2:
                # Default to last month
                default_end_date = datetime.now()
                default_start_date = default_end_date - timedelta(days=30)
                start_date = st.date_input("Start Date", default_start_date)
            
            with col3:
                end_date = st.date_input("End Date", default_end_date)
            
            # Update session state when customer changes
            if selected_customer != st.session_state.get('selected_customer'):
                st.session_state['selected_customer'] = selected_customer
                st.session_state['orders_finalized'] = False  # Reset finalization status
                          
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
            
            # Fetch button at the bottom of container 1
            if st.button("Get Pernia Orders", type="primary"):
                if not st.session_state.get('customer_id'):
                    st.error("Please select a valid customer to continue")
                else:
                    # Convert dates to string format
                    start_date_str = start_date.strftime('%Y-%m-%d')
                    end_date_str = end_date.strftime('%Y-%m-%d')
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
                            st.success(f"Found {len(orders)} Pernia orders between {start_date} and {end_date}")
        else:
            st.warning("No Pernia customers found in the customer master data")
    
    # CONTAINER 2: Display Pernia Orders
    with st.container():
        st.subheader("2️⃣ Pernia Orders")
        
        if st.session_state.get('pernia_orders') is not None:
            df = st.session_state['pernia_orders']
            
            # Display the orders table
            st.dataframe(df, use_container_width=True)
            
            # Download button
            col1, col2 = st.columns([1, 3])
            with col1:
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download as CSV",
                    data=csv,
                    file_name=f"pernia_orders_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                )
            
            # Finalize button
            with col2:
                if st.button("Finalize Orders", type="secondary"):
                    st.session_state['orders_finalized'] = True
                    st.success("Orders finalized! Associated salesorders are displayed below.")
            
            # Invoice generation option
            st.subheader("Invoice Generation")
            if st.button("Generate Invoice"):
                with st.spinner("Generating invoice..."):
                    # Get connection details for Zakya
                    zakya_connection = get_zakya_connection()
                    
                    # Process the sales data and generate invoice
                    customer_name = st.session_state['selected_customer']
                    result_df = process_pernia_sales(
                        df, 
                        datetime.now(),  # Use current date for invoice
                        customer_name,
                        zakya_connection
                    )
                    
                    # Display results
                    st.subheader("Invoice Results")
                    st.dataframe(result_df, use_container_width=True)
                    
                    # Show success or error message
                    if "Success" in result_df["status"].values:
                        st.success(f"Invoice generated successfully: {result_df['invoice_number'].iloc[0]}")
                    else:
                        st.error(f"Error generating invoice: {result_df['error'].iloc[0]}")
        else:
            st.info("No Pernia orders to display. Please select a customer and date range, then click 'Get Pernia Orders'.")
    
    # CONTAINER 3: Associated Salesorders (only shown when orders are finalized)
    with st.container():
        st.subheader("3️⃣ Associated Salesorders")
        
        with st.spinner("Fetching associated salesorders..."):
            customer_id = st.session_state['customer_id']
            config = get_zakya_connection()
            config['customer_id'] = customer_id
            config['pernia_orders'] = st.session_state['pernia_orders']
            sales_orders = fetch_salesorders_by_customer(config)
            
            if sales_orders.empty:
                st.warning(f"No sales orders found for {st.session_state['selected_customer']}")
            else:
                # Step 1: First apply date filtering
                filter_col1, filter_col2 = st.columns(2)
                
                with filter_col2:
                    # Date filter - default to 120 days
                    date_options = ["Last 30 days", "Last 60 days", "Last 90 days", "Last 120 days", "All time"]
                    date_filter = st.selectbox("Filter by Date", date_options, index=3)  # Default to 120 days
                
                # Apply date filter first
                date_filtered_orders = sales_orders.copy()
                
                if date_filter != "All time" and 'delivery_date' in date_filtered_orders.columns:
                    # Extract number of days from selected option
                    days = int(date_filter.split()[1])
                    cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
                    
                    # Convert to datetime for comparison if needed
                    if not pd.api.types.is_datetime64_dtype(date_filtered_orders['delivery_date']):
                        date_filtered_orders['delivery_date'] = pd.to_datetime(date_filtered_orders['delivery_date'], errors='coerce')
                    
                    # Filter to only include orders within the specified period
                    date_filtered_orders = date_filtered_orders[date_filtered_orders['delivery_date'] >= cutoff_date]
                
                # Step 2: Now populate item dropdown from date-filtered results
                with filter_col1:
                    # Create item name dropdown from unique values in date-filtered orders
                    if 'item_name' in date_filtered_orders.columns and not date_filtered_orders.empty:
                        # Get unique item names from date-filtered data
                        unique_items = date_filtered_orders['item_name'].dropna().unique().tolist()
                        unique_items.sort()  # Sort alphabetically
                        
                        # Add "All Items" option at the beginning
                        filter_options = ["All Items"] + unique_items
                        
                        # Item name dropdown
                        item_name_filter = st.selectbox(
                            "Filter by Item Name",
                            options=filter_options,
                            index=0,  # Default to "All Items"
                            key="item_filter"
                        )
                    else:
                        st.warning("No items found in the selected date range" if date_filtered_orders.empty else "Item name column not found in data")
                        item_name_filter = "All Items"
                
                # Apply item name filter if not "All Items"
                filtered_orders = date_filtered_orders.copy()
                
                if item_name_filter != "All Items" and 'item_name' in filtered_orders.columns:
                    filtered_orders = filtered_orders[filtered_orders['item_name'] == item_name_filter]
                
                # Build success message with filter info
                filter_info = []
                if item_name_filter != "All Items":
                    filter_info.append(f"item name is '{item_name_filter}'")
                if date_filter != "All time":
                    filter_info.append(f"within {date_filter.lower()}")
                
                filter_text = " and ".join(filter_info)
                if filter_text:
                    filter_text = f" where {filter_text}"
                
                if filtered_orders.empty:
                    st.warning(f"No sales orders found for {st.session_state['selected_customer']} with current filters")
                else:
                    st.success(f"Found {len(filtered_orders)} sales orders for {st.session_state['selected_customer']}{filter_text}")
                    
                    # Display filtered data
                    st.dataframe(filtered_orders, use_container_width=True)
                    
                    # Download button for filtered data
                    csv = filtered_orders.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Download Sales Orders as CSV",
                        data=csv,
                        file_name=f"sales_orders_{st.session_state['selected_customer']}.csv",
                        mime="text/csv",
                    )

                    
def initialize_cache():
    if 'customer_master_df' not in st.session_state:
        st.session_state['customer_master_df'] = None
    if 'selected_customer' not in st.session_state:
        st.session_state['selected_customer'] = None
    if 'customer_id' not in st.session_state:
        st.session_state['customer_id'] = None
    if 'pernia_orders' not in st.session_state:
        st.session_state['pernia_orders'] = None
    if 'orders_finalized' not in st.session_state:
        st.session_state['orders_finalized'] = False

if __name__ == "__main__":
    main()