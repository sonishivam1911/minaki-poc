# pages/all_salesorders.py
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from schema.zakya_schemas.schema import ZakyaContacts
from config.constants import customer_mapping_zakya_contacts, CustomerGstTreatmentKey
from server.sales_order_serivce import fetch_salesorders_by_customer, fetch_product_metrics_for_sales_order_by_customer
from main import load_customer_data, fetch_customer_data
from server.reports.update_salesorder_items_id_mapping_table import sync_salesorder_mappings_sync
from config.logger import logger

# Page configuration
st.set_page_config(
    page_title="All Sales Orders Dashboard",
    page_icon="📈",
    layout="wide"
)

def get_zakya_connection():
    """Return Zakya connection details from session state."""
    return {
        'base_url': st.session_state['api_domain'],
        'access_token': st.session_state['access_token'],
        'organization_id': st.session_state['organization_id']
    }

# Initialize session state for this page
def initialize_session_variables():
    if 'all_contacts_customer_master_df' not in st.session_state:
        st.session_state['all_contacts_customer_master_df'] = None
    if 'all_contacts_selected_customer' not in st.session_state:
        st.session_state['all_contacts_selected_customer'] = None
    if 'all_contacts_customer_id' not in st.session_state:
        st.session_state['all_contacts_customer_id'] = None
    if 'all_contacts_sales_orders' not in st.session_state:
        st.session_state['all_contacts_sales_orders'] = None
    # Add new session state variable for sub-page navigation
    if 'current_subpage' not in st.session_state:
        st.session_state['current_subpage'] = "Overview"

def deinitialize_session_variables():
    st.session_state['all_contacts_customer_master_df'] = None
    st.session_state['all_contacts_selected_customer'] = None
    st.session_state['all_contacts_customer_id'] = None
    st.session_state['all_contacts_sales_orders'] = None   

def load_customer_data_into_state():
    if st.session_state['all_contacts_customer_master_df'] is None:
        with st.spinner("Loading customer data..."):
            st.session_state['all_contacts_customer_master_df'] = load_customer_data()
            if st.session_state['all_contacts_customer_master_df'] is not None:
                st.success("Customer data loaded successfully")
            else:
                st.error("Failed to load customer data")          

def container_for_sales_order_mapping():
    with st.container():
        st.subheader("Sync Sales Order Mapping")
        if st.button("🔄 Sync Sales Orders DB", use_container_width=True):
            with st.spinner("Syncing sales order data..."):
                updated_mappings = sync_salesorder_mappings_sync()
                if not updated_mappings.empty:
                    st.success(f"Sync complete! {len(updated_mappings)} total mappings available.")
                
                # Show the results in an expander
                    with st.expander("View Results"):
                        st.dataframe(updated_mappings, use_container_width=True)
                else:
                    st.warning("No new mappings needed or sync process failed.")

# CONTAINER 1: Customer Selection
def container_for_customer_selection():
    with st.container():
        st.subheader("1️⃣ Customer Selection")
    
        customer_df = st.session_state['all_contacts_customer_master_df']
    
        if customer_df is not None and len(customer_df) > 0:
        # Get a list of all customer names
            customer_names = customer_df['contact_name'].unique().tolist()
            customer_names.sort()  # Sort alphabetically
        
        # Customer selection dropdown
            selected_customer = st.selectbox(
            "Select Customer", 
            customer_names
        )
        
        # Update session state when customer changes
            if selected_customer != st.session_state.get('all_contacts_selected_customer'):
                st.session_state['all_contacts_selected_customer'] = selected_customer
                      
                customer_data = fetch_customer_data(ZakyaContacts, {
                customer_mapping_zakya_contacts['branch_name']: {
                    'op': 'eq', 'value': selected_customer
                },
                customer_mapping_zakya_contacts['gst_treatment'] : {
                    'op': 'eq', 'value': CustomerGstTreatmentKey
                }
            })
            
                if customer_data and len(customer_data) > 0:
                    customer_data = customer_data[0]
                    logger.debug(f"customer_data is  : {customer_data}")
                    customer_id = customer_data.get('contact_id')
                    st.session_state['all_contacts_customer_id'] = customer_id
                
                # Display customer details
                    st.info(f"Selected Customer: {selected_customer} (ID: {customer_id})")
                else:
                    st.error(f"Could not fetch details for customer {selected_customer}")
                    st.session_state['all_contacts_customer_id'] = None
        
        # Fetch button
            if st.button("Get Sales Orders", type="primary"):
                if not st.session_state.get('all_contacts_customer_id'):
                    st.error("Please select a valid customer to continue")
                else:
                    with st.spinner("Fetching sales orders..."):
                        config = get_zakya_connection()
                        config['customer_id'] = st.session_state['all_contacts_customer_id']
                    
                    # We need to modify the fetch_salesorders_by_customer function to work without pernia_orders
                    # For now, let's assume it returns all sales orders for the customer when pernia_orders is None
                        sales_orders = fetch_salesorders_by_customer(config)
                    
                        if sales_orders.empty:
                            st.warning(f"No sales orders found for {selected_customer}")
                            st.session_state['all_contacts_sales_orders'] = None
                        else:
                            st.session_state['all_contacts_sales_orders'] = sales_orders
                            st.success(f"Found {len(sales_orders)} sales orders for {selected_customer}")
        else:
            st.warning("No customer data available")

# CONTAINER 2: Display Sales Orders with Filtering
def container_for_sales_order_filtering():
    with st.container():
        st.subheader("2️⃣ Sales Orders")
    
        if st.session_state.get('all_contacts_sales_orders') is not None:
            sales_orders = st.session_state['all_contacts_sales_orders']
        
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
                if 'Item Name' in date_filtered_orders.columns and not date_filtered_orders.empty:
                # Get unique item names from date-filtered data
                    unique_items = date_filtered_orders['Item Name'].dropna().unique().tolist()
                    unique_items.sort()  # Sort alphabetically
                
                # Add "All Items" option at the beginning
                    filter_options = ["All Items"] + unique_items
                
                # Item name dropdown
                    item_name_filter = st.selectbox(
                    "Filter by Item Name",
                    options=filter_options,
                    index=0,  # Default to "All Items"
                    key="all_contacts_item_filter"
                )
                else:
                    st.warning("No items found in the selected date range" if date_filtered_orders.empty else "Item name column not found in data")
                    item_name_filter = "All Items"
        
        # Apply item name filter if not "All Items"
            filtered_orders = date_filtered_orders.copy()
        
            if item_name_filter != "All Items" and 'Item Name' in filtered_orders.columns:
                filtered_orders = filtered_orders[filtered_orders['Item Name'] == item_name_filter]
        
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
                st.warning(f"No sales orders found for {st.session_state['all_contacts_selected_customer']} with current filters")
            else:
                st.success(f"Found {len(filtered_orders)} sales orders for {st.session_state['all_contacts_selected_customer']}{filter_text}")
            
            # Display filtered data
                st.dataframe(filtered_orders, use_container_width=True)
            
            # Download button for filtered data
                csv = filtered_orders.to_csv(index=False).encode('utf-8')
                st.download_button(
                label="Download Sales Orders as CSV",
                data=csv,
                file_name=f"sales_orders_{st.session_state['all_contacts_selected_customer']}.csv",
                mime="text/csv",
            )
        else:
            st.info("No sales orders to display. Please select a customer and click 'Get Sales Orders'.")

# NEW FUNCTION: Order Details Sub-Page
def product_metrics_subpage():
    with st.container():
        st.subheader("Product Metrics Dashboard")
        
        # Get data
        df = fetch_product_metrics_for_sales_order_by_customer()
        
        # Create filters in a sidebar
        filter_col1, filter_col2 = st.columns(2)

        with filter_col1:
            # Get unique categories for filter
            df["category_name"].fillna('',inplace=True)
            categories = ["All Categories"] + sorted(df["category_name"].unique().tolist())
            selected_category = st.selectbox("Select Category", categories)
        
        with filter_col2:
            # Get unique customers for filter
            df["customer_name"].fillna('',inplace=True)
            customers = ["All Customers"] + sorted(df["customer_name"].unique().tolist())
            selected_customer = st.selectbox("Select Customer", customers)


        # Add date range filter
        date_col1, date_col2 = st.columns(2)
        
        # Get min and max dates from the dataframe - with safer handling of date types
        if 'order_date' in df.columns and len(df) > 0:
            # Handle both datetime and date objects
            min_date = df['order_date'].min()
            max_date = df['order_date'].max()
            
            # Check if we need to convert to date
            if hasattr(min_date, 'date'):
                min_date = min_date.date()
            if hasattr(max_date, 'date'):
                max_date = max_date.date()
        else:
            # Fallback dates if data is empty or date column missing
            import datetime
            min_date = datetime.date.today() - datetime.timedelta(days=365)
            max_date = datetime.date.today()
        
        with date_col1:
            start_date = st.date_input("Start Date", value=min_date, min_value=min_date, max_value=max_date)
        
        with date_col2:
            end_date = st.date_input("End Date", value=max_date, min_value=min_date, max_value=max_date)
        
        # Apply filters
        filtered_df = df.copy()
        
        if selected_category != "All Categories":
            filtered_df = filtered_df[filtered_df["category_name"] == selected_category]
            
        if selected_customer != "All Customers":
            filtered_df = filtered_df[filtered_df["customer_name"] == selected_customer]

        # Apply date filter
        if 'order_date' in filtered_df.columns:
            # Convert date_input objects to datetime for comparison
            filtered_df = filtered_df[
                (filtered_df['order_date'] >= start_date) & 
                (filtered_df['order_date'] <= end_date)
            ]
        
        # Show filter status
        st.write(f"Showing data for: {'All Categories' if selected_category == 'All Categories' else selected_category} | "
                f"{'All Customers' if selected_customer == 'All Customers' else selected_customer} | "
                f"Period: {start_date} to {end_date}")            
        
        # # Show filter status
        # st.write(f"Showing data for: {'All Categories' if selected_category == 'All Categories' else selected_category} | {'All Customers' if selected_customer == 'All Customers' else selected_customer}")
        
        # Display data
        st.subheader("Product Sales Data")
        
        # Show number of rows and columns
        st.write(f"Total records: {filtered_df.shape[0]}")
        
        # Display the dataframe
        st.dataframe(filtered_df)
        
        # Download option
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            label="Download data as CSV",
            data=csv,
            file_name="product_metrics.csv",
            mime="text/csv"
        )
        
        # Additional metrics
        st.subheader("Summary Metrics")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Revenue", f"{filtered_df['total_item_revenue'].sum():,.2f}")
        with col2:
            st.metric("Total Quantity Sold", f"{filtered_df['total_quantity'].sum():,}")
        with col3:
            st.metric("Average Order Value", f"{filtered_df['total_order_value'].mean():,.2f}")

# NEW FUNCTION: Order Analytics Sub-Page
def order_analytics_subpage():
    st.header("Order Analytics")
    
    if st.session_state.get('all_contacts_sales_orders') is None:
        st.info("Please select a customer and fetch sales orders first")
        return
    
    sales_orders = st.session_state['all_contacts_sales_orders']
    
    if not sales_orders.empty:
        # Sample analytics sections
        st.subheader("Order Trends")
        
        # Check if necessary columns exist for analysis
        date_col = None
        for possible_col in ['delivery_date', 'order_date', 'date']:
            if possible_col in sales_orders.columns:
                date_col = possible_col
                break
        
        if date_col:
            # Ensure date column is datetime
            if not pd.api.types.is_datetime64_dtype(sales_orders[date_col]):
                sales_orders[date_col] = pd.to_datetime(sales_orders[date_col], errors='coerce')
            
            # Group by month and count orders
            sales_orders['month'] = sales_orders[date_col].dt.to_period('M')
            monthly_orders = sales_orders.groupby('month').size().reset_index(name='order_count')
            monthly_orders['month_str'] = monthly_orders['month'].astype(str)
            
            # Create a simple bar chart
            st.bar_chart(data=monthly_orders.set_index('month_str')['order_count'])
            
            # Item analysis
            if 'Item Name' in sales_orders.columns:
                st.subheader("Top Items")
                item_counts = sales_orders['Item Name'].value_counts().reset_index()
                item_counts.columns = ['Item Name', 'Count']
                st.dataframe(item_counts.head(10), use_container_width=True)
            
            # Add more analytics as needed
        else:
            st.warning("No date column found for trend analysis")
    else:
        st.warning("No sales order data available for analysis")

# NEW FUNCTION: Settings Sub-Page
def settings_subpage():
    st.header("Settings")
    
    st.subheader("Display Settings")
    
    # Example settings
    col1, col2 = st.columns(2)
    
    with col1:
        st.selectbox(
            "Default Date Filter",
            options=["Last 30 days", "Last 60 days", "Last 90 days", "Last 120 days", "All time"],
            index=3  # Default to 120 days
        )
        
        st.number_input(
            "Records Per Page",
            min_value=10,
            max_value=100,
            value=25,
            step=5
        )
    
    with col2:
        st.selectbox(
            "Default Sort Column",
            options=["Order Date", "Delivery Date", "Order ID", "Amount"],
            index=0
        )
        
        st.checkbox("Auto-refresh Data", value=False)
    
    st.subheader("API Settings")
    
    # Connection settings (read-only display)
    connection_info = get_zakya_connection()
    st.text_input("API Base URL", value=connection_info['base_url'], disabled=True)
    st.text_input("Organization ID", value=connection_info['organization_id'], disabled=True)
    
    # Save settings button
    if st.button("Save Settings", type="primary"):
        st.success("Settings saved successfully!")
        # In a real app, you would save these to session_state or a config file

# Function to render navigation and the selected sub-page
def render_subpages():
    # Create navigation tabs
    tabs = ["Overview", "Product Metrics", "Analytics", "Settings"]
    
    cols = st.columns(len(tabs))
    for i, tab in enumerate(tabs):
        if cols[i].button(tab, key=f"tab_{tab}", use_container_width=True):
            st.session_state['current_subpage'] = tab
    
    # Divider line
    st.markdown("---")
    
    # Render the selected sub-page
    if st.session_state['current_subpage'] == "Overview":
        # This is the original main view
        container_for_customer_selection()
        container_for_sales_order_filtering()
    elif st.session_state['current_subpage'] == "Product Metrics":
        product_metrics_subpage()
    elif st.session_state['current_subpage'] == "Analytics":
        order_analytics_subpage()
    elif st.session_state['current_subpage'] == "Settings":
        settings_subpage()

def main():
    initialize_session_variables()  
    # Main app
    st.title("All Contacts Sales Orders Dashboard")
    # Load customer data if not already loaded
    load_customer_data_into_state() 

    container_for_sales_order_mapping()
    
    # Render subpages instead of directly calling the containers
    render_subpages()

main()