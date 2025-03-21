import streamlit as st
import pandas as pd
import datetime
from server.invoice_service import fetch_product_metrics_for_invoice_by_customer

def invoice_sub_dashboard_subpage():
    with st.container():

        st.subheader("Invoice Dashboard")
        
        # Get data
        df = fetch_product_metrics_for_invoice_by_customer()
        
        # Create filters in a sidebar
        filter_col1, filter_col2, filter_col3 = st.columns(3)

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


        with filter_col3:
            # Get unique customers for filter
            df["customer_type"].fillna('',inplace=True)
            customer_type = ["All Customer Types"] + sorted(df["customer_type"].unique().tolist())
            selected_customer_type = st.selectbox("Select Customer Type", customer_type)

        # Add date range filter
        date_col1, date_col2 = st.columns(2)
        
        # Get min and max dates from the dataframe - with safer handling of date types
        # if 'invoice_date' in df.columns and len(df) > 0:
        #     # Handle both datetime and date objects
        #     min_date = df['invoice_date'].min()
        #     max_date = df['invoice_date'].max()
            
        #     # Check if we need to convert to date
        #     if hasattr(min_date, 'date'):
        #         min_date = min_date.date()
        #     if hasattr(max_date, 'date'):
        #         max_date = max_date.date()
        # else:
        #     # Fallback dates if data is empty or date column missing
        
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

        if selected_customer_type != "All Customer Types":
            filtered_df = filtered_df[filtered_df["customer_type"] == selected_customer_type]            

        filtered_df['invoice_date'] = pd.to_datetime(filtered_df['invoice_date'])
        # Apply date filter
        if 'invoice_date' in filtered_df.columns:
            # Convert date_input objects to datetime for comparison
            filtered_df = filtered_df[
                (filtered_df['invoice_date'].dt.date >= start_date) & 
                (filtered_df['invoice_date'].dt.date <= end_date)
            ]
        
        # Show filter status
        st.write(f"Showing data for: {'All Categories' if selected_category == 'All Categories' else selected_category} | "
                f"{'All Customers' if selected_customer == 'All Customers' else selected_customer} | "
                f"Period: {start_date} to {end_date}")            
        
        # # Show filter status
        # st.write(f"Showing data for: {'All Categories' if selected_category == 'All Categories' else selected_category} | {'All Customers' if selected_customer == 'All Customers' else selected_customer}")
        
        # Display data
        st.subheader("Invoice Metrics For Customer Selected")
        
        # Show number of rows and columns
        st.write(f"Total records: {filtered_df.shape[0]}")
        
        # Display the dataframe
        st.dataframe(filtered_df)
        
        # Download option
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            label="Download data as CSV",
            data=csv,
            file_name="invoice_metrics.csv",
            mime="text/csv"
        )
        
        # Additional metrics
        # st.subheader("Summary Metrics")
        
        # col1, col2, col3 = st.columns(3)
        # with col1:
        #     st.metric("Total Revenue", f"{filtered_df['total_item_revenue'].sum():,.2f}")
        # with col2:
        #     st.metric("Total Quantity Sold", f"{filtered_df['total_quantity'].sum():,}")
        # with col3:
        #     st.metric("Average Order Value", f"{filtered_df['total_order_value'].mean():,.2f}")