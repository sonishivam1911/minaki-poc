import streamlit as st
import pandas as pd
from config.logger import logger
from datetime import datetime, timedelta
from server.create_shiprocket_for_sales_orders import fetch_all_return_orders_service


def list_return_orders_component():
    with st.container():
        st.header("Return All Orders Summary Report")
        return_orders_df=fetch_all_return_orders_service()
        return_orders_df['created_at'] = pd.to_datetime(return_orders_df['created_at'], errors='coerce')      
        apply_advanced_filtering(return_orders_df)


def apply_advanced_filtering(df):
    # Date range filtering
    st.subheader("Filter Options")
    
    # Date range selection with default to last 30 days
    date_filter_type = st.radio(
        key="return_order",
        label="Date Filter Type", 
        options=["Last 30 Days", "Custom Date Range"]
    )
    
    if date_filter_type == "Last 30 Days":
        # Default to last 30 days
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
    else:
        # Custom date range selection
        start_date = st.date_input(
            "Start Date", 
            value=datetime.now() - timedelta(days=30)
        )
        end_date = st.date_input(
            "End Date", 
            value=datetime.now()
        )
        # Convert date objects to datetime objects for consistent comparison
        start_date = datetime.combine(start_date, datetime.min.time())
        end_date = datetime.combine(end_date, datetime.max.time())        
    
    # Status column filtering
    unique_statuses = df['status'].unique()
    selected_statuses = st.multiselect(
        "Filter by Status", 
        unique_statuses, 
        default=unique_statuses
    )
    
    # Channel Sales Order ID filtering - Dropdown
    # unique_channel_order_ids = sorted(df['channel_order_id'].unique())
    # selected_channel_order_ids = st.multiselect(
    #     "Select Channel Sales Order IDs", 
    #     unique_channel_order_ids,
    #     help="Select specific Channel Sales Order IDs"
    # )
    
    # Apply filters
    filtered_df = df.copy()
   
    logger.debug(f"type for created_at is :{type(filtered_df['created_at'])}")

    pd_start_date = pd.Timestamp(start_date)
    pd_end_date = pd.Timestamp(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)     
    # Date range filter
    filtered_df = filtered_df[
        (filtered_df['created_at'] >= pd_start_date) & 
        (filtered_df['created_at'] <= pd_end_date)
    ]
    
    # Status filter
    filtered_df = filtered_df[filtered_df['status'].isin(selected_statuses)]
    
    # Channel Sales Order ID dropdown filter
    # if selected_channel_order_ids:
    #     filtered_df = filtered_df[
    #         filtered_df['channel_sales_order_id'].isin(selected_channel_order_ids)
    #     ]
    
    

    
    # Display filtering summary
    st.markdown("---")
    st.metric("Total Records", len(df))
    st.metric("Filtered Records", len(filtered_df))
    
    # Display filtered dataframe
    st.dataframe(filtered_df)
    
    # Optional: Download filtered data
    if st.button(key="return-orders-component",label="Download Filtered Data"):
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            key="return-orders-component",
            label="Download CSV",
            data=csv,
            file_name="filtered_return_orders.csv",
            mime="text/csv"
        )