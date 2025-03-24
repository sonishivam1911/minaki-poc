import streamlit as st
import pandas as pd
from datetime import datetime
import time

# Import the async mapping service
from server.reports.create_salesorder_retreive_table import (
    sync_salesorder_mappings_sync
)

# Page configuration
st.set_page_config(
    page_title="Zakya Data Mapping",
    page_icon="🔄"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.2rem;
        color: #2c3e50;
        text-align: center;
        margin-bottom: 20px;
    }
    .section-header {
        font-size: 1.5rem;
        color: #34495e;
        margin-bottom: 10px;
    }
    .status-container {
        padding: 15px;
        border-radius: 5px;
        margin-bottom: 15px;
    }
    .status-success {
        background-color: #d4edda;
        color: #155724;
    }
    .status-error {
        background-color: #f8d7da;
        color: #721c24;
    }
    .status-processing {
        background-color: #cce5ff;
        color: #004085;
    }
</style>
""", unsafe_allow_html=True)

# Session state initialization
if 'processing' not in st.session_state:
    st.session_state.processing = False
if 'invoice_result' not in st.session_state:
    st.session_state.invoice_result = None
if 'salesorder_result' not in st.session_state:
    st.session_state.salesorder_result = None
if 'log_messages' not in st.session_state:
    st.session_state.log_messages = []

# Helper function to add log messages
def add_log(message, level="info"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st.session_state.log_messages.append({
        "timestamp": timestamp,
        "message": message,
        "level": level
    })

# Function to process both mappings
def process_mappings(batch_size):
    try:
        st.session_state.processing = True
        add_log(f"Starting data mapping with batch size {batch_size}...")
        
        # Process invoice mappings
        add_log("Processing invoice mappings...")
        config = {
                'organization_id' : st.session_state['organization_id'],
                'access_token' : st.session_state['access_token'],
                'api_domain' : st.session_state['api_domain'],
                'batch_size' : 3,
            }        
        # Process sales order mappings
        add_log("Processing sales order mappings...")
        start_time = time.time()
        salesorder_df = sync_salesorder_mappings_sync(config)
        salesorder_time = time.time() - start_time
        
        if not salesorder_df.empty:
            st.session_state.salesorder_result = {
                "success": True,
                "count": len(salesorder_df),
                "time": salesorder_time
            }
            add_log(f"Successfully processed {len(salesorder_df)} sales order line items in {salesorder_time:.2f} seconds", "success")
        else:
            st.session_state.salesorder_result = {
                "success": False,
                "message": "No sales order data was processed",
                "time": salesorder_time
            }
            add_log("No sales order data was processed", "warning")
        
        total_time = salesorder_time
        add_log(f"Completed all processing in {total_time:.2f} seconds", "success")
        
    except Exception as e:
        error_message = str(e)
        add_log(f"Error during processing: {error_message}", "error")
        st.error(f"An error occurred: {error_message}")
    finally:
        st.session_state.processing = False

# Main app
def main():
    st.markdown('<div class="main-header">Zakya Data Mapping Tool</div>', unsafe_allow_html=True)
    
    # Main processing section
    st.write("""
    This tool will fetch data from Zakya and create mappings for:
    1. Invoices and their line items
    2. Sales orders and their line items
    
    The data will be processed asynchronously in batches.
    """)
    
    col1, col2 = st.columns([3, 1])
    
    with col2:
        batch_size = st.number_input("Batch Size", min_value=1, max_value=50, value=20)
    
    # Process button
    if st.button("Run Data Mapping", type="primary", disabled=st.session_state.processing):
        process_mappings(batch_size)
    
    # Show status during processing
    if st.session_state.processing:
        st.markdown("""
        <div class="status-container status-processing">
            <h3>⏳ Processing Data...</h3>
            <p>Please wait while the data is being processed. This may take a few minutes.</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Add a spinner for visual feedback
        with st.spinner("Processing..."):
            # This is just for visual effect as the actual processing happens in process_mappings
            while st.session_state.processing:
                time.sleep(0.1)
    
    # Results section
    if st.session_state.invoice_result or st.session_state.salesorder_result:
        st.markdown('<div class="section-header">Processing Results</div>', unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        with col2:
            if st.session_state.salesorder_result:
                result = st.session_state.salesorder_result
                if result.get("success"):
                    st.markdown(f"""
                    <div class="status-container status-success">
                        <h3>✅ Sales Order Mapping Complete</h3>
                        <p>Successfully processed <b>{result.get('count', 0)}</b> sales order line items.</p>
                        <p>Processing time: <b>{result.get('time', 0):.2f}</b> seconds</p>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown(f"""
                    <div class="status-container status-error">
                        <h3>❌ Sales Order Mapping Issue</h3>
                        <p>{result.get('message', 'No data was processed')}</p>
                        <p>Processing time: <b>{result.get('time', 0):.2f}</b> seconds</p>
                    </div>
                    """, unsafe_allow_html=True)
    
    # Log section
    if st.session_state.log_messages:
        st.markdown('<div class="section-header">Process Log</div>', unsafe_allow_html=True)
        
        log_df = pd.DataFrame(st.session_state.log_messages)
        
        # Style the log based on level
        def style_log(row):
            if row['level'] == 'error':
                return ['color: #dc3545'] * len(row)
            elif row['level'] == 'warning':
                return ['color: #ffc107'] * len(row)
            elif row['level'] == 'success':
                return ['color: #28a745'] * len(row)
            else:
                return ['color: #212529'] * len(row)
        
        styled_log = log_df.style.apply(style_log, axis=1)
        st.dataframe(styled_log, use_container_width=True)
        
        if st.button("Clear Log"):
            st.session_state.log_messages = []
            # st.experimental_rerun()

if __name__ == "__main__":
    main()