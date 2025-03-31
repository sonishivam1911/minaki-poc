import streamlit as st
import pandas as pd
import time
from server.reports.create_invoice_retreive_table import sync_invoice_mappings_sync

# Page configuration
st.set_page_config(page_title="Invoice Sync Tool", layout="centered")

# Header
st.title("Invoice Sync Tool")

# Hardcoded configuration
config = {
    'api_domain': st.session_state['api_domain'],
    'access_token': st.session_state['access_token'], 
    'organization_id': st.session_state['organization_id'],
    'batch_size': 3
}

# Sync button
if st.button("Sync Invoices", type="primary", use_container_width=True):
    # Show processing indicator
    progress_placeholder = st.empty()
    results_placeholder = st.empty()
    
    progress_placeholder.info("Starting invoice synchronization...")
    
    # Start timing
    start_time = time.time()
    
    try:
        # Run the sync process
        with st.spinner("Processing invoices..."):
            line_item_mappings_df = sync_invoice_mappings_sync(config)
        
        # Calculate processing time
        processing_time = time.time() - start_time
        
        # Clear the progress message
        progress_placeholder.empty()
        
        # Display results
        if isinstance(line_item_mappings_df, pd.DataFrame) and not line_item_mappings_df.empty:
            # Count unique invoices
            unique_invoices = line_item_mappings_df['invoice_id'].nunique()
            
            # Show success message with stats
            results_placeholder.success(f"""
            ✅ Sync completed successfully in {processing_time:.2f} seconds!
            
            📊 Results:
            - Line items processed: {len(line_item_mappings_df)}
            - Unique invoices: {unique_invoices}
            """)
        else:
            # No data processed
            results_placeholder.warning("No new invoices to process.")
            
    except Exception as e:
        # Show error message
        progress_placeholder.empty()
        results_placeholder.error(f"❌ Error during synchronization: {str(e)}")

# # Footer
# st.markdown("---")
# st.caption("Invoice Sync Tool - Powered by Zakya API")