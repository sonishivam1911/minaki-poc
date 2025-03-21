import streamlit as st
from config.logger import logger
from server.reports.update_invoice_item_ids_mapping_table import sync_invoice_mappings_sync

def sync_widget():
    """
    Creates a simple widget with just a refresh button to synchronize invoices and mappings.
    """
    with st.container():
        col1, col2 = st.columns([3, 1])
        
        with col1:
            st.subheader("Invoice Database Sync")
        
        with col2:
            sync_button = st.button("🔄 Sync Now", type="primary")
        
        # Status message placeholder
        status_message = st.empty()
        
        if sync_button:
            try:
                # Display syncing message
                status_message.info("Syncing database and mappings...")
                results = sync_invoice_mappings_sync()
                
                # Show success message with count
                if results is not None and not results.empty:
                    invoice_count = len(results['invoice_id'].unique()) if 'invoice_id' in results.columns else 0
                    status_message.success(f"✅ Sync complete! {invoice_count} invoices synchronized.")
                else:
                    status_message.warning("Sync completed, but no data was returned.")
                    
            except Exception as e:
                status_message.error(f"❌ Sync failed: {str(e)}")
                logger.error(f"Sync error: {str(e)}")
