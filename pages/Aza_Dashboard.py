import streamlit as st
from frontend_components.aza.utils.state_manager import initialize_aza_session_state
from frontend_components.aza.components.file_selector import aza_file_selection_section
from frontend_components.aza.components.orders_display import aza_orders_section
from frontend_components.aza.components.salesorder_tab import aza_sales_orders_tab
from frontend_components.aza.components.invoice_tab import aza_invoice_tab

# Page configuration
st.set_page_config(
    page_title="Aza Orders Dashboard",
    page_icon="📊",
    layout="wide"
)

def main():
    """Main application function."""
    st.title("Aza Orders Dashboard")
    
    # Initialize session state
    initialize_aza_session_state()
    
    # CONTAINER 1: File/Customer Selection
    with st.container():
        aza_file_selection_section()
    
    # CONTAINER 2: Aza Orders Overview
    with st.container():
        aza_orders_section()
    
    # CONTAINER 3: Tabbed interface for Sales Orders and Invoices
    if st.session_state.get('aza_orders') is not None:
        with st.container():
            st.subheader("Order Management")
            
            # Create tabs
            tab1, tab2 = st.tabs(["Sales Orders", "Invoices"])
            
            # Sales Orders Tab
            with tab1:
                aza_sales_orders_tab()
            
            # Invoices Tab (enabled only when all sales orders are mapped)
            with tab2:
                # Check if all items have sales orders mapped
                if st.session_state.get('all_items_mapped', False):
                    aza_invoice_tab()
                else:
                    st.warning("All items must have sales orders mapped before generating invoices. Please complete the sales order mapping in the Sales Orders tab.")

if __name__ == "__main__":
    main()