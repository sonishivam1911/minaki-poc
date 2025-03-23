import streamlit as st
from datetime import datetime, timedelta
from frontend_components.pernia.components.customer_selector import customer_selection_section
from frontend_components.pernia.components.orders_display import pernia_orders_section
from frontend_components.pernia.components.salesorder_tab import sales_orders_tab
from frontend_components.pernia.components.invoice_tab import invoice_tab
from frontend_components.pernia.utils.state_manager import initialize_session_state

# Page configuration
st.set_page_config(
    page_title="Pernia Orders Dashboard",
    page_icon="📊",
    layout="wide"
)

def main():
    """Main application function."""
    st.title("Pernia Orders Dashboard")
    
    # Initialize session state
    initialize_session_state()
    
    # CONTAINER 1: Customer Selection
    with st.container():
        customer_selection_section()
    
    # CONTAINER 2: Pernia Orders Overview
    with st.container():
        pernia_orders_section()
    
    # CONTAINER 3: Tabbed interface for Sales Orders and Invoices
    if st.session_state.get('pernia_orders') is not None:
        with st.container():
            st.subheader("Order Management")
            
            # Create tabs
            tab1, tab2 = st.tabs(["Sales Orders", "Invoices"])
            
            # Sales Orders Tab
            with tab1:
                sales_orders_tab()
            
            # Invoices Tab (enabled only when all sales orders are mapped)
            with tab2:
                # Check if all items have sales orders mapped
                if st.session_state.get('all_items_mapped', False):
                    invoice_tab()
                else:
                    st.warning("All items must have sales orders mapped before generating invoices. Please complete the sales order mapping in the Sales Orders tab.")

if __name__ == "__main__":
    main()