import streamlit as st
from config.logger import logger
from frontend_components.select_salesorder_and_display_details_by_id import select_salesorder_and_display_details_by_id
from frontend_components.book_shipment_component import book_shipment_component
from frontend_components.shipment_order_component import shipment_order_component
from frontend_components.list_shipment_component import list_shipment_component
from frontend_components.list_return_orders_component import list_return_orders_component

def shiprocket_streamlit_interface():
    st.subheader("Create Shiprocket Shipment")
    
    # Initialize state variables if they don't exist
    if 'courier_df' not in st.session_state:
        st.session_state['courier_df'] = None
    if 'courier_fetched' not in st.session_state:
        st.session_state['courier_fetched'] = False
    if 'sales_order_details' not in st.session_state:
        st.session_state['sales_order_details'] = None
    
    selected_so_id = select_salesorder_and_display_details_by_id()
    # Use a form for the shipment details to prevent reloading on each change
    book_shipment_component(selected_so_id)

def main():

    with st.container():
        st.markdown('## Minaki Courier Service')

        tab1, tab2 = st.tabs(['Book Courier', 'Order Shipment Details'])

        with tab1:
            shiprocket_streamlit_interface()
        
        with tab2:
            shipment_order_component()
            list_shipment_component()
            list_return_orders_component()


main()