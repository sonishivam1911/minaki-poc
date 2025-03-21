import streamlit as st
from frontend_components.dashboard.sales_order_analysis import product_metrics_subpage
from frontend_components.dashboard.invoice_analysis import invoice_sub_dashboard_subpage
from frontend_components.sync_invoice_item_id_mapping_button_component import sync_widget

def index():

    with st.container():

        st.markdown("## Welcome To Minaki DashBoard")

        tab1, tab2 = st.tabs(['Sales Order Analytics', 'Invoice Analytics'])

        with tab1:
            product_metrics_subpage()
        
        with tab2:
            sync_widget()
            invoice_sub_dashboard_subpage()          


