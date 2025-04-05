import streamlit as st
import json
import pandas as pd
from server.create_shiprocket_for_sales_orders import create_packages_on_zakya
# Import Zakya API functions
from utils.zakya_api import (
    fetch_records_from_zakya,
    retrieve_record_from_zakya,
    post_record_to_zakya,
    put_record_to_zakya,
    extract_record_list
)

def zakya_create_package():
    st.header("Create Packages")

    # Input for object (only for RETRIEVE & PUT)
    object_id = None
    object_id = st.text_input("Object ID", placeholder="object_id")

    # Trigger button
    if st.button("Execute API Call"):
        try:
            # Fetch API details from session state
            api_domain = st.session_state['api_domain']
            access_token = st.session_state['access_token']
            organization_id = st.session_state['organization_id']

            # Call the respective function
            so_data = retrieve_record_from_zakya(api_domain, access_token, organization_id, f'salesorders/{object_id}')
            print(so_data)
            response = create_packages_on_zakya(so_data)
            print(response)

            show_preview = True
            show_preview = st.checkbox(f"Show/Hide Package DataFrame",value=True)
            if show_preview:                 
                response_df = pd.DataFrame.from_records(response)
                st.dataframe(response_df)
            
            show_preview_json = False
            show_preview_json = st.checkbox(f"Show/Hide Package JSON Response",value=True)
            if show_preview_json:
                st.json(response)

        except Exception as e:
            st.error(f"An error occurred: {str(e)}")

# Run the function
zakya_create_package()
