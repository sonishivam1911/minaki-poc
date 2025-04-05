import streamlit as st
import json
import pandas as pd

# Import Zakya API functions
from utils.zakya_api import (
    fetch_records_from_zakya,
    retrieve_record_from_zakya,
    post_record_to_zakya,
    put_record_to_zakya,
    extract_record_list
)

def zakya_api_interaction():
    st.header("Zakya API Playground")

    # Radio buttons to select the operation
    operation = st.radio("Choose API Operation:", ["fetch", "retrieve", "post", "put", "post-args"])

    # Input for API endpoint
    endpoint = st.text_input("API Endpoint", placeholder="endpoint")

    # Input for object (only for RETRIEVE & PUT)
    object_id = None
    if operation in ["retrieve", "put"]:
        object_id = st.text_input("Object ID", placeholder="object_id")

    # Input for payload (only for POST & PUT)
    extra_args = {}
    payload = None
    if operation in ["post-args", "put", "post"]:
        payload_input = st.text_area("Payload (JSON)", placeholder='{"key": "value"}')
        if operation == "post-args":
            extra_args['salesorder_id'] = st.text_input("Sales order id", placeholder='salesorder id')
        if payload_input:
            try:
                payload = json.loads(payload_input)  # Convert string to JSON
            except json.JSONDecodeError:
                st.error("Invalid JSON format. Please check the payload.")

    # Trigger button
    if st.button("Execute API Call"):
        try:
            # Fetch API details from session state
            api_domain = st.session_state['api_domain']
            access_token = st.session_state['access_token']
            organization_id = st.session_state['organization_id']

            # Call the respective function
            if operation == "fetch":
                endpoint1 = '/' + endpoint
                records = fetch_records_from_zakya(api_domain, access_token, organization_id, endpoint1)
                response = extract_record_list(records,endpoint)
            elif operation == "retrieve":
                endpoint1 = endpoint + "/" + object_id
                response = retrieve_record_from_zakya(api_domain, access_token, organization_id, endpoint1)
                # response = response.get(endpoint.rstrip("s"))
                # print(response)
            elif operation == "post":
                response = post_record_to_zakya(api_domain, access_token, organization_id, endpoint, payload)
                print(response)
            elif operation == "post-args":
                response = post_record_to_zakya(api_domain, access_token, organization_id, endpoint, payload, extra_args)
                print(response)

            elif operation == "put":
                endpoint1 = endpoint + "/" + object_id
                response = put_record_to_zakya(api_domain, access_token, organization_id, endpoint1)
            else:
                response = {"error": "Invalid operation selected"}

            show_preview = True
            show_preview = st.checkbox(f"Show/Hide {endpoint} DataFrame",value=True)
            if show_preview:                 
                response_df = pd.DataFrame.from_records(response)
                st.dataframe(response_df)
            
            show_preview_json = False
            show_preview_json = st.checkbox(f"Show/Hide {endpoint} JSON Response",value=True)
            if show_preview_json:
                st.json(response)

        except Exception as e:
            st.error(f"An error occurred: {str(e)}")

# Run the function
zakya_api_interaction()
