# import streamlit as st
# import pandas as pd
# from utils.zakya_api import (get_authorization_url
#     ,fetch_object_for_each_id
#     ,fetch_records_from_zakya)

# from utils.postgres_connector import crud

# def extract_record_list(input_data,key):
#     records = []
#     for record in input_data:
#         records.extend(record[f'{key}'])
#     return records

# st.header("Invoices")
# invoices_data = fetch_records_from_zakya(
#         st.session_state['api_domain'],
#         st.session_state['access_token'],
#         st.session_state['organization_id'],
#         '/invoices'                  
# )
# invoices_record = extract_record_list(invoices_data,"invoices")
# invoices_data = pd.DataFrame.from_records(invoices_record)

# # for index, row in invoices_data.iterrows():
