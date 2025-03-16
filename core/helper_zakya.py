import streamlit as st
from config.logger import logger
import pandas as pd
from utils.zakya_api import (fetch_records_from_zakya)

from utils.postgres_connector import crud

def extract_record_list(input_data,key):
    records = []
    for record in input_data:
        records.extend(record[f'{key}'])
    return records

def fetch_records_from_zakya_in_df_format(endpoint):
    object_data = fetch_records_from_zakya(
        st.session_state['api_domain'],
        st.session_state['access_token'],
        st.session_state['organization_id'],
        f'/{endpoint}'                  
)
    object_data = extract_record_list(object_data,f"{endpoint}")
    object_data = pd.DataFrame.from_records(object_data)
    return object_data
