import streamlit as st
from main import process_csv

# Streamlit app title
st.title("CSV File Viewer")

# File uploader for CSV files
uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])

if uploaded_file:
    try:
        # Process the uploaded file using the function
        display_df = process_csv(uploaded_file)
        
        # Display the filtered data
        st.write("Filtered Data:")
        st.dataframe(display_df)
        
    except Exception as e:
        st.error(f"Error processing the file: {e}")
