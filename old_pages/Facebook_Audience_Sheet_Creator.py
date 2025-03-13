import streamlit as st
import pandas as pd
from main import preprocess_contacts, convert_df_to_csv, preprocess_remarketing_audience, merge_audiences

st.title("Facebook Audience Creator")
    
st.write("Upload your Contacts CSV file for processing:")

# Upload Contacts CSV file
contacts_file = st.file_uploader("Upload Contacts CSV", type=["csv"])

st.write("Optionally, upload an existing Facebook Remarketing Audience CSV:")

# Upload Remarketing Audience CSV file
remarketing_file = st.file_uploader("Upload Remarketing Audience CSV", type=["csv"])

if contacts_file is not None:
    # Process Contacts CSV
    contacts_df = pd.read_csv(contacts_file)
    processed_contacts = preprocess_contacts(contacts_df)
    
    if remarketing_file is not None:
        # Process Remarketing Audience CSV
        remarketing_df = pd.read_csv(remarketing_file)
        processed_remarketing = preprocess_remarketing_audience(remarketing_df)
        
        # Merge both datasets
        final_audience = merge_audiences(processed_contacts, processed_remarketing)
        
        st.write("Merged Audience:")
        st.dataframe(final_audience)
        
        # Provide download option for merged dataset
        csv_data = convert_df_to_csv(final_audience)
        st.download_button(
            label="Download Merged Audience as CSV",
            data=csv_data,
            file_name="merged_facebook_audience.csv",
            mime="text/csv"
        )
    else:
        st.write("Processed Contacts:")
        st.dataframe(processed_contacts)

        # Provide download option for processed contacts
        csv_data = convert_df_to_csv(processed_contacts)
        st.download_button(
            label="Download Processed Contacts as CSV",
            data=csv_data,
            file_name="processed_contacts.csv",
            mime="text/csv"
        )

