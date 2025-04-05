import streamlit as st
import tempfile
import os
from io import BytesIO
from server.file_management.item import upload_product_photos, get_or_create_folder, extract_zip_and_upload

def main():
    st.title("Product Photo Upload to Google Drive")
    
    # Select inputs for organization
    brand = st.radio("Select Brand", ["MINAKI", "MINAKI Menz", "MINAKI Womanz"])
    category = st.selectbox("Select Category", [
        "Jewellery Set", "Necklace", "Earrings",  "Maang Teeka", "Matha Patti",
        "Bracelets", "Kadas", "Rings", "Passa", "Haath Phool"
    ])
    sku = st.text_input("Enter SKU")
    photo_type = st.radio("Select Photo Type", ["inventory_photo", "listing_photo"])
    
    # File uploader
    uploaded_files = st.file_uploader("Upload Product Photos", type=['jpg', 'jpeg', 'png', 'webp'], accept_multiple_files=True)
    uploaded_zip = st.file_uploader("Upload ZIP File", type=['zip'])
    
    if st.button("Upload Files"):
        if not sku:
            st.error("SKU is required!")
            return
        
        if uploaded_files:
            temp_files = []
            for uploaded_file in uploaded_files:
                with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(uploaded_file.name)[-1]) as temp_file:
                    temp_file.write(uploaded_file.getbuffer())
                    temp_files.append(temp_file.name)
            
            result = upload_product_photos(temp_files, brand, category, sku, photo_type)
            
            for file in temp_files:
                os.remove(file)
            
            st.success("Photos uploaded successfully!")
            st.write("[View Uploaded Folder](%s)" % result["folder_link"])
            for link in result["photo_links"]:
                st.write(f"[Photo Link]({link})")
        
        elif uploaded_zip:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as temp_zip:
                temp_zip.write(uploaded_zip.getbuffer())
                zip_path = temp_zip.name
            
            result = extract_zip_and_upload(zip_path, brand, category, sku, photo_type)
            os.remove(zip_path)
            
            st.success("ZIP file extracted and uploaded successfully!")
            st.write("[View Uploaded Folder](%s)" % result["folder_link"])
            for link in result["photo_links"]:
                st.write(f"[Photo Link]({link})")
        
        else:
            st.error("Please upload at least one file.")

if __name__ == "__main__":
    main()
