import streamlit as st
import pandas as pd
import shutil
import os
from main import process_images

st.title("Process CSV and Add Images")

# Step 1: Upload CSV
uploaded_csv = st.file_uploader("Upload CSV with SKU, Price, Quantity, and Category", type=["csv"])
if uploaded_csv:
    # Parse the CSV
    df = pd.read_csv(uploaded_csv)
    st.dataframe(df)

    # Step 2: Process each SKU
    for index, row in df.iterrows():
        sku = row["SKU"]
        st.write(f"SKU: {sku}")

        # Step 3: Upload images for each SKU
        uploaded_images = st.file_uploader(
            f"Upload images for SKU: {sku}",
            type=["jpg", "png"],
            accept_multiple_files=True,
            key=f"upload_{sku}"
        )

        if uploaded_images:
            # Save uploaded images temporarily
            temp_dir = "uploaded_images"
            os.makedirs(temp_dir, exist_ok=True)
            image_paths = []

            for uploaded_image in uploaded_images:
                temp_path = os.path.join(temp_dir, uploaded_image.name)
                with open(temp_path, "wb") as f:
                    f.write(uploaded_image.getbuffer())
                image_paths.append(temp_path)

            # Step 4: Process images
            output_dir = "processed_images"
            process_images(image_paths, output_dir, sku)
            st.success(f"Images processed and saved for SKU: {sku}")

    # Step 5: Download all processed images
    if st.button("Process File"):
        # Create ZIP archive
        zip_filename = "processed_images.zip"
        shutil.make_archive("processed_images", "zip", "processed_images")
        with open(zip_filename, "rb") as zip_file:
            st.download_button(
                label="Download Processed Images",
                data=zip_file,
                file_name=zip_filename,
                mime="application/zip"
            )

        # Cleanup: Delete all uploaded and processed images
        try:
            if os.path.exists("uploaded_images"):
                shutil.rmtree("uploaded_images", ignore_errors=True)
                st.success("All uploaded_images files have been deleted.")
            if os.path.exists("processed_images"):
                shutil.rmtree("processed_images", ignore_errors=True)
            if os.path.exists("processed_images.zip"):
                os.remove("processed_images.zip")
            # st.success("All uploaded and processed files have been deleted.")
        except Exception as e:
            st.error(f"Error while cleaning up files: {e}")