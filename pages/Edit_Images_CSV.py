import streamlit as st
import pandas as pd
import shutil
from PIL import Image
import os
from main import process_images

# Function to filter unwanted images
def filter_images(directory):
    """
    Filters unwanted images based on filename prefixes and dimensions.

    Args:
        directory (str): Directory containing images.

    Returns:
        list: List of valid image file paths.
    """
    valid_images = []
    for root, _, files in os.walk(directory):
        for file in files:
            # Skip unwanted filenames
            if any(file.startswith(prefix) for prefix in ["image", "normal", "rect_mask", "goods", 
                                                           "gotop", "ico-img", "star", "9b49b21fc4"]):
                continue
            elif 'x' in files or '500' in files or '240' in files:
                continue

            file_path = os.path.join(root, file)
            try:
                with Image.open(file_path) as img:
                    width, height = img.size
                    # Skip unwanted dimensions
                    print(f"{file_path} dimension is {width} and {height}")
                    if (width == 240 and height == 240) or (width == 40) or (width == 500) or ((width == 100 and height == 100)):
                        continue
                    valid_images.append(file_path)
            except Exception as e:
                st.error(f"Error reading image {file}: {e}")
    print(f"Valid images : {valid_images}")
    return valid_images


# Streamlit app starts here
st.title("Process ZIP File and Add Images")

# Step 1: Upload CSV
uploaded_csv = st.file_uploader("Upload CSV with SKU, Price, Quantity, and Category", type=["csv"])
if uploaded_csv:
    # Parse the CSV
    df = pd.read_csv(uploaded_csv)
    st.dataframe(df)

    # Step 2: Upload ZIP file containing images for each SKU
    output_dir = "processed_images"
    os.makedirs(output_dir, exist_ok=True)


    for index, row in df.iterrows():
        sku = row["SKU"]
        st.write(f"Upload ZIP file containing images for SKU: {sku}")
        
        uploaded_zip = st.file_uploader(f"Upload a ZIP file for SKU: {sku}", type=["zip"], key=f"zip_{sku}")
        if uploaded_zip:
            # Save the uploaded ZIP file temporarily
            temp_zip_path = f"{sku}_uploaded_images.zip"
            temp_dir = f"{sku}_uploaded_images"
            try:
                with open(temp_zip_path, "wb") as f:
                    f.write(uploaded_zip.getbuffer())

                # Extract the ZIP file into a unique directory per SKU
                os.makedirs(temp_dir, exist_ok=True)
                shutil.unpack_archive(temp_zip_path, temp_dir)

                st.success(f"ZIP file extracted successfully for SKU: {sku}")

                # Step 3: Filter unwanted images
                filtered_images = filter_images(temp_dir)
                if filtered_images:
                    st.success(f"Filtered {len(filtered_images)} valid images for SKU: {sku}.")
                else:
                    st.warning(f"No valid images found after filtering for SKU: {sku}.")

                # Step 4: Process filtered images
                if filtered_images:
                    process_images(filtered_images, output_dir, sku)
                    st.success(f"Images processed and renamed successfully for SKU: {sku}.")
            
            except Exception as e:
                st.error(f"Error processing ZIP file for SKU {sku}: {e}")

            finally:
                # Cleanup temporary files after processing this SKU
                try:
                    if os.path.exists(temp_dir):
                        shutil.rmtree(temp_dir)
                    if os.path.exists(temp_zip_path):
                        os.remove(temp_zip_path)
                except Exception as cleanup_error:
                    st.error(f"Error during cleanup for SKU {sku}: {cleanup_error}")
    # Step 5: Download all processed images as a ZIP archive
    if st.button("Download All Processed Images"):
        zip_filename = "processed_images.zip"
        shutil.make_archive("processed_images", "zip", "processed_images")
        with open(zip_filename, "rb") as zip_file:
            st.download_button(
                label="Download Processed Images",
                data=zip_file,
                file_name=zip_filename,
                mime="application/zip"
            )

        # Cleanup all processed files after download
        try:
            if os.path.exists("processed_images"):
                shutil.rmtree("processed_images")
            if os.path.exists(zip_filename):
                os.remove(zip_filename)
        except Exception as e:
            st.error(f"Error during final cleanup: {e}")
