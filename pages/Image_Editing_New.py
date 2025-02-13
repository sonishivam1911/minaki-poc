import streamlit as st
import pandas as pd
import shutil
import os
from zipfile import ZipFile
from main import process_images


# Function to upload a ZIP file
def upload_zip_file():
    st.title("Upload and Process ZIP File")
    uploaded_file = st.file_uploader("Upload a ZIP file containing .xls and .jpg files", type=["zip"])
    if uploaded_file:
        temp_zip_path = "temp_uploaded.zip"
        with open(temp_zip_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        st.success("ZIP file uploaded successfully.")
        return temp_zip_path
    return None


# Function to extract ZIP file contents
def extract_zip_file(zip_path, extract_dir):
    os.makedirs(extract_dir, exist_ok=True)
    with ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
    all_files = []
    for root, _, files in os.walk(extract_dir):
        for file in files:
            all_files.append(os.path.join(root, file))
    # print(f"Extracted fils : {all_files}")
    return all_files


# Function to read and validate the Excel file
def read_excel_file(extracted_files):
    for file in extracted_files:
        if file.endswith(".xls") or file.endswith(".xlsx"):
            try:
                df = pd.read_excel(file)
                print(f"file is {df.columns}")
                print(df)
                if "Item number" in df.columns:
                    st.success("Excel file parsed successfully.")
                    return df
                else:
                    st.error("Excel file must contain an 'item_number' column.")
                    return None
            except Exception as e:
                st.error(f"Error reading Excel file: {e}")
    st.error("No valid Excel file found in the ZIP.")
    return None


# Function to display Excel data
def display_excel_data(df):
    st.subheader("Excel Data")
    st.dataframe(df)


# Function to organize images by item number
def organize_images_by_item_number(df, extracted_files, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    for _, row in df.iterrows():
        item_number = row["Item number"]
        item_folder = os.path.join(output_dir, str(item_number))
        os.makedirs(item_folder, exist_ok=True)

        for file in extracted_files:
            print(f"file is {file} and item folder is {item_folder}")
            file_name = file.split("/")[-1]
            if file.endswith(".jpg") and file_name.startswith(f"{item_number}_"):
                shutil.copy(file, item_folder)
    st.success("Images organized successfully.")


# Function to create ZIP of processed images
def create_zip_of_processed_files(output_dir, zip_path):
    shutil.make_archive(zip_path.replace(".zip", ""), "zip", output_dir)
    return zip_path


# Function to download processed ZIP
def download_processed_zip(zip_path):
    with open(zip_path, "rb") as f:
        st.download_button(
            label="Download Processed ZIP",
            data=f,
            file_name=os.path.basename(zip_path),
            mime="application/zip"
        )


# Function to clean up temporary files
def cleanup_temp_files(temp_dirs, temp_files):
    for dir_path in temp_dirs:
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
    for file_path in temp_files:
        if os.path.exists(file_path):
            os.remove(file_path)
    st.success("Temporary files cleaned up successfully.")


# Function to validate ZIP file contents
def validate_uploaded_files(extracted_files):
    has_excel = True if(file.endswith(".xls") or file.endswith(".xlsx") for file in extracted_files) else False
    has_images = True if (file.endswith(".jpg") for file in extracted_files) else False

    if not has_excel:
        st.error("ZIP file must contain at least one .xls file.")
        return False
    if not has_images:
        st.error("ZIP file must contain at least one .jpg file.")
        return False
    st.success("ZIP file contents validated.")
    return True


# Function to process images for each SKU
def process_images_for_skus(df, output_dir):
    for index, row in df.iterrows():
        sku = row["Item number"]
        st.write(f"Processing images for SKU: {sku}")
        
        # Specify the folder for the current SKU
        sku_folder = os.path.join(output_dir, str(sku))
        print(f"SKU folders are {sku_folder}")
        
        if not os.path.exists(sku_folder):
            st.warning(f"No images found for SKU: {sku}")
            continue
        
        # Process the images in the SKU folder
        try:
            images = [os.path.join(sku_folder, file) for file in os.listdir(sku_folder) if file.endswith(".jpg")]
            print(f"Images are {images}")
            if images:
                process_images(images, output_dir, sku)
                st.success(f"Images processed successfully for SKU: {sku}.")
            else:
                st.warning(f"No valid images found for SKU: {sku}.")
        except Exception as e:
            st.error(f"Error processing images for SKU: {sku}: {e}")

# Main orchestration function
def process_zip_and_display():
    zip_path = upload_zip_file()
    if zip_path:
        extract_dir = "temp_extracted"
        output_dir = "processed_images"
        extracted_files = extract_zip_file(zip_path, extract_dir)

        if validate_uploaded_files(extracted_files):
            df = read_excel_file(extracted_files)
            if df is not None:
                display_excel_data(df)
                organize_images_by_item_number(df, extracted_files, output_dir)
                process_images_for_skus(df, output_dir)

                # Create ZIP for download
                zip_path = create_zip_of_processed_files(output_dir, "processed_images.zip")
                download_processed_zip(zip_path)

                # # Cleanup temporary files
                cleanup_temp_files([extract_dir, output_dir], [zip_path])


# Run the Streamlit app
if __name__ == "__main__":
    process_zip_and_display()
