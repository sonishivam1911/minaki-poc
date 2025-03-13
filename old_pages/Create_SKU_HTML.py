import streamlit as st
import pandas as pd
import asyncio
import os
import shutil
from zipfile import ZipFile
from core.product_parser import extract_product_links  # Product parsing logic
from core.sku_generator import generate_skus  # SKU generation logic
from core.image_processing import process_batch  # Image downloading and processing logic
from main import process_images, load_and_rename_master  # Image processing function from main.py

# Initialize session state for tracking batches and SKUs
if "processed_batches" not in st.session_state:
    st.session_state.processed_batches = []
if "failed_skus" not in st.session_state:
    st.session_state.failed_skus = []
if "current_batch_index" not in st.session_state:
    st.session_state.current_batch_index = 0

# Streamlit App Title
st.title("Order Product Scraper with Batched Image Processing")

# Step 1: Upload Order HTML File
order_file = st.file_uploader("Upload Order HTML File", type=["html"])
if order_file is not None:
    order_html_content = order_file.read()
    
    # Extract products data into a DataFrame
    products_df = extract_product_links(order_html_content)

    # Load existing SKUs from the product master (if required)
    existing_skus = set(load_and_rename_master()['SKU'].dropna().astype(str).tolist())
    
    # Generate SKUs for products
    products_df_with_skus = generate_skus(products_df, existing_skus)
    
    # Store the DataFrame in session state for persistence across re-runs
    st.session_state.products_df_with_skus = products_df_with_skus

    # Display extracted data with SKUs as a DataFrame
    st.write("Extracted Product Data with Generated SKUs:")
    st.dataframe(products_df_with_skus)

# Step 2: Download Images in Batches of 10 SKUs at a Time
if "products_df_with_skus" in st.session_state:
    products_df_with_skus = st.session_state.products_df_with_skus

    if st.button("Download and Process Images"):
        skus_per_batch = 10  # Number of SKUs to process per batch

        # Create an output folder to store batch ZIP files
        output_folder = "output"
        os.makedirs(output_folder, exist_ok=True)

        for i in range(st.session_state.current_batch_index, len(products_df_with_skus), skus_per_batch):
            current_batch = products_df_with_skus.iloc[i:i + skus_per_batch]
            
            # Process current batch asynchronously (download images)
            failed_skus = asyncio.run(process_batch(current_batch))
            st.session_state.failed_skus.extend(failed_skus)  # Track failed SKUs
            
            st.success(f"Batch {i // skus_per_batch + 1} downloaded!")

            # Step 3: Process Images After Downloading (Pass SKU)
            base_output_dir = "processed_images"
            os.makedirs(base_output_dir, exist_ok=True)
            
            for _, row in current_batch.iterrows():
                sku = row['Generated SKU']
                vendor_folder = os.path.join("vendor_images", sku)
                processed_folder = os.path.join(base_output_dir, sku)
                
                try:
                    process_images(
                        [os.path.join(vendor_folder, f) for f in os.listdir(vendor_folder)],
                        processed_folder,
                        sku=sku,
                    )
                except Exception as e:
                    st.error(f"Failed to process images for SKU: {sku}. Error: {e}")
                    st.session_state.failed_skus.append(sku)

            st.success(f"Batch {i // skus_per_batch + 1} processed!")

            # Step 4: Zip the processed images for this batch
            zip_name = f"batch_{i // skus_per_batch + 1}.zip"
            zip_path = os.path.join(output_folder, zip_name)
            shutil.make_archive(zip_path.replace(".zip", ""), 'zip', base_output_dir)

            st.success(f"Batch {i // skus_per_batch + 1} zipped!")

            # Cleanup temporary folders after zipping
            shutil.rmtree("vendor_images", ignore_errors=True)
            shutil.rmtree("processed_images", ignore_errors=True)

        # Allow user to download all batch ZIPs as a single ZIP file
        final_zip_name = "final_output.zip"
        shutil.make_archive(final_zip_name.replace(".zip", ""), 'zip', output_folder)

        with open(final_zip_name, "rb") as zip_file:
            st.download_button(
                label="Download All Processed Batches",
                data=zip_file,
                file_name=final_zip_name,
                mime="application/zip",
            )

        # Cleanup after final download
        shutil.rmtree(output_folder, ignore_errors=True)
        os.remove(final_zip_name)

        # Display failed SKUs, if any
        if st.session_state.failed_skus:
            st.warning(f"The following SKUs failed during processing: {', '.join(st.session_state.failed_skus)}")
