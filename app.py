import streamlit as st
import io
import pandas as pd
import os
import shutil
from main import (
    process_csv, 
    load_and_rename_master, 
    filter_existing_products, 
    generate_csv_template, 
    create_sku,
    map_existing_products,
    aggregated_df,
    process_images
)

# Streamlit app title
st.title("Minaki Inventory Creation")

# Step 1: Template Download (from previous code)
st.subheader("Download CSV Template")
template_buffer = generate_csv_template()
st.download_button(
    label="Download CSV Template",
    data=template_buffer,
    file_name="template.csv",
    mime="text/csv"
)

# Add text box for exchange rate of dollar near file upload
st.subheader("Dollar Exchange Rate")
dollar_exchange_rate = st.number_input(
    "Enter the exchange rate for 1 USD:",
    min_value=0.0,
    step=0.01,
    format="%.2f"
)

st.write(f"The entered exchange rate is: {dollar_exchange_rate}")

# File uploader for CSV files
st.subheader("Creating Purchase Order CSV and New SKU CSV")
uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])

if uploaded_file is not None:
    try:
        df = process_csv(uploaded_file,dollar_exchange_rate)     

        # Separate rows based on IsProductPresent column
        new_sku_df = df[df["IsProductPresent"] == 'N']

        if not new_sku_df.empty:
            st.subheader("New SKUs Created: Download this CSV and Upload on Zakya")
            st.write("The following SKUs need to be created:")
            

            # Generate the CSV for download
            csv_data = create_sku(new_sku_df)
            st.dataframe(csv_data)
            buffer = io.BytesIO()
            csv_data.to_csv(buffer, index=False)
            buffer.seek(0)

            # Add download button
            st.download_button(
                label="Download New SKUs CSV",
                data=buffer,
                file_name="new_skus.csv",
                mime="text/csv"
            )

        else:
            st.info("No new SKUs to be created.")

        # Map existing products and attach product master
        existing_sku_df = map_existing_products(df)

        # print(f"existing dataframe is {existing_sku_df}")

        # Generate the aggregated DataFrame
        purchase_order_df = aggregated_df(new_sku_df,existing_sku_df)

        st.success("Purchase Order Table - Download Purchase Order & Upload on Zakya to update inventory")
        st.write(purchase_order_df)

        # Add download button
        csv_data = purchase_order_df.to_csv(index=False)
        st.download_button(
            label="Download Aggregated CSV",
            data=csv_data,
            file_name="purchase_order.csv",
            mime="text/csv"
        )        
        
        # Store the DF in session state for later use
        st.session_state["df"] = df

    except Exception as e:
        st.error(f"Error processing the file: {e}")


# Set up Streamlit app
st.title("SKU Image Watermark Removal")

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










# Load Product Master at startup (no user upload)
product_master_df = load_and_rename_master()

st.title("Existing Product Finder")
st.write("You can always filter the product master to find existing products.")

# Extract filter options
category_options = list(product_master_df["Category_Name"].dropna().unique())
components_options = list(product_master_df["CF_Components"].dropna().unique())
work_options = list(product_master_df["CF_Work"].dropna().unique())
finish_options = list(product_master_df["CF_Finish"].dropna().unique())
finding_options = list(product_master_df["CF_Finding"].dropna().unique())

st.subheader("Filter Existing Products")
selected_category = st.selectbox("Category Name (optional)", [""] + category_options)
selected_components = st.selectbox("Components (optional)", [""] + components_options)
selected_work = st.selectbox("Work (optional)", [""] + work_options)
selected_finish = st.selectbox("Finish (optional)", [""] + finish_options)
selected_finding = st.selectbox("Finding (optional)", [""] + finding_options)

filtered_products = filter_existing_products(
    product_master_df,
    category_name=selected_category if selected_category else None,
    components=selected_components if selected_components else None,
    work=selected_work if selected_work else None,
    finish=selected_finish if selected_finish else None,
    finding=selected_finding if selected_finding else None
)

st.subheader("Filtered Products")
if filtered_products.empty:
    st.write("No products match the selected filters.")
else:
    columns_to_show = ["SKU", "Item_Name", "Brand", "Selling_Price", "Stock_On_Hand", 
                       "Category_Name", "CF_Components", "CF_Work", "CF_Finish", "CF_Finding"]
    available_columns = [col for col in columns_to_show if col in filtered_products.columns]
    st.dataframe(filtered_products[available_columns])