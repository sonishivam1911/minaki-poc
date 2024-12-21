import streamlit as st
import io
from main import (
    process_csv, 
    load_and_rename_master, 
    filter_existing_products, 
    generate_csv_template, 
    create_sku,
    map_existing_products,
    aggregated_df,
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
uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])

if uploaded_file is not None:
    try:
        df = process_csv(uploaded_file,dollar_exchange_rate)
        st.success("CSV processed successfully!")
        st.write("Processed Data:")
        st.dataframe(df)        

        # Separate rows based on IsProductPresent column
        new_sku_df = df[df["IsProductPresent"] == 'N']

        if not new_sku_df.empty:
            st.subheader("New SKUs to be Created")
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

        st.success("Aggregated DataFrame created successfully!")
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