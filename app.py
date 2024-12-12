import streamlit as st
from main import process_csv, load_and_rename_master, filter_existing_products

# Streamlit app title
st.title("Minaki Inventory Creation")

# File uploader for CSV files
uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])

if uploaded_file is not None:
    try:
        df = process_csv(uploaded_file)
        st.success("CSV processed successfully!")
        st.write("Processed Data:")
        st.dataframe(df)
        
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