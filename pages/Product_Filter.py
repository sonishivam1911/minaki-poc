import streamlit as st
import pandas as pd
from main import filter_existing_products, load_and_rename_master
from utils.auth_decorator import require_auth

@require_auth  # Enforce authentication on this page
def product_filter_function():

    st.title("Product Filtering")

    # Load product master data
    product_master_df = load_and_rename_master()

    # Filter options
    category_options = list(product_master_df["Category_Name"].dropna().unique())
    components_options = list(product_master_df["CF_Components"].dropna().unique())

    st.subheader("Filter Existing Products")
    selected_category = st.selectbox("Category Name", [""] + category_options)
    selected_components = st.selectbox("Components", [""] + components_options)

    # Filter products
    filtered_products = filter_existing_products(
        product_master_df,
        category_name=selected_category if selected_category else None,
        components=selected_components if selected_components else None,
    )

    st.subheader("Filtered Products")
    if not filtered_products.empty:
        st.dataframe(filtered_products)
    else:
        st.write("No products match the selected filters.")


product_filter_function()