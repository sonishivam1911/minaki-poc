# linesheet_generator/app.py

import streamlit as st
import pandas as pd

# UI components
from Linsheet_Generator.ui import (
    render_file_uploader,
    render_column_mapping_step,
    render_derived_columns_step,
    render_process_and_download_button
)

# Transform functions
from Linsheet_Generator.transforms import (
    apply_static_mappings,
    apply_derived_columns,
    map_sku_to_product_master
)

# Helpers
from main import load_and_rename_master

def linesheet_generator_app():
    """
    Main Streamlit function orchestrating the linesheet generation steps.
    """

    st.title("Dynamic Column Mapping and Derived Logic Handling")

    # Step 1: File Upload
    uploaded_df = render_file_uploader()
    if uploaded_df is None:
        # No file uploaded, stop execution here
        return

    st.write("Uploaded File Preview:")
    st.dataframe(uploaded_df.head())

    # Load the Product Master (custom to your business)
    product_master = load_and_rename_master()

    # Step 2: Column Mapping
    sku_column, column_mapping, static_values = render_column_mapping_step(uploaded_df, product_master)

    # Step 3: Derived Columns
    #   Now includes either Arithmetic or MultiBranch logic
    derived_configs = render_derived_columns_step(uploaded_df, column_mapping)

    # Step 4: Process & Download
    if render_process_and_download_button():
        processed_df = process_data(
            uploaded_df,
            sku_column,
            column_mapping,
            static_values,
            derived_configs,
            product_master
        )

        # Download processed DataFrame
        csv_data = processed_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download Processed File",
            data=csv_data,
            file_name="processed_file.csv",
            mime="text/csv"
        )


def process_data(
    df,
    sku_column,
    column_mapping,
    static_values,
    derived_configs,
    product_master
):
    """
    Applies all transformations to a copy of the DataFrame, in order:
      1. Static mappings
      2. Derived columns (Arithmetic or MultiBranch)
      3. Map columns from Product Master (using SKU)
    """
    processed_df = df.copy()

    # 1. Static values
    processed_df = apply_static_mappings(processed_df, static_values)

    # 2. Derived columns
    processed_df = apply_derived_columns(processed_df, derived_configs)

    # 3. Map columns from product master based on SKU
    processed_df = map_sku_to_product_master(processed_df, sku_column, column_mapping, product_master)

    return processed_df


linesheet_generator_app()