# linesheet_generator/ui.py

import streamlit as st
import pandas as pd

def render_file_uploader():
    """
    Step 1: File Upload
    Returns a DataFrame if file is uploaded, else None.
    """
    st.header("Step 1: File Upload")
    uploaded_file = st.file_uploader("Upload your CSV/Excel file", type=["csv", "xlsx"])

    if uploaded_file:
        if uploaded_file.name.endswith(".csv"):
            return pd.read_csv(uploaded_file)
        else:
            return pd.read_excel(uploaded_file)

    return None


def render_column_mapping_step(df, product_master):
    """
    Step 2: Column Mapping
      - Select one SKU column
      - For each other column, choose None / Static / Derived / product_master_col
      - If Static, ask user for the static value
    """
    st.header("Step 2: Column Mapping")

    # SKU column
    sku_column = st.selectbox("Select the SKU column:", options=df.columns)
    st.write(f"SKU column selected: {sku_column}")

    # Potential mappings
    product_master_columns = ["None", "Static", "Derived"] + list(product_master.columns)

    column_mapping = {}
    static_values = {}

    for col in df.columns:
        if col == sku_column:
            column_mapping[col] = "SKU (Pre-Mapped)"
            st.write(f"{col} is the SKU column (pre-mapped).")
            continue

        st.write(f"**Column: {col}**")
        map_choice = st.selectbox(
            f"Map '{col}' to:",
            options=product_master_columns,
            index=0,  # Default: "None"
            key=f"mapping_{col}"
        )

        if map_choice == "Static":
            val = st.text_input(f"Enter static value for '{col}':", key=f"static_val_{col}")
            static_values[col] = val

        column_mapping[col] = map_choice

    return sku_column, column_mapping, static_values


def render_derived_columns_step(df, column_mapping):
    """
    Step 3: Define Derived Columns.
    For columns set as 'Derived', provide options for Arithmetic or MultiBranch logic.
    """

    st.header("Step 3: Define Derived Columns")

    derived_configs = []

    # Identify columns set to Derived
    derived_columns = [col for col, map_type in column_mapping.items() if map_type == "Derived"]

    if not derived_columns:
        st.write("No columns set to 'Derived'.")
        return derived_configs

    for derived_col in derived_columns:
        st.subheader(f"Logic for Derived Column: '{derived_col}'")
        deriv_type = st.radio(
            f"Select derive type for '{derived_col}':",
            options=["Arithmetic", "MultiBranch"],
            key=f"derive_type_{derived_col}"
        )

        if deriv_type == "Arithmetic":
            # Same Arithmetic Logic
            left_choice_type = st.radio(
                "Left Operand Type:",
                ["Column", "Static"],
                key=f"left_choice_{derived_col}"
            )
            if left_choice_type == "Column":
                left_operand = st.selectbox(
                    "Select left column",
                    df.columns,
                    key=f"left_operand_{derived_col}"
                )
            else:
                left_operand = st.text_input(
                    "Enter numeric value for left operand",
                    "0",
                    key=f"left_operand_val_{derived_col}"
                )

            operator = st.selectbox(
                "Operator:",
                ["+", "-", "*", "/"],
                key=f"op_{derived_col}"
            )

            right_choice_type = st.radio(
                "Right Operand Type:",
                ["Column", "Static"],
                key=f"right_choice_{derived_col}"
            )
            if right_choice_type == "Column":
                right_operand = st.selectbox(
                    "Select right column",
                    df.columns,
                    key=f"right_operand_{derived_col}"
                )
            else:
                right_operand = st.text_input(
                    "Enter numeric value for right operand",
                    "0",
                    key=f"right_operand_val_{derived_col}"
                )

            derived_configs.append({
                "derived_col": derived_col,
                "type": "Arithmetic",
                "logic": {
                    "left_operand_type": left_choice_type,
                    "left_operand": left_operand,
                    "operator": operator,
                    "right_operand_type": right_choice_type,
                    "right_operand": right_operand
                }
            })

        else:  # MultiBranch
            st.markdown("Define multiple (condition → output) pairs. Click '+' to add a condition.")

            # Initialize session state for this derived column
            if f"multi_branch_conditions_{derived_col}" not in st.session_state:
                st.session_state[f"multi_branch_conditions_{derived_col}"] = []

            # Button to add a new condition
            if st.button(f"Add Condition for '{derived_col}'"):
                st.session_state[f"multi_branch_conditions_{derived_col}"].append({
                    "column": "",
                    "operator": "==",
                    "value": "",
                    "output": ""
                })

            # Display and edit conditions
            conditions = st.session_state[f"multi_branch_conditions_{derived_col}"]
            for i, condition in enumerate(conditions):
                st.write(f"**Condition #{i+1}**")
                condition["column"] = st.selectbox(
                    "Column:",
                    df.columns,
                    key=f"mb_col_{derived_col}_{i}"
                )
                condition["operator"] = st.selectbox(
                    "Operator:",
                    ["==", ">", "<", "contains"],
                    key=f"mb_op_{derived_col}_{i}"
                )
                condition["value"] = st.text_input(
                    "Value to compare:",
                    key=f"mb_val_{derived_col}_{i}"
                )
                condition["output"] = st.text_input(
                    "Output if this condition is satisfied:",
                    key=f"mb_out_{derived_col}_{i}"
                )

            # Default output if no conditions match
            default_output = st.text_input(
                "Default output if no conditions match:",
                "N/A",
                key=f"mb_default_{derived_col}"
            )

            # Save MultiBranch config
            if st.button(f"Save Multi-Branch Logic for '{derived_col}'"):
                derived_configs.append({
                    "derived_col": derived_col,
                    "type": "MultiBranch",
                    "logic": {
                        "conditions": conditions,
                        "default_output": default_output
                    }
                })
                st.success(f"Multi-branch logic for '{derived_col}' saved.")

    return derived_configs


def render_process_and_download_button():
    """
    Step 4: Process and Download
    Returns True if button is clicked, else False.
    """
    return st.button("Process and Download")
