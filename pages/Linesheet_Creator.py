import streamlit as st
import pandas as pd
import numpy as np
from main import load_and_rename_master


def validate_equation(equation, column_types):
    """Validate if all variables in equation are valid and their types are compatible."""
    try:
        variables = parse_variables(equation)
        for var in variables:
            if var not in column_types:
                return False, f"Variable '{var}' is not mapped."
            if column_types[var] not in ["int", "float"]:
                return False, f"Variable '{var}' is of type '{column_types[var]}'. Numeric type required."
        return True, None
    except Exception as e:
        return False, str(e)

def parse_variables(equation):
    """Extract unique variables from an equation string."""
    tokens = equation.replace(" ", "").split("+-*/()")
    return [token for token in set(tokens) if token.isalpha()]

# Streamlit App
st.title("Dynamic Column Mapping and Derived Logic Handling")

# Load Product Master
product_master = load_and_rename_master()
product_master_columns = ["None", "Static", "Mathematically Derived", "Conditionally Derived"] + list(product_master.columns)

# Step 1: File Upload
st.header("Step 1: File Upload")
uploaded_file = st.file_uploader("Upload your CSV/Excel file", type=["csv", "xlsx"])
if uploaded_file:
    if uploaded_file.name.endswith(".csv"):
        uploaded_df = pd.read_csv(uploaded_file)
    else:
        uploaded_df = pd.read_excel(uploaded_file)
    
    st.write("Uploaded File Preview:")
    st.dataframe(uploaded_df.head())

    # Identify SKU Column
    sku_column = st.selectbox("Select the SKU column:", options=uploaded_df.columns, key="sku_column")
    st.write(f"SKU column selected: {sku_column}")

    # Step 2: Column Mapping
    st.header("Step 2: Column Mapping")
    column_mapping = {}
    static_values = {}
    mathematically_derived = []
    conditionally_derived = []

    for col in uploaded_df.columns:
        if col == sku_column:
            # Pre-map the SKU column and skip further selection
            column_mapping[col] = "SKU (Pre-Mapped)"
            st.write(f"{col} (SKU column - pre-mapped)")
            continue

        st.write(f"Column: {col}")  # Display the column label
        mapping_type = st.selectbox(
            f"Map '{col}' to:",
            options=product_master_columns,
            key=f"mapping_{col}",
            index=0  # Default to "None"
        )
        
        if mapping_type == "Static":
            static_value = st.text_input(f"Enter static value for column '{col}':", key=f"static_value_{col}")
            static_values[col] = static_value
        
        elif mapping_type == "Mathematically Derived":
            st.subheader(f"Mathematically Derived Column: {col}")
            equation = st.text_area(f"Define equation for '{col}' (e.g., a * 0.75 + b):", key=f"math_eq_{col}")
            variables = parse_variables(equation)
            variable_mapping = {}
            for var in variables:
                variable_mapping[var] = st.selectbox(
                    f"Map variable '{var}' to:",
                    options=list(uploaded_df.columns),
                    key=f"math_var_map_{col}_{var}"
                )
            mathematically_derived.append({
                "column_name": col,
                "equation": equation,
                "variable_mapping": variable_mapping
            })
        
        elif mapping_type == "Conditionally Derived":
            st.subheader(f"Conditionally Derived Column: {col}")
            condition_column = st.selectbox(f"Select column for condition:", options=uploaded_df.columns, key=f"cond_col_{col}")
            condition_operator = st.selectbox(f"Select operator:", options=["is equal to", "is greater than", "is less than"], key=f"cond_op_{col}")
            condition_value = st.text_input(f"Enter value for condition:", key=f"cond_value_{col}")
            true_value = st.text_input(f"Value if condition is true:", key=f"true_value_{col}")
            false_value = st.text_input(f"Value if condition is false:", key=f"false_value_{col}")
            conditionally_derived.append({
                "column_name": col,
                "condition": {
                    "column": condition_column,
                    "operator": condition_operator,
                    "value": condition_value
                },
                "true_value": true_value,
                "false_value": false_value
            })
        
        column_mapping[col] = mapping_type

    # Step 3: Process File
    st.header("Step 3: Process File")
    if st.button("Process and Download"):
        processed_df = uploaded_df.copy()
        
        # Apply static values
        for col, value in static_values.items():
            processed_df[col] = value

        # Apply mathematically derived columns
        for derived in mathematically_derived:
            equation = derived["equation"]
            for var, mapped_col in derived["variable_mapping"].items():
                equation = equation.replace(var, f"processed_df['{mapped_col}']")
            processed_df[derived["column_name"]] = eval(equation)
        
        # Apply conditionally derived columns
        for derived in conditionally_derived:
            condition = derived["condition"]
            column = condition["column"]
            operator = condition["operator"]
            value = condition["value"]
            true_val = derived["true_value"]
            false_val = derived["false_value"]

            if operator == "is equal to":
                processed_df[derived["column_name"]] = np.where(processed_df[column] == value, true_val, false_val)
            elif operator == "is greater than":
                processed_df[derived["column_name"]] = np.where(processed_df[column].astype(float) > float(value), true_val, false_val)
            elif operator == "is less than":
                processed_df[derived["column_name"]] = np.where(processed_df[column].astype(float) < float(value), true_val, false_val)
        
        # Map SKU to Product Master
        for col, mapping in column_mapping.items():
            if mapping in product_master.columns:
                processed_df[col] = processed_df[sku_column].map(
                    product_master.set_index("SKU")[mapping]
                )
        
        # Download Processed File
        csv = processed_df.to_csv(index=False).encode("utf-8")
        st.download_button("Download Processed File", data=csv, file_name="processed_file.csv", mime="text/csv")
