# linesheet_generator/transforms.py

import numpy as np
import pandas as pd

def apply_static_mappings(df, static_values):
    """
    For columns mapped as 'Static', set that column's value to the user-entered static value.
    """
    for col, val in static_values.items():
        df[col] = val
    return df


def apply_derived_columns(df, derived_configs):
    """
    Applies logic for columns mapped as 'Derived'.
    If 'type' == 'Arithmetic', we do a numeric operation.
    If 'type' == 'MultiBranch', we do multi-condition checks.
    """
    for config in derived_configs:
        col_name = config["derived_col"]
        derive_type = config["type"]

        if derive_type == "Arithmetic":
            logic = config["logic"]
            left_val = get_operand_value(df, logic["left_operand"], logic["left_operand_type"])
            right_val = get_operand_value(df, logic["right_operand"], logic["right_operand_type"])

            op = logic["operator"]

            if op == "+":
                df[col_name] = left_val + right_val
            elif op == "-":
                df[col_name] = left_val - right_val
            elif op == "*":
                df[col_name] = left_val * right_val
            elif op == "/":
                # Handle division by zero or zero-like issues
                if isinstance(right_val, pd.Series):
                    df[col_name] = left_val / right_val.replace(0, np.nan)
                else:
                    # If scalar, handle carefully
                    df[col_name] = left_val / (right_val if right_val != 0 else np.nan)

        elif derive_type == "MultiBranch":
            logic = config["logic"]
            conditions = logic["conditions"]
            default_output = logic.get("default_output", "N/A")

            # Evaluate row by row
            for idx, row in df.iterrows():
                assigned_value = None
                for cond in conditions:
                    if evaluate_condition(row, cond["column"], cond["operator"], cond["value"]):
                        assigned_value = cond["output"]
                        break  # Stop at the first match
                if assigned_value is None:
                    assigned_value = default_output

                df.at[idx, col_name] = assigned_value

    return df


def get_operand_value(df, operand, operand_type):
    """
    If operand_type == 'Column', return the corresponding Series from df, converted to float if possible.
    If operand_type == 'Static', convert it to float.
    """
    if operand_type == "Column":
        return df[operand].astype(float, errors="ignore")
    else:
        return float(operand)


def evaluate_condition(row, col_name, operator, val):
    """
    Evaluate a single condition: row[col_name] <operator> val
    Operators: "==", ">", "<", "contains"
    """
    col_val = row[col_name]

    if operator == "==":
        return str(col_val) == str(val)
    elif operator == ">":
        try:
            return float(col_val) > float(val)
        except:
            return False
    elif operator == "<":
        try:
            return float(col_val) < float(val)
        except:
            return False
    elif operator == "contains":
        return str(val) in str(col_val)
    else:
        return False


def map_sku_to_product_master(df, sku_column, column_mapping, product_master):
    """
    For columns mapped to an existing product_master column, fill from product_master
    using df[sku_column] as the lookup key for product_master['SKU'].
    """
    if "SKU" not in product_master.columns:
        return df  # Or raise an error, depending on your needs

    pm_lookup = product_master.set_index("SKU")

    for col, pm_col in column_mapping.items():
        if pm_col in pm_lookup.columns:
            df[col] = df[sku_column].map(pm_lookup[pm_col])

    return df
