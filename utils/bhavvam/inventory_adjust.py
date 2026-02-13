import streamlit as st
import datetime
import pandas as pd
from config.logger import logger
from utils.zakya_api import extract_record_list, post_record_to_zakya, fetch_records_from_zakya

def create_inventory_adjustment_via_csv(df, api_domain, access_token, organization_id):
    inventory_df1 = fetch_records_from_zakya(api_domain, access_token, organization_id)
    inventory_df2 = extract_record_list(inventory_df1, "items")
    inventory_df = pd.DataFrame(inventory_df2)
    inventory_df = inventory_df[["item_id", "sku", "actual_available_stock"]]

    # Merge input df with inventory on SKU
    merged_df = pd.merge(df, inventory_df, on="sku", how="left")
    merged_df["adjustment"] = merged_df["qty"] - merged_df["actual_available_stock"]

    for index, row in merged_df.iterrows():
        item_id = row["item_id"]
        quantity_adjusted = row["adjustment"]

        if pd.isna(item_id) or pd.isna(quantity_adjusted):
            logger.warning(f"Missing data for SKU {row['sku']}, skipping...")
            continue

        inv_payload = {
            "date": str(datetime.datetime.now().strftime("%Y-%m-%d")),
            "reason": "Stock Retally",
            "adjustment_type": "quantity",
            "line_items": [
                {
                    "item_id": item_id,
                    "quantity_adjusted": quantity_adjusted
                }
            ]
        }

        try:
            inventory_correction_response = post_record_to_zakya(
                api_domain,
                access_token,
                organization_id,
                'inventoryadjustments',
                inv_payload
            )
            logger.info(f"Adjustment posted for SKU {row['sku']}: {inventory_correction_response}")
        except Exception as e:
            logger.error(f"Error posting adjustment for SKU {row['sku']}: {e}")
