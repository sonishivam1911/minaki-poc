import pandas as pd
from pandas import json_normalize
from utils.zakya_api import fetch_records_from_zakya, retrieve_record_from_zakya
from utils.postgres_connector import crud
from config.logger import logger
from core.helper_zakya import extract_record_list


def flatten_item_json(json_data):
    """Flatten a single item JSON into a DataFrame"""
    try:
        df = json_normalize(
            json_data,
            sep='_',
            meta=[
                'group_id', 'group_name', 'item_id', 'name', 'sku', 'brand', 'manufacturer',
                'category_id', 'category_name', 'hsn_or_sac', 'status', 'source', 'unit', 'unit_id',
                'description', 'rate', 'account_id', 'account_name', 'purchase_description',
                'sales_rate', 'purchase_rate', 'label_rate', 'purchase_account_id',
                'purchase_account_name', 'inventory_account_id', 'inventory_account_name',
                'created_time', 'last_modified_time', 'can_be_sold', 'can_be_purchased',
                'track_inventory', 'item_type', 'product_type', 'is_returnable', 'reorder_level',
                'minimum_order_quantity', 'maximum_order_quantity', 'initial_stock',
                'initial_stock_rate', 'total_initial_stock', 'vendor_id', 'vendor_name',
                'stock_on_hand', 'asset_value', 'available_stock', 'actual_available_stock',
                'committed_stock', 'actual_committed_stock', 'available_for_sale_stock',
                'actual_available_for_sale_stock', 'quantity_in_transit', 'warehouses', 'sales_channels'
            ],
            max_level=1,
            errors='ignore'
        )
        
        # Handle custom fields
        custom_fields = json_data.get("custom_field_hash", {})
        for key, value in custom_fields.items():
            if 'unformatted' not in key:
                df[key] = value

        return df

    except Exception as e:
        logger.error(f"Error flattening item_id {json_data.get('item_id')}: {e}")
        return pd.DataFrame()  # Return empty frame on error

def classify_row(row):
    item_name = str(row['item_name']).lower()
    category = str(row['category']).lower()
    sku = str(row['sku']).lower()
    work = str(row.get('custom_field_hash_cf_work', '')).lower()
    collection = str(row.get('custom_field_hash_cf_collection', '')).strip()

    # CATEGORY
    if 'set' in item_name or 'set' in category:
        cat = 'Jewellery Set'
    elif any(x in item_name for x in ['earring', 'hoop', 'jhum', 'dangler']) or 'earring' in category:
        cat = 'Earrings'
    elif 'necklace' in item_name or 'necklace' in category:
        cat = 'Necklace'
    elif any(x in item_name for x in ['teeka', 'maang']) or 'maang' in category:
        cat = 'Maang Teeka'
    elif 'matha' in item_name:
        cat = 'Matha Patti'
    elif 'brace' in item_name:
        cat = 'Bracelets'
    elif 'kada' in item_name:
        cat = 'Kadas'
    elif 'ring' in item_name:
        cat = 'Rings'
    elif category.strip() != '':
        cat = row['category']
    else:
        cat = 'Uncategorized'

    # SUB-CATEGORY
    if 'set' in item_name and 'choker' in item_name or sku.startswith('ms'):
        sub_cat = 'Choker Necklace Set'
    elif 'set' in item_name and 'long' in item_name or sku.startswith('ml'):
        sub_cat = 'Long Necklace Set'
    elif 'set' in item_name and 'collar' in item_name or sku.startswith('mm'):
        sub_cat = 'Collar Necklace Set'
    elif 'necklace' in item_name and 'choker' in item_name or sku.startswith('ms'):
        sub_cat = 'Choker Necklace'
    elif 'necklace' in item_name and 'long' in item_name or sku.startswith('ml'):
        sub_cat = 'Long Necklace'
    elif 'necklace' in item_name and 'collar' in item_name or sku.startswith('mm'):
        sub_cat = 'Collar Necklace'
    elif any(x in item_name for x in ['chaand', 'chand', 'baali', 'bali']):
        sub_cat = 'Chaand Baali Earrings'
    elif 'dangler' in item_name:
        sub_cat = 'Dangler Earrings'
    elif 'hoop' in item_name:
        sub_cat = 'Hoop Earrings'
    elif 'jhumk' in item_name:
        sub_cat = 'Jhumka Earrings'
    elif 'stud ' in item_name or 'studs' in item_name:
        sub_cat = 'Stud Earrings'
    else:
        sub_cat = cat

    # COLLECTION (only update if empty)
    if collection:
        final_collection = collection
    elif 'kundan' in item_name or 'kundan' in work:
        final_collection = 'Shahana Kundan'
    elif 'temple' in item_name or 'temple' in work:
        final_collection = 'Minakshi Temple'
    elif sku.startswith('mx') or sku.startswith('mcp') or sku.startswith('mcz'):
        final_collection = 'Eleganza'
    elif 'crystal' in work:
        final_collection = 'Crystal'
    else:
        final_collection = 'Unclassified'

    return pd.Series({
        'cat': cat,
        'sub_cat': sub_cat,
        'collection': final_collection
    })


def create_item_master(api_domain, access_token, organization_id):
    # Step 1: Fetch all item IDs
    all_items = fetch_records_from_zakya(api_domain, access_token, organization_id, '/items')
    item_records = extract_record_list(all_items, "items")
    item_ids = [item.get("item_id") for item in item_records if item.get("item_id")]

    # Step 2: For each item_id, retrieve its full record
    all_item_dfs = []
    for item_id in item_ids:
        endpoint1 = "items/" + item_id
        try:
            item_json = retrieve_record_from_zakya(api_domain, access_token, organization_id, endpoint1)
            item_df = flatten_item_json(item_json)
            if not item_df.empty:
                all_item_dfs.append(item_df)
        except Exception as e:
            logger.warning(f"Skipping item_id {item_id} due to error: {e}")

    # Step 3: Combine all records
    final_df = pd.concat(all_item_dfs, ignore_index=True)
    final_df[['cat', 'sub_cat', 'collection']] = final_df.apply(classify_row, axis=1)

    # Step 4: Export to CSV (optional)
    final_df.to_csv("output.csv", index=False)
    print("CSV exported successfully with shape:", final_df.shape)
    crud.create_table('prod.master__item', final_df)
    return final_df


