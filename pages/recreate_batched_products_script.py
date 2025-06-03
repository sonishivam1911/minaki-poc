import streamlit as st
import pandas as pd
import requests
import time
from utils.zakya_api import (
    fetch_records_from_zakya, 
    extract_record_list, 
    retrieve_record_from_zakya, 
    post_record_to_zakya
)
from app import fetch_and_assign_session_variables

def delete_record_from_zakya(base_url, access_token, organization_id, endpoint, record_id):
    """Delete a record from Zakya API."""
    url = f"{base_url}inventory/v1/{endpoint}/{record_id}"
    
    headers = {
        'Authorization': f"Zoho-oauthtoken {access_token}",
        'Content-Type': 'application/json'
    }
    params = {'organization_id': organization_id}

    response = requests.delete(url=url, headers=headers, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        st.error(f"Error deleting item {record_id}: {response.text}")
        return None

def get_items_with_batch_tracking():
    """Fetch all items and filter those with batch tracking enabled."""
    if 'access_token' not in st.session_state:
        fetch_and_assign_session_variables()
    
    with st.spinner("Fetching all items..."):
        # Fetch all items
        all_items_data = fetch_records_from_zakya(
            st.session_state['api_domain'], 
            st.session_state['access_token'], 
            st.session_state['organization_id'], 
            "/items"
        )
        all_items = extract_record_list(all_items_data, 'items')
        
        # Filter items that have track_batch_number = True
        batch_tracked_items = []
        for item in all_items:
            if item.get('track_batch_number', False):
                batch_tracked_items.append(item)
        
    return batch_tracked_items

def create_new_item_without_batch_tracking(original_item):
    """Create payload for new item without batch tracking."""
    new_item_payload = {
        "name": original_item["name"],
        "sku": original_item["sku"],
        "item_type": "inventory",
        "product_type": "goods",
        "track_inventory": True,
        "track_batch_number": False,
        "track_serial_number": False,
        "rate": original_item["rate"],
        "sales_rate": original_item.get("sales_rate", original_item["rate"]),
        "purchase_rate": original_item.get("purchase_rate", original_item["rate"]),
        "is_taxable": original_item.get("is_taxable", True),
        "can_be_sold": original_item.get("can_be_sold", True),
        "can_be_purchased": original_item.get("can_be_purchased", True),
        "unit": original_item.get("unit", "pcs")
    }

    # Add optional fields
    optional_fields = [
        "brand", "manufacturer", "category_id", "hsn_or_sac", "description",
        "account_id", "purchase_account_id", "inventory_account_id",
        "group_id", "group_name", "reorder_level", "upc", "ean", "isbn", "part_number"
    ]
    
    for field in optional_fields:
        if field in original_item and original_item[field]:
            new_item_payload[field] = original_item[field]

    # Add ALL attribute fields (this is crucial for group items)
    attribute_fields = [
        "attribute_id1", "attribute_id2", "attribute_id3",
        "attribute_name1", "attribute_name2", "attribute_name3", 
        "attribute_type1", "attribute_type2", "attribute_type3",
        "attribute_option_id1", "attribute_option_id2", "attribute_option_id3",
        "attribute_option_name1", "attribute_option_name2", "attribute_option_name3",
        "attribute_option_data1", "attribute_option_data2", "attribute_option_data3"
    ]
    
    for field in attribute_fields:
        if field in original_item and original_item[field]:
            new_item_payload[field] = original_item[field]

    # Add tax preferences
    if "item_tax_preferences" in original_item and original_item["item_tax_preferences"]:
        new_item_payload["item_tax_preferences"] = original_item["item_tax_preferences"]

    # Add custom fields
    if "custom_fields" in original_item and original_item["custom_fields"]:
        new_item_payload["custom_fields"] = []
        for cf in original_item["custom_fields"]:
            new_item_payload["custom_fields"].append({
                "customfield_id": cf["customfield_id"],
                "value": cf["value"]
            })

    # Calculate stock from warehouses
    total_stock = 0
    warehouse_data = []
    
    if "warehouses" in original_item:
        for warehouse in original_item["warehouses"]:
            stock = warehouse.get("warehouse_stock_on_hand", 0)
            if stock > 0:
                total_stock += stock
                warehouse_data.append({
                    "warehouse_id": warehouse["warehouse_id"],
                    "initial_stock": stock,
                    "initial_stock_rate": original_item.get("purchase_rate", original_item["rate"])
                })

    if total_stock > 0:
        new_item_payload["initial_stock"] = total_stock
        new_item_payload["initial_stock_rate"] = original_item.get("purchase_rate", original_item["rate"])
        if warehouse_data:
            new_item_payload["warehouses"] = warehouse_data

    # Debug: Log the payload to see what's being sent
    st.write("**Debug - New Item Payload:**")
    st.json(new_item_payload)

    return new_item_payload

def process_selected_items_callback(selected_item_ids):
    """Callback function to process selected items."""
    st.session_state.processing = True
    st.session_state.processing_results = process_selected_items(selected_item_ids)
    st.session_state.processing = False

def mark_item_inactive(base_url, access_token, organization_id, item_id):
    """Mark an item as inactive in Zakya API."""
    url = f"{base_url}inventory/v1/items/{item_id}/inactive"
    
    headers = {
        'Authorization': f"Zoho-oauthtoken {access_token}",
        'Content-Type': 'application/json'
    }
    params = {'organization_id': organization_id}

    response = requests.post(url=url, headers=headers, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        st.error(f"Error marking item {item_id} as inactive: {response.text}")
        return None

def update_item_sku(base_url, access_token, organization_id, item_id, new_sku):
    """Update an item's SKU in Zakya API."""
    url = f"{base_url}inventory/v1/items/{item_id}"
    
    headers = {
        'Authorization': f"Zoho-oauthtoken {access_token}",
        'Content-Type': 'application/json'
    }
    params = {'organization_id': organization_id}
    payload = {'sku': new_sku}

    response = requests.put(url=url, headers=headers, params=params, json=payload)
    
    if response.status_code == 200:
        return response.json()
    else:
        st.error(f"Error updating SKU for item {item_id}: {response.text}")
        return None

def process_selected_items(selected_item_ids):
    """Process selected items to remove batch tracking."""
    results = []
    
    st.write(f"Processing {len(selected_item_ids)} items...")
    
    for i, item_id in enumerate(selected_item_ids):
        # Ensure item_id is a string
        item_id = str(item_id)
        
        try:
            # Fetch detailed item info
            detailed_item = retrieve_record_from_zakya(
                st.session_state['api_domain'], 
                st.session_state['access_token'], 
                st.session_state['organization_id'], 
                f"items/{item_id}"
            )
            
            if not detailed_item or 'item' not in detailed_item:
                results.append({
                    "SKU": "N/A",
                    "Name": "N/A", 
                    "Original ID": item_id,
                    "New ID": "N/A",
                    "Status": "❌ Failed to fetch item details"
                })
                continue
                
            item = detailed_item['item']
            original_sku = item.get('sku', '')
            st.info(f"Processing: {item['name']} (SKU: {original_sku})")
            
            # Step 1: Update SKU of original item to avoid conflicts
            inactive_sku = f"{original_sku}-INACTIVE" if original_sku else f"INACTIVE-{item_id}"
            st.write(f"🔄 Updating original item SKU to: {inactive_sku}")
            
            sku_update_result = update_item_sku(
                st.session_state['api_domain'],
                st.session_state['access_token'],
                st.session_state['organization_id'],
                item["item_id"],
                inactive_sku
            )
            
            if not sku_update_result or sku_update_result.get("code") != 0:
                results.append({
                    "Original SKU": original_sku,
                    "New SKU": original_sku,
                    "Name": item["name"],
                    "Original ID": item["item_id"],
                    "New ID": "N/A",
                    "Status": "❌ Failed to update original item SKU"
                })
                continue
            
            st.success(f"✅ Original item SKU updated to: {inactive_sku}")
            time.sleep(1)
            
            # Step 2: Mark original item as inactive
            st.write("🔄 Marking item as inactive...")
            inactive_result = mark_item_inactive(
                st.session_state['api_domain'],
                st.session_state['access_token'], 
                st.session_state['organization_id'], 
                item["item_id"]
            )
            
            if not inactive_result or inactive_result.get("code") != 0:
                results.append({
                    "Original SKU": original_sku,
                    "New SKU": original_sku,
                    "Name": item["name"],
                    "Original ID": item["item_id"],
                    "New ID": "N/A",
                    "Status": "❌ Failed to mark as inactive"
                })
                continue
            
            st.success("✅ Item marked as inactive")
            time.sleep(1)
            
            # Step 3: Create new item payload with original SKU
            new_item_payload = create_new_item_without_batch_tracking(item)
            new_item_payload['sku'] = original_sku  # Use original SKU for new item
            
            # Step 4: Create new item without batch tracking
            st.write(f"🔄 Creating new item with original SKU: {original_sku}")
            create_result = post_record_to_zakya(
                st.session_state['api_domain'],
                st.session_state['access_token'], 
                st.session_state['organization_id'], 
                "items", 
                new_item_payload
            )
            
            if create_result and create_result.get("code") == 0:
                st.success(f"✅ New item created successfully with SKU: {original_sku}")
                results.append({
                    "Original SKU": original_sku,
                    "New SKU": original_sku,
                    "Name": item["name"],
                    "Original ID": item["item_id"],
                    "New ID": create_result["item"]["item_id"],
                    "Status": "✅ Success - Original inactive, New active"
                })
            else:
                results.append({
                    "Original SKU": original_sku,
                    "New SKU": original_sku,
                    "Name": item["name"],
                    "Original ID": item["item_id"],
                    "New ID": "N/A",
                    "Status": "❌ Failed to create new item"
                })
                
        except Exception as e:
            st.error(f"❌ Error processing item {item_id}: {str(e)}")
            results.append({
                "Original SKU": "N/A",
                "New SKU": "N/A",
                "Name": "N/A",
                "Original ID": item_id,
                "New ID": "N/A",
                "Status": f"❌ Error: {str(e)}"
            })
        
        st.write("---")  # Separator between items
        time.sleep(1)  # Rate limiting
    
    return results

# Main Streamlit App
def main():
    st.title("🔄 Remove Batch Tracking from Items")
    st.write("This tool removes batch tracking by making items inactive and creating new active items without batch tracking.")
    
    st.info("""
    📋 **Process Overview:**
    1. **Update Original SKU** - Add '-INACTIVE' suffix to avoid conflicts
    2. **Mark Original as Inactive** - Preserves transaction history  
    3. **Create New Active Item** - Original SKU, no batch tracking
    4. **Result** - Original item inactive with modified SKU, new item active with original SKU
    
    ✅ **Benefits:**
    - Original SKU maintained on new active item
    - Transaction history preserved on inactive item
    - No SKU conflicts
    - No batch tracking on new items
    """)
    
    # Initialize session state
    if 'batch_items' not in st.session_state:
        st.session_state.batch_items = None
    if 'processing' not in st.session_state:
        st.session_state.processing = False
    if 'processing_results' not in st.session_state:
        st.session_state.processing_results = None
    
    # Fetch items button
    col1, col2 = st.columns([1, 3])
    
    with col1:
        if st.button("🔍 Fetch Items with Batch Tracking", type="primary"):
            st.session_state.batch_items = get_items_with_batch_tracking()
    
    # Display items if available
    if st.session_state.batch_items is not None:
        if len(st.session_state.batch_items) == 0:
            st.success("🎉 No items with batch tracking found!")
            return
        
        st.write(f"**Found {len(st.session_state.batch_items)} items with batch tracking:**")
        
        # Create DataFrame for display
        display_data = []
        for item in st.session_state.batch_items:
            display_data.append({
                "Select": False,
                "Item ID": item["item_id"],
                "SKU": item.get("sku", "N/A"),
                "Name": item["name"],
                "Stock": item.get("stock_on_hand", 0),
                "Rate": item.get("rate", 0),
                "Brand": item.get("brand", "N/A"),
                "Track Batch": item.get("track_batch_number", False)
            })
        
        df = pd.DataFrame(display_data)
        
        # Display editable dataframe
        edited_df = st.data_editor(
            df,
            column_config={
                "Select": st.column_config.CheckboxColumn(
                    "Select",
                    help="Select items to process",
                    default=False,
                ),
                "Item ID": st.column_config.TextColumn("Item ID", disabled=True),
                "SKU": st.column_config.TextColumn("SKU", disabled=True),
                "Name": st.column_config.TextColumn("Name", disabled=True),
                "Stock": st.column_config.NumberColumn("Stock", disabled=True),
                "Rate": st.column_config.NumberColumn("Rate", disabled=True),
                "Brand": st.column_config.TextColumn("Brand", disabled=True),
                "Track Batch": st.column_config.CheckboxColumn("Track Batch", disabled=True),
            },
            disabled=["Item ID", "SKU", "Name", "Stock", "Rate", "Brand", "Track Batch"],
            hide_index=True,
            use_container_width=True
        )
        
        # Get selected item IDs
        selected_indices = edited_df[edited_df["Select"]].index.tolist()
        selected_item_ids = [st.session_state.batch_items[i]["item_id"] for i in selected_indices]
        
        st.write(f"**Selected {len(selected_item_ids)} items**")
        
        # Debug information
        if selected_item_ids:
            st.write("Selected Item IDs:")
            for item_id in selected_item_ids:
                st.write(f"- {item_id} (type: {type(item_id)})")
        
        # Process button
        if selected_item_ids:
            # Ensure selected_item_ids is a list of strings
            if isinstance(selected_item_ids, str):
                selected_item_ids = [selected_item_ids]
            elif isinstance(selected_item_ids, list):
                selected_item_ids = [str(item_id) for item_id in selected_item_ids]
            
            st.button(
                "🚀 Create New Products (Remove Batch Tracking)", 
                type="primary",
                on_click=process_selected_items_callback,
                args=(selected_item_ids,),
                key="process_button"
            )
            
            # Show processing status
            if st.session_state.get('processing', False):
                st.info("🔄 Processing items... Please wait...")
            
            # Show results if available
            if 'processing_results' in st.session_state and st.session_state.processing_results:
                results = st.session_state.processing_results
                
                st.write("## 📊 Processing Results")
                results_df = pd.DataFrame(results)
                st.dataframe(results_df, use_container_width=True)
                
                # Summary
                success_count = len([r for r in results if "✅ Success" in r["Status"]])
                st.write(f"### Summary: {success_count}/{len(results)} items processed successfully")
                
                # Download results as CSV
                csv = results_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Results CSV",
                    data=csv,
                    file_name="batch_tracking_removal_results.csv",
                    mime="text/csv"
                )

if __name__ == "__main__":
    main()