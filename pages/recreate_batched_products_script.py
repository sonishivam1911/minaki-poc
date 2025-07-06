import streamlit as st
import pandas as pd
import requests
import time
from config.logger import logger
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
        logger.info(f"Successfully deleted record {record_id}")
        return response.json()
    else:
        logger.error(f"Error deleting item {record_id}: {response.text}")
        st.error(f"Error deleting item {record_id}: {response.text}")
        return None

def mark_item_group_inactive(base_url, access_token, organization_id, group_id):
    """Mark an item group as inactive in Zakya API."""
    url = f"{base_url}inventory/v1/itemgroups/{group_id}/inactive"
    
    headers = {
        'Authorization': f"Zoho-oauthtoken {access_token}",
        'Content-Type': 'application/json'
    }
    params = {'organization_id': organization_id}

    response = requests.post(url=url, headers=headers, params=params)
    
    if response.status_code == 200:
        logger.info(f"Successfully marked item group {group_id} as inactive")
        return response.json()
    else:
        logger.error(f"Error marking item group {group_id} as inactive: {response.text}")
        st.error(f"Error marking item group {group_id} as inactive: {response.text}")
        return None

def mark_item_group_active(base_url, access_token, organization_id, group_id):
    """Mark an item group as active in Zakya API."""
    url = f"{base_url}inventory/v1/itemgroups/{group_id}/active"
    
    headers = {
        'Authorization': f"Zoho-oauthtoken {access_token}",
        'Content-Type': 'application/json'
    }
    params = {'organization_id': organization_id}

    response = requests.post(url=url, headers=headers, params=params)
    
    if response.status_code == 200:
        logger.info(f"Successfully marked item group {group_id} as active")
        return response.json()
    else:
        logger.error(f"Error marking item group {group_id} as active: {response.text}")
        st.error(f"Error marking item group {group_id} as active: {response.text}")
        return None

def create_item_group_for_no_batch(original_group_data, new_group_name):
    """Create a new item group based on original group but for no-batch items."""
    logger.info(f"Creating new item group: {new_group_name}")
    
    # Create new group payload based on original
    new_group_payload = {
        "group_name": new_group_name,
        "brand": original_group_data.get("brand", ""),
        "manufacturer": original_group_data.get("manufacturer", ""),
        "unit": original_group_data.get("unit", "pcs"),
        "description": f"No batch tracking version - {original_group_data.get('description', '')}",
        "item_type": "inventory"
    }
    
    # Copy tax information if present
    if original_group_data.get("tax_id"):
        new_group_payload["tax_id"] = original_group_data["tax_id"]
    
    # Copy attributes structure if present
    if original_group_data.get("attribute_name1"):
        new_group_payload["attribute_name1"] = original_group_data["attribute_name1"]
    
    # Note: We'll add items to this group later, so we don't include items array here
    
    logger.info(f"Creating group with payload: {new_group_payload}")
    
    # Create the new group
    result = post_record_to_zakya(
        st.session_state['api_domain'],
        st.session_state['access_token'],
        st.session_state['organization_id'],
        "itemgroups",
        new_group_payload
    )
    
    if result and result.get("code") == 0:
        new_group_id = result.get("group_id")
        logger.info(f"Successfully created new group {new_group_name} with ID: {new_group_id}")
        return new_group_id
    else:
        logger.error(f"Failed to create new group: {result}")
        return None

def get_items_with_batch_tracking():
    """Fetch all items and filter those with batch tracking enabled (excluding -INACTIVE items)."""
    if 'access_token' not in st.session_state:
        fetch_and_assign_session_variables()
    
    logger.info("Starting to fetch items with batch tracking")
    
    with st.spinner("Fetching all items..."):
        # Fetch all items
        all_items_data = fetch_records_from_zakya(
            st.session_state['api_domain'], 
            st.session_state['access_token'], 
            st.session_state['organization_id'], 
            "/items"
        )
        all_items = extract_record_list(all_items_data, 'items')
        
        # Filter items that have track_batch_number = True AND do not have -INACTIVE in SKU
        batch_tracked_items = []
        for item in all_items:
            sku = item.get('sku', '')
            if (item.get('track_batch_number', False) and 
                '-INACTIVE' not in sku.upper()):
                batch_tracked_items.append(item)
        
    logger.info(f"Found {len(batch_tracked_items)} items with batch tracking (excluding -INACTIVE items)")
    return batch_tracked_items

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
        logger.info(f"Successfully marked item {item_id} as inactive")
        return response.json()
    else:
        logger.error(f"Error marking item {item_id} as inactive: {response.text}")
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
        logger.info(f"Successfully updated SKU for item {item_id} to {new_sku}")
        return response.json()
    else:
        logger.error(f"Error updating SKU for item {item_id}: {response.text}")
        st.error(f"Error updating SKU for item {item_id}: {response.text}")
        return None

def create_new_item_for_group(original_item_data, new_group_id, new_group_name):
    """Create payload for new item without batch tracking for a specific group."""
    logger.info(f"Creating item payload for: {original_item_data['name']} in group: {new_group_name}")
    
    new_item_payload = {
        "name": original_item_data["name"],
        "sku": original_item_data["sku"],  # Will use original SKU
        "item_type": "inventory",
        "product_type": "goods",
        "track_inventory": True,
        "track_batch_number": False,
        "track_serial_number": False,
        "rate": original_item_data["rate"],
        "sales_rate": original_item_data.get("sales_rate", original_item_data["rate"]),
        "purchase_rate": original_item_data.get("purchase_rate", original_item_data["rate"]),
        "is_taxable": original_item_data.get("is_taxable", True),
        "can_be_sold": original_item_data.get("can_be_sold", True),
        "can_be_purchased": original_item_data.get("can_be_purchased", True),
        "unit": original_item_data.get("unit", "pcs"),
        "group_id": new_group_id,
        "group_name": new_group_name
    }

    # Add optional fields
    optional_fields = [
        "brand", "manufacturer", "category_id", "hsn_or_sac", "description",
        "account_id", "purchase_account_id", "inventory_account_id",
        "reorder_level", "upc", "ean", "isbn", "part_number"
    ]
    
    for field in optional_fields:
        if field in original_item_data and original_item_data[field]:
            new_item_payload[field] = original_item_data[field]

    # Add attribute fields for group variants
    attribute_fields = [
        "attribute_id1", "attribute_id2", "attribute_id3",
        "attribute_name1", "attribute_name2", "attribute_name3", 
        "attribute_option_id1", "attribute_option_id2", "attribute_option_id3",
        "attribute_option_name1", "attribute_option_name2", "attribute_option_name3",
        "attribute_option_data1", "attribute_option_data2", "attribute_option_data3"
    ]
    
    for field in attribute_fields:
        if field in original_item_data and original_item_data[field]:
            new_item_payload[field] = original_item_data[field]

    # Add tax preferences
    if "item_tax_preferences" in original_item_data and original_item_data["item_tax_preferences"]:
        new_item_payload["item_tax_preferences"] = original_item_data["item_tax_preferences"]

    # Add custom fields
    if "custom_fields" in original_item_data and original_item_data["custom_fields"]:
        new_item_payload["custom_fields"] = []
        for cf in original_item_data["custom_fields"]:
            new_item_payload["custom_fields"].append({
                "customfield_id": cf["customfield_id"],
                "value": cf["value"]
            })

    # Calculate stock from warehouses
    total_stock = 0
    warehouse_data = []
    
    if "warehouses" in original_item_data:
        for warehouse in original_item_data["warehouses"]:
            stock = warehouse.get("warehouse_stock_on_hand", 0)
            if stock > 0:
                total_stock += stock
                warehouse_data.append({
                    "warehouse_id": warehouse["warehouse_id"],
                    "initial_stock": stock,
                    "initial_stock_rate": original_item_data.get("purchase_rate", original_item_data["rate"])
                })

    if total_stock > 0:
        new_item_payload["initial_stock"] = total_stock
        new_item_payload["initial_stock_rate"] = original_item_data.get("purchase_rate", original_item_data["rate"])
        if warehouse_data:
            new_item_payload["warehouses"] = warehouse_data

    logger.info(f"Created payload for item: {original_item_data['name']} in group: {new_group_name}")
    return new_item_payload

def process_all_items_with_group_management(items):
    """Process all items with proper group management and mapping."""
    logger.info(f"Starting to process {len(items)} items with group management")
    
    results = []
    group_mapping = {}  # {old_group_id: new_group_id}
    processed_groups = set()  # Track which groups we've already processed
    inactive_groups = set()  # Track which groups we've marked as inactive
    total_items = len(items)
    
    # Create progress tracking
    progress_bar = st.progress(0)
    status_text = st.empty()
    current_item_info = st.empty()
    results_container = st.empty()
    
    for i, item in enumerate(items):
        # Update progress
        progress = (i + 1) / total_items
        progress_bar.progress(progress)
        status_text.text(f"Processing item {i + 1} of {total_items} ({progress:.1%})")
        
        item_id = str(item["item_id"])
        original_sku = item.get('sku', '')
        item_name = item.get('name', 'Unknown')
        
        current_item_info.info(f"🔄 Processing: {item_name} (SKU: {original_sku})")
        logger.info(f"Processing item {i+1}/{total_items}: {item_name} (SKU: {original_sku})")
        
        try:
            # Step 1: Fetch detailed item info
            detailed_item = retrieve_record_from_zakya(
                st.session_state['api_domain'], 
                st.session_state['access_token'], 
                st.session_state['organization_id'], 
                f"items/{item_id}"
            )
            
            if not detailed_item or 'item' not in detailed_item:
                logger.error(f"Failed to fetch item details for {item_id}")
                results.append({
                    "Original SKU": original_sku,
                    "New SKU": original_sku,
                    "Name": item_name, 
                    "Original ID": item_id,
                    "New ID": "N/A",
                    "Group Status": "N/A",
                    "Status": "❌ Failed to fetch item details"
                })
                continue
                
            detailed_item_data = detailed_item['item']
            original_group_id = detailed_item_data.get('group_id')
            original_group_name = detailed_item_data.get('group_name', 'No Group')
            
            new_group_id = None
            new_group_name = "Standalone Item"
            group_status = "No Group"
            
            # Step 2: Handle group management
            if original_group_id:
                if original_group_id not in group_mapping:
                    # Fetch original group details
                    logger.info(f"Fetching group details for group ID: {original_group_id}")
                    original_group = retrieve_record_from_zakya(
                        st.session_state['api_domain'],
                        st.session_state['access_token'],
                        st.session_state['organization_id'],
                        f"itemgroups/{original_group_id}"
                    )
                    
                    if original_group and 'group' in original_group:
                        original_group_data = original_group['group']
                        new_group_name = f"{original_group_name} (No Batch)"
                        
                        # Create new group
                        new_group_id = create_item_group_for_no_batch(original_group_data, new_group_name)
                        
                        if new_group_id:
                            group_mapping[original_group_id] = new_group_id
                            processed_groups.add(original_group_id)
                            logger.info(f"Created mapping: {original_group_id} -> {new_group_id}")
                            
                            # Mark original group as inactive (only once per group)
                            if original_group_id not in inactive_groups:
                                mark_result = mark_item_group_inactive(
                                    st.session_state['api_domain'],
                                    st.session_state['access_token'],
                                    st.session_state['organization_id'],
                                    original_group_id
                                )
                                if mark_result:
                                    inactive_groups.add(original_group_id)
                                    logger.info(f"Marked original group {original_group_id} as inactive")
                            
                            group_status = f"New Group Created: {new_group_name}"
                        else:
                            logger.error(f"Failed to create new group for {original_group_name}")
                            group_status = "❌ Failed to create group"
                    else:
                        logger.error(f"Failed to fetch group details for {original_group_id}")
                        group_status = "❌ Failed to fetch group"
                else:
                    # Use existing mapping
                    new_group_id = group_mapping[original_group_id]
                    new_group_name = f"{original_group_name} (No Batch)"
                    group_status = f"Using Existing Group: {new_group_name}"
                    logger.info(f"Using existing group mapping: {original_group_id} -> {new_group_id}")
            
            # Step 3: Update original item SKU
            inactive_sku = f"{original_sku}-INACTIVE" if original_sku else f"INACTIVE-{item_id}"
            logger.info(f"Updating original item SKU to: {inactive_sku}")
            
            sku_update_result = update_item_sku(
                st.session_state['api_domain'],
                st.session_state['access_token'],
                st.session_state['organization_id'],
                detailed_item_data["item_id"],
                inactive_sku
            )
            
            if not sku_update_result or sku_update_result.get("code") != 0:
                logger.error(f"Failed to update SKU for item {item_id}")
                results.append({
                    "Original SKU": original_sku,
                    "New SKU": original_sku,
                    "Name": detailed_item_data["name"],
                    "Original ID": detailed_item_data["item_id"],
                    "New ID": "N/A",
                    "Group Status": group_status,
                    "Status": "❌ Failed to update original item SKU"
                })
                continue
            
            time.sleep(0.5)  # Rate limiting
            
            # Step 4: Mark original item as inactive
            logger.info(f"Marking item {item_id} as inactive")
            inactive_result = mark_item_inactive(
                st.session_state['api_domain'],
                st.session_state['access_token'], 
                st.session_state['organization_id'], 
                detailed_item_data["item_id"]
            )
            
            if not inactive_result or inactive_result.get("code") != 0:
                logger.error(f"Failed to mark item {item_id} as inactive")
                results.append({
                    "Original SKU": original_sku,
                    "New SKU": original_sku,
                    "Name": detailed_item_data["name"],
                    "Original ID": detailed_item_data["item_id"],
                    "New ID": "N/A",
                    "Group Status": group_status,
                    "Status": "❌ Failed to mark as inactive"
                })
                continue
            
            time.sleep(0.5)  # Rate limiting
            
            # Step 5: Create new item
            if new_group_id:
                new_item_payload = create_new_item_for_group(detailed_item_data, new_group_id, new_group_name)
            else:
                # Create standalone item
                new_item_payload = create_new_item_for_group(detailed_item_data, None, "")
                new_item_payload.pop('group_id', None)
                new_item_payload.pop('group_name', None)
            
            new_item_payload['sku'] = original_sku  # Use original SKU for new item
            
            logger.info(f"Creating new item with SKU: {original_sku}")
            create_result = post_record_to_zakya(
                st.session_state['api_domain'],
                st.session_state['access_token'], 
                st.session_state['organization_id'], 
                "items", 
                new_item_payload
            )
            
            if create_result and create_result.get("code") == 0:
                logger.info(f"Successfully created new item for SKU: {original_sku}")
                results.append({
                    "Original SKU": original_sku,
                    "New SKU": original_sku,
                    "Name": detailed_item_data["name"],
                    "Original ID": detailed_item_data["item_id"],
                    "New ID": create_result["item"]["item_id"],
                    "Group Status": group_status,
                    "Status": "✅ Success - Item and Group processed"
                })
            else:
                logger.error(f"Failed to create new item for SKU: {original_sku}")
                results.append({
                    "Original SKU": original_sku,
                    "New SKU": original_sku,
                    "Name": detailed_item_data["name"],
                    "Original ID": detailed_item_data["item_id"],
                    "New ID": "N/A",
                    "Group Status": group_status,
                    "Status": "❌ Failed to create new item"
                })
                
        except Exception as e:
            logger.error(f"Error processing item {item_id}: {str(e)}")
            results.append({
                "Original SKU": original_sku,
                "New SKU": original_sku,
                "Name": item_name,
                "Original ID": item_id,
                "New ID": "N/A",
                "Group Status": "Error",
                "Status": f"❌ Error: {str(e)}"
            })
        
        # Update live results display
        if results:
            results_df = pd.DataFrame(results)
            success_count = len([r for r in results if "✅ Success" in r["Status"]])
            
            with results_container.container():
                st.write(f"### 📊 Live Results - Processed: {len(results)}/{total_items} | Success: {success_count}")
                st.dataframe(results_df, use_container_width=True)
        
        time.sleep(0.5)  # Rate limiting between items
    
    # Final updates
    progress_bar.progress(1.0)
    status_text.text("✅ Processing completed!")
    current_item_info.empty()
    
    logger.info(f"Processing complete. Groups created: {len(group_mapping)}, Groups marked inactive: {len(inactive_groups)}")
    logger.info(f"Group mapping: {group_mapping}")
    
    return results, group_mapping

def get_all_inactive_items():
    """Fetch all inactive items from the system."""
    if 'access_token' not in st.session_state:
        fetch_and_assign_session_variables()
    
    logger.info("Starting to fetch all inactive items")
    
    with st.spinner("Fetching all items to filter inactive ones..."):
        all_items_data = fetch_records_from_zakya(
            st.session_state['api_domain'], 
            st.session_state['access_token'], 
            st.session_state['organization_id'], 
            "/items"
        )
        all_items = extract_record_list(all_items_data, 'items')
        
        inactive_items = []
        for item in all_items:
            if item.get('status', '').lower() == 'inactive':
                inactive_items.append(item)
        
    logger.info(f"Found {len(inactive_items)} inactive items out of {len(all_items)} total items")
    return inactive_items

def get_all_active_items():
    """Fetch all active items from the system."""
    if 'access_token' not in st.session_state:
        fetch_and_assign_session_variables()
    
    logger.info("Starting to fetch all active items")
    
    with st.spinner("Fetching all items to filter active ones..."):
        all_items_data = fetch_records_from_zakya(
            st.session_state['api_domain'], 
            st.session_state['access_token'], 
            st.session_state['organization_id'], 
            "/items"
        )
        all_items = extract_record_list(all_items_data, 'items')
        
        active_items = []
        for item in all_items:
            if item.get('status', '').lower() == 'active':
                active_items.append(item)
        
    logger.info(f"Found {len(active_items)} active items out of {len(all_items)} total items")
    return active_items

def cleanup_inactive_items_with_groups():
    """Enhanced cleanup that also handles group reactivation."""
    logger.info("Starting enhanced cleanup of inactive items with group management")
    
    # Get all inactive items
    inactive_items = get_all_inactive_items()
    
    # Filter inactive items that have -INACTIVE suffix
    inactive_with_suffix = []
    for item in inactive_items:
        sku = item.get('sku', '')
        if sku.upper().endswith('-INACTIVE'):
            inactive_with_suffix.append(item)
    
    logger.info(f"Found {len(inactive_with_suffix)} inactive items with -INACTIVE suffix")
    
    if not inactive_with_suffix:
        st.info("No inactive items with -INACTIVE suffix found!")
        return [], []
    
    # Get all active items to check against
    active_items = get_all_active_items()
    
    # Create a mapping of active SKUs
    active_skus = set()
    for item in active_items:
        sku = item.get('sku', '')
        active_skus.add(sku)
    
    logger.info(f"Found {len(active_skus)} active item SKUs for comparison")
    
    restored_items = []
    failed_items = []
    groups_to_reactivate = set()  # Track groups that need reactivation
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i, inactive_item in enumerate(inactive_with_suffix):
        current_sku = inactive_item.get('sku', '')
        original_sku = current_sku.replace('-INACTIVE', '').replace('-inactive', '')
        item_name = inactive_item.get('name', 'N/A')
        item_id = inactive_item.get('item_id')
        item_group_id = inactive_item.get('group_id')
        
        logger.info(f"Processing cleanup {i+1}/{len(inactive_with_suffix)}: {item_name} (SKU: {current_sku})")
        status_text.text(f"Processing cleanup {i+1}/{len(inactive_with_suffix)}: {item_name}")
        
        try:
            # Check if active item with original SKU exists
            if original_sku not in active_skus:
                logger.info(f"No active item with SKU {original_sku} found - restoring SKU and potentially reactivating group")
                
                # Update the SKU to remove -INACTIVE suffix
                update_result = update_item_sku(
                    st.session_state['api_domain'],
                    st.session_state['access_token'],
                    st.session_state['organization_id'],
                    item_id,
                    original_sku
                )
                
                if update_result and update_result.get("code") == 0:
                    logger.info(f"Successfully restored SKU from {current_sku} to {original_sku}")
                    
                    # If item belongs to a group, mark group for reactivation
                    if item_group_id:
                        groups_to_reactivate.add(item_group_id)
                        logger.info(f"Added group {item_group_id} to reactivation list")
                    
                    restored_items.append({
                        "Name": item_name,
                        "Old_SKU": current_sku,
                        "New_SKU": original_sku,
                        "Item_ID": item_id,
                        "Group_ID": item_group_id or "No Group"
                    })
                else:
                    logger.error(f"Failed to restore SKU for item {item_id}")
                    failed_items.append({
                        "Name": item_name,
                        "SKU": current_sku,
                        "Item_ID": item_id,
                        "Error": "Failed to update SKU"
                    })
            else:
                logger.info(f"Active item with SKU {original_sku} exists - keeping {current_sku} as is")
                    
        except Exception as e:
            logger.error(f"Error processing cleanup for item {item_id}: {str(e)}")
            failed_items.append({
                "Name": item_name,
                "SKU": current_sku,
                "Item_ID": item_id,
                "Error": f"Exception: {str(e)}"
            })
        
        # Update progress
        progress_bar.progress((i + 1) / len(inactive_with_suffix))
        time.sleep(0.5)  # Rate limiting
    
    # Reactivate groups that had items restored
    group_reactivation_results = []
    if groups_to_reactivate:
        logger.info(f"Reactivating {len(groups_to_reactivate)} groups")
        status_text.text("Reactivating groups...")
        
        for group_id in groups_to_reactivate:
            try:
                reactivate_result = mark_item_group_active(
                    st.session_state['api_domain'],
                    st.session_state['access_token'],
                    st.session_state['organization_id'],
                    group_id
                )
                
                if reactivate_result and reactivate_result.get("code") == 0:
                    logger.info(f"Successfully reactivated group {group_id}")
                    group_reactivation_results.append(f"✅ Group {group_id} reactivated")
                else:
                    logger.error(f"Failed to reactivate group {group_id}")
                    group_reactivation_results.append(f"❌ Group {group_id} failed to reactivate")
                    
                time.sleep(0.5)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error reactivating group {group_id}: {str(e)}")
                group_reactivation_results.append(f"❌ Group {group_id} error: {str(e)}")
    
    progress_bar.empty()
    status_text.empty()
    
    logger.info(f"Cleanup complete. Restored: {len(restored_items)}, Failed: {len(failed_items)}, Groups reactivated: {len(groups_to_reactivate)}")
    
    return restored_items, failed_items, group_reactivation_results

# Main Streamlit App
def main():
    st.title("🔄 Advanced Batch Tracking Manager with Group Management")
    st.write("Comprehensive batch tracking removal and cleanup with intelligent group management")
    
    # Create tabs for different operations
    tab1, tab2 = st.tabs(["📦 Remove Batch Tracking", "🧹 Enhanced Cleanup"])
    
    with tab1:
        st.header("Remove Batch Tracking with Group Management")
        st.write("Advanced tool that processes items and manages groups intelligently.")
        
        st.info("""
        📋 **Advanced Process Overview:**
        1. **Fetch Items** - Find all items with batch tracking
        2. **Group Analysis** - For each item, analyze its group
        3. **Group Creation** - Create "No Batch" version of each group
        4. **Group Mapping** - Map old groups to new groups
        5. **Item Processing** - Process each item:
           - Update Original SKU (add '-INACTIVE' suffix)
           - Mark Original as Inactive
           - Create New Active Item in new group
        6. **Group Management** - Mark original groups as inactive
        
        ✅ **Advanced Benefits:**
        - 🏗️ **Group Structure Preserved** - Maintains all variants together
        - 🗺️ **Smart Mapping** - Creates mapping between old and new groups
        - ⚡ **Efficient Processing** - Creates each group only once
        - 📊 **Complete Tracking** - Tracks all group and item changes
        - 🔄 **Batch Processing** - Handles all items in groups properly
        """)
        
        # Initialize session state
        if 'batch_items' not in st.session_state:
            st.session_state.batch_items = None
        if 'processing_complete' not in st.session_state:
            st.session_state.processing_complete = False
        if 'final_results' not in st.session_state:
            st.session_state.final_results = None
        if 'group_mapping' not in st.session_state:
            st.session_state.group_mapping = None
        
        # Fetch items button
        col1, col2 = st.columns([1, 2])
        
        with col1:
            if st.button("🔍 Fetch Items with Batch Tracking", type="primary"):
                st.session_state.batch_items = get_items_with_batch_tracking()
                st.session_state.processing_complete = False
                st.session_state.final_results = None
                st.session_state.group_mapping = None
        
        # Display items if available
        if st.session_state.batch_items is not None:
            if len(st.session_state.batch_items) == 0:
                st.success("🎉 No items with batch tracking found!")
            else:
                st.write(f"**Found {len(st.session_state.batch_items)} items with batch tracking:**")
                
                # Analyze groups
                groups_analysis = {}
                for item in st.session_state.batch_items:
                    group_id = item.get('group_id')
                    group_name = item.get('group_name', 'No Group')
                    
                    if group_id:
                        if group_id not in groups_analysis:
                            groups_analysis[group_id] = {
                                'name': group_name,
                                'items': []
                            }
                        groups_analysis[group_id]['items'].append(item)
                
                # Show group analysis
                if groups_analysis:
                    st.write(f"**Group Analysis: {len(groups_analysis)} unique groups found**")
                    for group_id, group_info in groups_analysis.items():
                        st.write(f"- **{group_info['name']}** (ID: {group_id}): {len(group_info['items'])} items")
                
                # Create DataFrame for display
                display_data = []
                for item in st.session_state.batch_items:
                    display_data.append({
                        "Item ID": item["item_id"],
                        "SKU": item.get("sku", "N/A"),
                        "Name": item["name"],
                        "Group": item.get("group_name", "No Group"),
                        "Stock": item.get("stock_on_hand", 0),
                        "Rate": item.get("rate", 0),
                        "Brand": item.get("brand", "N/A"),
                        "Track Batch": "✅" if item.get("track_batch_number", False) else "❌"
                    })
                
                df = pd.DataFrame(display_data)
                st.dataframe(df, use_container_width=True)
                
                # Process all items button
                st.write("---")
                
                col1, col2, col3 = st.columns([1, 2, 1])
                with col2:
                    if st.button(
                        f"🚀 Process All {len(st.session_state.batch_items)} Items with Groups", 
                        type="primary",
                        use_container_width=True
                    ):
                        st.write("### 🔄 Processing All Items with Group Management...")
                        
                        # Process all items with group management
                        results, group_mapping = process_all_items_with_group_management(st.session_state.batch_items)
                        
                        # Store final results
                        st.session_state.final_results = results
                        st.session_state.group_mapping = group_mapping
                        st.session_state.processing_complete = True
                        
                        # Show final summary
                        success_count = len([r for r in results if "✅ Success" in r["Status"]])
                        
                        if success_count == len(results):
                            st.success(f"🎉 All {len(results)} items processed successfully!")
                        elif success_count > 0:
                            st.warning(f"⚠️ {success_count}/{len(results)} items processed successfully.")
                        else:
                            st.error("❌ No items were processed successfully.")
                        
                        # Show group mapping
                        if group_mapping:
                            st.write("### 🗺️ Group Mapping Created:")
                            mapping_data = []
                            for old_id, new_id in group_mapping.items():
                                mapping_data.append({
                                    "Original Group ID": old_id,
                                    "New Group ID": new_id,
                                    "Status": "✅ Mapped"
                                })
                            mapping_df = pd.DataFrame(mapping_data)
                            st.dataframe(mapping_df, use_container_width=True)
        
        # Show final results if processing is complete
        if st.session_state.processing_complete and st.session_state.final_results:
            st.write("---")
            st.write("## 📊 Final Processing Results")
            
            results_df = pd.DataFrame(st.session_state.final_results)
            st.dataframe(results_df, use_container_width=True)
            
            # Final summary
            success_count = len([r for r in st.session_state.final_results if "✅ Success" in r["Status"]])
            failed_count = len(st.session_state.final_results) - success_count
            groups_created = len(st.session_state.group_mapping) if st.session_state.group_mapping else 0
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Items", len(st.session_state.final_results))
            with col2:
                st.metric("✅ Successful", success_count)
            with col3:
                st.metric("❌ Failed", failed_count)
            with col4:
                st.metric("🏗️ Groups Created", groups_created)
            
            # Download results as CSV
            csv = results_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Results CSV",
                data=csv,
                file_name=f"batch_tracking_removal_results_{int(time.time())}.csv",
                mime="text/csv",
                use_container_width=True
            )
            
            # Reset button
            if st.button("🔄 Process New Items", type="secondary"):
                st.session_state.batch_items = None
                st.session_state.processing_complete = False
                st.session_state.final_results = None
                st.session_state.group_mapping = None
                st.rerun()
    
    with tab2:
        st.header("Enhanced Cleanup with Group Reactivation")
        st.write("Advanced cleanup that handles both item restoration and group reactivation.")
        
        st.info("""
        📋 **Enhanced Cleanup Process:**
        1. **Find Inactive Items** - With '-INACTIVE' suffix in SKU
        2. **Check Active Items** - See if original SKU exists as active item
        3. **Restore SKU** - If no active item exists, remove '-INACTIVE' suffix
        4. **Group Analysis** - Check if restored item belongs to a group
        5. **Group Reactivation** - Reactivate groups that have restored items
        6. **Complete Tracking** - Track all item and group changes
        
        **Example Workflow:**
        - Inactive Item: "ABC123-INACTIVE" in inactive group
        - Check: Does active "ABC123" exist?
        - If NO: 
          - Rename to "ABC123"
          - Reactivate the group it belongs to
        - If YES: Leave as "ABC123-INACTIVE"
        """)
        
        # Initialize session state for cleanup
        if 'cleanup_results' not in st.session_state:
            st.session_state.cleanup_results = None
        
        if st.button("🧹 Start Enhanced Cleanup Process", type="primary"):
            logger.info("Starting enhanced cleanup process with group management")
            
            # Run enhanced cleanup
            restored_items, failed_items, group_results = cleanup_inactive_items_with_groups()
            
            # Store results
            st.session_state.cleanup_results = {
                'restored': restored_items,
                'failed': failed_items,
                'groups': group_results
            }
            
            # Display summary
            st.write("## 🧹 Enhanced Cleanup Summary")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("✅ SKUs Restored", len(restored_items))
            with col2:
                st.metric("❌ Failed", len(failed_items))
            with col3:
                st.metric("🏗️ Groups Processed", len(group_results))
            
            # Show restored items
            if restored_items:
                st.write("### ✅ Successfully Restored Items")
                restored_df = pd.DataFrame(restored_items)
                st.dataframe(restored_df, use_container_width=True)
                
                st.write("**Restored SKUs:**")
                for item in restored_items:
                    st.write(f"- {item['Old_SKU']} → {item['New_SKU']} ({item['Name']}) - Group: {item['Group_ID']}")
            
            # Show group reactivation results
            if group_results:
                st.write("### 🏗️ Group Reactivation Results")
                for result in group_results:
                    if "✅" in result:
                        st.success(result)
                    else:
                        st.error(result)
            
            # Show failed items
            if failed_items:
                st.write("### ❌ Failed Items")
                failed_df = pd.DataFrame(failed_items)
                st.dataframe(failed_df, use_container_width=True)
                
                st.write("**Failed SKUs:**")
                for item in failed_items:
                    st.write(f"- {item['SKU']} ({item['Name']}) - {item['Error']}")
            
            # Download results
            if restored_items or failed_items:
                all_cleanup_results = []
                
                # Add restored items
                for item in restored_items:
                    all_cleanup_results.append({
                        "Name": item["Name"],
                        "Old_SKU": item["Old_SKU"],
                        "New_SKU": item["New_SKU"],
                        "Item_ID": item["Item_ID"],
                        "Group_ID": item["Group_ID"],
                        "Status": "Restored",
                        "Error": ""
                    })
                
                # Add failed items
                for item in failed_items:
                    all_cleanup_results.append({
                        "Name": item["Name"],
                        "Old_SKU": item["SKU"],
                        "New_SKU": "",
                        "Item_ID": item["Item_ID"],
                        "Group_ID": "",
                        "Status": "Failed",
                        "Error": item["Error"]
                    })
                
                cleanup_results_df = pd.DataFrame(all_cleanup_results)
                csv = cleanup_results_df.to_csv(index=False)
                
                col1, col2 = st.columns(2)
                with col1:
                    st.download_button(
                        label="📥 Download Cleanup Results CSV",
                        data=csv,
                        file_name=f"enhanced_cleanup_results_{int(time.time())}.csv",
                        mime="text/csv"
                    )
                
                with col2:
                    # Create group results CSV
                    if group_results:
                        group_csv_data = []
                        for result in group_results:
                            status = "Success" if "✅" in result else "Failed"
                            group_csv_data.append({
                                "Group_Result": result,
                                "Status": status
                            })
                        
                        group_df = pd.DataFrame(group_csv_data)
                        group_csv = group_df.to_csv(index=False)
                        
                        st.download_button(
                            label="📥 Download Group Results CSV",
                            data=group_csv,
                            file_name=f"group_reactivation_results_{int(time.time())}.csv",
                            mime="text/csv"
                        )
            
            logger.info("Enhanced cleanup process completed")

if __name__ == "__main__":
    main()