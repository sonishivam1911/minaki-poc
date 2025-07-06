
import pandas as pd
import requests
import time
import os
import sys
from dotenv import load_dotenv
from datetime import datetime
from typing import List, Dict, Optional

# Add current working directory to Python path so it can find config/ and utils/
current_working_dir = os.getcwd()
if current_working_dir not in sys.path:
    sys.path.insert(0, current_working_dir)

# Import YOUR existing modules
from config.logger import logger  # noqa: E402
from utils.zakya_api import get_access_token, fetch_organizations  
from utils.postgres_connector import crud

class BatchTrackingRemover:
    def __init__(self, api_domain: str, access_token: str, organization_id: str):
        """Initialize the batch tracking remover with API credentials."""
        self.api_domain = api_domain
        self.access_token = access_token
        self.organization_id = organization_id
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f"Zoho-oauthtoken {access_token}",
            'Content-Type': 'application/json'
        })
        
        logger.info("Initialized BatchTrackingRemover with API credentials")
    
    def fetch_records_from_zakya(self, endpoint: str) -> Optional[Dict]:
        """Fetch records from Zakya API."""
        url = f"{self.api_domain}inventory/v1{endpoint}"
        params = {'organization_id': self.organization_id}
        
        try:
            logger.info(f"Fetching records from endpoint: {endpoint}")
            response = self.session.get(url=url, params=params)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Successfully fetched {len(data.get('items', []))} records")
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching records from {endpoint}: {str(e)}")
            return None
    
    def retrieve_record_from_zakya(self, endpoint: str) -> Optional[Dict]:
        """Retrieve a single record from Zakya API."""
        url = f"{self.api_domain}inventory/v1/{endpoint}"
        params = {'organization_id': self.organization_id}
        
        try:
            logger.info(f"Retrieving record from endpoint: {endpoint}")
            response = self.session.get(url=url, params=params)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Successfully retrieved record")
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"Error retrieving record from {endpoint}: {str(e)}")
            return None
    
    def post_record_to_zakya(self, endpoint: str, payload: Dict) -> Optional[Dict]:
        """Post a record to Zakya API."""
        url = f"{self.api_domain}inventory/v1/{endpoint}"
        params = {'organization_id': self.organization_id}
        
        try:
            logger.info(f"Creating record at endpoint: {endpoint}")
            response = self.session.post(url=url, params=params, json=payload)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Successfully created record")
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"Error creating record at {endpoint}: {str(e)}")
            logger.error(f"Response: {response.text if 'response' in locals() else 'No response'}")
            return None
    
    def update_item_sku(self, item_id: str, new_sku: str) -> Optional[Dict]:
        """Update an item's SKU in Zakya API."""
        url = f"{self.api_domain}inventory/v1/items/{item_id}"
        params = {'organization_id': self.organization_id}
        payload = {'sku': new_sku}
        
        try:
            logger.info(f"Updating SKU for item {item_id} to: {new_sku}")
            response = self.session.put(url=url, params=params, json=payload)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Successfully updated SKU for item {item_id}")
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"Error updating SKU for item {item_id}: {str(e)}")
            return None
    
    def mark_item_inactive(self, item_id: str) -> Optional[Dict]:
        """Mark an item as inactive in Zakya API."""
        url = f"{self.api_domain}inventory/v1/items/{item_id}/inactive"
        params = {'organization_id': self.organization_id}
        
        try:
            logger.info(f"Marking item {item_id} as inactive")
            response = self.session.post(url=url, params=params)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Successfully marked item {item_id} as inactive")
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"Error marking item {item_id} as inactive: {str(e)}")
            return None
    
    def get_items_with_batch_tracking(self) -> List[Dict]:
        """Fetch all items and filter those with batch tracking enabled."""
        logger.info("Starting to fetch items with batch tracking")
        
        # Fetch all items
        all_items_data = self.fetch_records_from_zakya("/items")
        if not all_items_data:
            logger.error("Failed to fetch items data")
            return []
        
        all_items = all_items_data.get('items', [])
        logger.info(f"Total items fetched: {len(all_items)}")
        
        # Filter items that have track_batch_number = True
        batch_tracked_items = []
        for item in all_items:
            if item.get('track_batch_number', False):
                batch_tracked_items.append(item)
        
        logger.info(f"Found {len(batch_tracked_items)} items with batch tracking enabled")
        return batch_tracked_items
    
    def create_new_item_without_batch_tracking(self, original_item: Dict) -> Dict:
        """Create payload for new item without batch tracking."""
        logger.info(f"Creating payload for new item based on: {original_item['name']}")
        
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
        
        # Add ALL attribute fields (crucial for group items)
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
        
        logger.info(f"Created payload for new item with {len(new_item_payload)} fields")
        return new_item_payload
    
    def process_single_item(self, item_id: str) -> Dict:
        """Process a single item to remove batch tracking."""
        logger.info(f"Starting to process item: {item_id}")
        
        result = {
            "item_id": item_id,
            "original_sku": "N/A",
            "new_sku": "N/A",
            "name": "N/A",
            "original_id": item_id,
            "new_id": "N/A",
            "status": "Processing",
            "error_message": "",
            "processing_time": datetime.now().isoformat(),
            "steps_completed": []
        }
        
        try:
            # Step 1: Fetch detailed item info
            logger.info(f"Step 1: Fetching detailed info for item {item_id}")
            detailed_item = self.retrieve_record_from_zakya(f"items/{item_id}")
            
            if not detailed_item or 'item' not in detailed_item:
                error_msg = "Failed to fetch item details"
                logger.error(f"Item {item_id}: {error_msg}")
                result.update({
                    "status": "❌ Failed",
                    "error_message": error_msg
                })
                return result
            
            item = detailed_item['item']
            original_sku = item.get('sku', '')
            result.update({
                "original_sku": original_sku,
                "new_sku": original_sku,
                "name": item["name"]
            })
            result["steps_completed"].append("✅ Fetched item details")
            
            logger.info(f"Processing item: {item['name']} (SKU: {original_sku})")
            
            # Step 2: Update SKU of original item to avoid conflicts
            inactive_sku = f"{original_sku}-INACTIVE" if original_sku else f"INACTIVE-{item_id}"
            logger.info(f"Step 2: Updating original item SKU to: {inactive_sku}")
            
            sku_update_result = self.update_item_sku(item["item_id"], inactive_sku)
            
            if not sku_update_result or sku_update_result.get("code") != 0:
                error_msg = "Failed to update original item SKU"
                logger.error(f"Item {item_id}: {error_msg}")
                result.update({
                    "status": "❌ Failed",
                    "error_message": error_msg
                })
                return result
            
            result["steps_completed"].append("✅ Updated original SKU")
            logger.info(f"Successfully updated original item SKU to: {inactive_sku}")
            time.sleep(1)  # Rate limiting
            
            # Step 3: Mark original item as inactive
            logger.info(f"Step 3: Marking item {item_id} as inactive")
            inactive_result = self.mark_item_inactive(item["item_id"])
            
            if not inactive_result or inactive_result.get("code") != 0:
                error_msg = "Failed to mark item as inactive"
                logger.error(f"Item {item_id}: {error_msg}")
                result.update({
                    "status": "❌ Failed",
                    "error_message": error_msg
                })
                return result
            
            result["steps_completed"].append("✅ Marked original as inactive")
            logger.info(f"Successfully marked item {item_id} as inactive")
            time.sleep(1)  # Rate limiting
            
            # Step 4: Create new item payload with original SKU
            logger.info(f"Step 4: Creating new item payload")
            new_item_payload = self.create_new_item_without_batch_tracking(item)
            new_item_payload['sku'] = original_sku  # Use original SKU for new item
            
            # Step 5: Create new item without batch tracking
            logger.info(f"Step 5: Creating new item with original SKU: {original_sku}")
            create_result = self.post_record_to_zakya("items", new_item_payload)
            
            if create_result and create_result.get("code") == 0:
                new_item_id = create_result["item"]["item_id"]
                result.update({
                    "new_id": new_item_id,
                    "status": "✅ Success",
                    "error_message": ""
                })
                result["steps_completed"].append("✅ Created new item")
                logger.info(f"Successfully created new item {new_item_id} with SKU: {original_sku}")
            else:
                error_msg = "Failed to create new item"
                logger.error(f"Item {item_id}: {error_msg}")
                result.update({
                    "status": "❌ Failed",
                    "error_message": error_msg
                })
                return result
                
        except Exception as e:
            error_msg = f"Exception occurred: {str(e)}"
            logger.error(f"Item {item_id}: {error_msg}")
            result.update({
                "status": "❌ Error",
                "error_message": error_msg
            })
        
        result["steps_completed_count"] = len(result["steps_completed"])
        logger.info(f"Completed processing item {item_id} with status: {result['status']}")
        return result
    
    def process_items(self, item_ids: List[str] = None, process_all: bool = False) -> List[Dict]:
        """Process multiple items to remove batch tracking."""
        logger.info("Starting batch processing of items")
        
        if process_all:
            logger.info("Processing all items with batch tracking")
            items_with_batch = self.get_items_with_batch_tracking()
            item_ids = [item["item_id"] for item in items_with_batch]
        elif not item_ids:
            logger.warning("No item IDs provided for processing")
            return []
        
        logger.info(f"Processing {len(item_ids)} items")
        results = []
        
        for i, item_id in enumerate(item_ids, 1):
            logger.info(f"Processing item {i}/{len(item_ids)}: {item_id}")
            result = self.process_single_item(str(item_id))
            results.append(result)
            
            # Rate limiting between items
            if i < len(item_ids):
                time.sleep(2)
        
        logger.info(f"Completed processing {len(results)} items")
        return results
    
    def save_results_to_csv(self, results: List[Dict], filename: str = None) -> str:
        """Save processing results to CSV file."""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"batch_tracking_removal_results_{timestamp}.csv"
        
        logger.info(f"Saving results to CSV file: {filename}")
        
        # Flatten the results for CSV
        flattened_results = []
        for result in results:
            flattened_result = {
                "Item ID": result["item_id"],
                "Original SKU": result["original_sku"],
                "New SKU": result["new_sku"],
                "Name": result["name"],
                "Original ID": result["original_id"],
                "New ID": result["new_id"],
                "Status": result["status"],
                "Error Message": result["error_message"],
                "Processing Time": result["processing_time"],
                "Steps Completed": " | ".join(result["steps_completed"]),
                "Steps Count": result.get("steps_completed_count", 0)
            }
            flattened_results.append(flattened_result)
        
        df = pd.DataFrame(flattened_results)
        df.to_csv(filename, index=False)
        
        # Generate summary statistics
        total_items = len(results)
        success_count = len([r for r in results if "✅ Success" in r["status"]])
        failed_count = len([r for r in results if "❌" in r["status"]])
        
        logger.info(f"Results saved to {filename}")
        logger.info(f"Summary - Total: {total_items}, Success: {success_count}, Failed: {failed_count}")
        
        return filename


def get_config_from_database():
    """Get configuration values from database and API, similar to the Streamlit app."""
    try:
        logger.info("Loading configuration from database...")
        
        # Get authentication data from database
        zakya_auth_df = crud.read_table("zakya_auth")
        if isinstance(zakya_auth_df, pd.DataFrame):
            zakya_auth_df = zakya_auth_df[zakya_auth_df['env'] == os.getenv('env')]
        else:
            logger.error(f"Issue with database connection: {zakya_auth_df}")
            return None
        
        if zakya_auth_df.empty:
            logger.error("No authentication data found in database")
            return None
        
        # Get refresh token and generate access token
        refresh_token = zakya_auth_df["refresh_token"].iloc[0]
        logger.info("Refreshing access token...")
        
        refresh_token_data = get_access_token(refresh_token=refresh_token)
        
        if 'access_token' not in refresh_token_data:
            logger.error(f"Failed to get access token from refresh token: {refresh_token_data}")
            return None
        
        access_token = refresh_token_data['access_token']
        api_domain = 'https://api.zakya.in/'
        
        # Get organization ID
        logger.info("Fetching organization information...")
        org_data = fetch_organizations(access_token)
        
        if not org_data or 'organizations' not in org_data or len(org_data['organizations']) == 0:
            logger.error("No organizations found in response")
            return None
        
        organization_id = org_data['organizations'][0]['organization_id']
        
        config = {
            "api_domain": api_domain,
            "access_token": access_token,
            "organization_id": organization_id
        }
        
        logger.info("Configuration loaded successfully from database")
        return config
        
    except ImportError as e:
        logger.error(f"Failed to import required modules: {e}")
        logger.error("Make sure you have the utils modules available in your Python path")
        return None
    except Exception as e:
        logger.error(f"Error loading configuration from database: {e}")
        return None

def get_manual_config():
    """Manual configuration - use this if database method fails."""
    return {
        "api_domain": "https://api.zakya.in/",  # Replace with your API domain
        "access_token": "your-access-token",  # Replace with your access token
        "organization_id": "your-organization-id"  # Replace with your organization ID
    }

def main():
    """Main function to run the batch tracking removal process."""
    logger.info("Starting Batch Tracking Removal Script")
    
    # Try to get configuration from database first
    CONFIG = get_config_from_database()
    
    if CONFIG is None:
        logger.warning("Failed to load configuration from database, falling back to manual configuration")
        CONFIG = get_manual_config()
        
        # Validate manual configuration
        if any(value.startswith("your-") for value in CONFIG.values()):
            logger.error("Please update the manual configuration with your actual API credentials")
            logger.error("Or ensure your database and utils modules are properly set up")
            return
    
    try:
        # Initialize the batch tracking remover
        remover = BatchTrackingRemover(
            CONFIG["api_domain"],
            CONFIG["access_token"],
            CONFIG["organization_id"]
        )
        
        # Option 1: Process all items with batch tracking
        logger.info("Option: Processing all items with batch tracking")
        results = remover.process_items(process_all=True)
        
        # Option 2: Process specific item IDs (uncomment and modify as needed)
        # specific_item_ids = ["item_id_1", "item_id_2", "item_id_3"]
        # results = remover.process_items(item_ids=specific_item_ids)
        
        # Save results to CSV
        csv_filename = remover.save_results_to_csv(results)
        
        # Print summary
        total_items = len(results)
        success_count = len([r for r in results if "✅ Success" in r["status"]])
        failed_count = len([r for r in results if "❌" in r["status"]])
        
        print(f"\n{'='*60}")
        print("BATCH TRACKING REMOVAL SUMMARY")
        print(f"{'='*60}")
        print(f"Total Items Processed: {total_items}")
        print(f"Successful: {success_count}")
        print(f"Failed: {failed_count}")
        print(f"Success Rate: {(success_count/total_items)*100:.1f}%")
        print(f"Results saved to: {csv_filename}")
        print("Logs saved to: batch_tracking_removal.log")
        print(f"{'='*60}")
        
        # Show failed items if any
        if failed_count > 0:
            print("\nFailed Items:")
            for result in results:
                if "❌" in result["status"]:
                    print(f"- {result['name']} (ID: {result['item_id']}): {result['error_message']}")
        
        logger.info("Script completed successfully")
        
    except Exception as e:
        logger.error(f"Script failed with error: {str(e)}")
        raise


if __name__ == "__main__":
    main()