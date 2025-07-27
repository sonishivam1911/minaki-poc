import pandas as pd
from datetime import datetime
from config.logger import logger
from utils.postgres_connector import crud
from utils.zakya_api import post_record_to_zakya
from server.invoice.main import InvoiceProcessor


class AzaInvoiceProcessor(InvoiceProcessor):
    """Simplified Invoice processor for Aza vendor - no sales order logic."""
    
    def __init__(self, sales_df, invoice_date, zakya_connection_object, customer_name):
        """Initialize with simplified parameters."""
        super().__init__(sales_df, invoice_date, zakya_connection_object)
        self.customer_name = customer_name
    
    def get_sku_field_name(self):
        """Return the field name for SKU in Aza dataframe."""
        return "SKU"
    
    def get_vendor_field_name(self):
        return 'Item#'
    
    def preprocess_data_sync(self):
        """Preprocess Aza sales data."""
        # Filter rows where SKU is not null
        self.sales_df = self.sales_df[self.sales_df["SKU"].notnull()]
    
    async def preprocess_data(self):
        """No additional async preprocessing needed for Aza."""
        pass
    
    def update_inventory(self, inventory_adjustments_needed):
        """Process inventory adjustments if needed."""
        adjustment_results = []
        for adjustment in inventory_adjustments_needed:
            try:
                inv_payload = {
                    "date": datetime.now().strftime("%Y-%m-%d"),
                    "reason": "Stock Retally",
                    "adjustment_type": "quantity",
                    "line_items": [
                        {
                            "item_id": adjustment["item_id"],
                            "quantity_adjusted": adjustment["quantity_adjusted"]
                        }
                    ]
                }
                
                inventory_correction_response = post_record_to_zakya(
                    self.zakya_connection_object['base_url'],
                    self.zakya_connection_object['access_token'],
                    self.zakya_connection_object['organization_id'],
                    'inventoryadjustments',
                    inv_payload   
                )
                
                if "inventory_adjustment" in inventory_correction_response:
                    adjustment_id = inventory_correction_response["inventory_adjustment"].get("inventory_adjustment_id")
                    adjustment_results.append({
                        "item_name": adjustment["item_name"],
                        "adjustment_id": adjustment_id,
                        "quantity_adjusted": adjustment["quantity_adjusted"],
                        "status": "Success"
                    })
                else:
                    adjustment_results.append({
                        "item_name": adjustment["item_name"],
                        "status": "Failed",
                        "error": str(inventory_correction_response)
                    })
                    
            except Exception as e:
                logger.error(f"Error creating inventory adjustment for {adjustment['item_name']}: {e}")
                adjustment_results.append({
                    "item_name": adjustment["item_name"],
                    "status": "Failed",
                    "error": str(e)
                })
        
        return adjustment_results

    async def create_invoices(self, invoice_object):
        """Create a single invoice for specified customer for Aza."""
        # Get customer data
        customer_data = await self.find_customer_by_name(self.customer_name)
        
        if not customer_data or "error" in customer_data or len(customer_data) == 0:
            logger.error(f"Customer not found: {self.customer_name}")
            return pd.DataFrame([{
                "customer_name": self.customer_name,
                "status": "Failed",
                "error": "Customer not found"
            }])
        
        customer_id = customer_data[0]["contact_id"]
        gst = customer_data[0].get("gst_no", "")
        
        # Analyze products first to get mapping
        product_analysis = await self.analyze_uploaded_products()
        product_mapping = product_analysis.get('product_mapping', {})
        
        line_items = []
        inventory_adjustments_needed = []
        
        # Process each row in the sales dataframe
        for _, row in self.sales_df.iterrows():
            try:
                sku = row.get('SKU', '').strip()
                item_description = row.get('Item Description', '').strip()
                po_number = str(row.get('PO No.', ''))
                quantity = 1  # Default quantity
                rate = float(row.get("Total", 0))
                
                # Skip if no rate or invalid data
                if rate <= 0:
                    continue
                
                # Check if SKU is mapped to an item in Zakya
                if sku in product_mapping:
                    # Mapped SKU - use proper item details
                    item_id = product_mapping[sku]
                    
                    line_item = {
                        "name": item_description,
                        "description": f"PO Number: {po_number}",
                        "rate": rate,
                        "quantity": quantity,
                        "item_id": item_id,
                        "hsn_or_sac": "711790"  # Default HSN code
                    }
                    
                    # Check if inventory adjustment is needed
                    stock_on_hand = await self.get_stock_on_hand(item_id)
                    if stock_on_hand < quantity:
                        quantity_adjusted = quantity - stock_on_hand
                        inventory_adjustments_needed.append({
                            "item_id": item_id,
                            "quantity_adjusted": quantity_adjusted,
                            "item_name": item_description
                        })
                else:
                    # Unmapped SKU - create generic line item
                    line_item = {
                        "description": f"Missing SKU from database - SKU: {sku}, Description: {item_description}, PO: {po_number}",
                        "rate": rate,
                        "quantity": quantity,
                        "hsn_or_sac": "711790"  # Default HSN code
                    }
                
                line_items.append(line_item)
                
            except Exception as e:
                logger.error(f"Error processing row: {e}")
                continue
        
        # Process inventory adjustments
        adjustments_result = self.update_inventory(inventory_adjustments_needed)
        
        # Create invoice payload
        invoice_payload = {
            "customer_id": customer_id,
            "date": invoice_object['invoice_date'].strftime("%Y-%m-%d"),
            "payment_terms": 30,
            "exchange_rate": 1.0,
            "line_items": line_items,
            "gst_treatment": "business_gst",
            "is_inclusive_tax": True,
            "template_id": 1923531000000916001  # Hardcoded template ID
        }
        
        # Add GST number if available
        if gst:
            invoice_payload["gst_no"] = gst
        
        try:
            logger.debug(f"Creating invoice for {self.customer_name} with {len(line_items)} items")
            invoice_response = post_record_to_zakya(
                self.zakya_connection_object['base_url'],
                self.zakya_connection_object['access_token'],
                self.zakya_connection_object['organization_id'],
                'invoices',
                invoice_payload
            )
            
            if isinstance(invoice_response, dict) and "invoice" in invoice_response:
                invoice_data = invoice_response["invoice"]
                total_amount = sum(item["rate"] * item["quantity"] for item in line_items)
                
                invoice_df = pd.DataFrame([{
                    "invoice_id": invoice_data.get("invoice_id"),
                    "invoice_number": invoice_data.get("invoice_number"),
                    "customer_name": self.customer_name,
                    "date": invoice_payload["date"],
                    "due_date": invoice_data.get("due_date"),
                    "amount": total_amount,
                    "status": "Success",
                    "mapped_items": len([item for item in line_items if "item_id" in item]),
                    "unmapped_items": len([item for item in line_items if "item_id" not in item]),
                    "inventory_adjustments": len(adjustments_result),
                    "inventory_adjustments_success": sum(1 for adj in adjustments_result if adj["status"] == "Success")
                }])

                return {
                    'invoice_df': invoice_df, 
                    'adjustment_df': pd.DataFrame.from_records(adjustments_result)
                }
            else:
                logger.error(f"Invalid invoice response for {self.customer_name}: {invoice_response}")
                return {
                    'invoice_df': pd.DataFrame([{
                        "customer_name": self.customer_name,
                        "date": invoice_payload["date"],
                        "status": "Failed",
                        "error": str(invoice_response)
                    }]),
                    'adjustment_df': pd.DataFrame()
                }
        except Exception as e:
            logger.error(f"Error creating invoice for {self.customer_name}: {e}")
            return {
                'invoice_df': pd.DataFrame([{
                    "customer_name": self.customer_name,
                    "status": "Failed",
                    "error": str(e)
                }]), 
                'adjustment_df': pd.DataFrame()
            }

    async def get_stock_on_hand(self, item_id):
        """Get stock on hand for a specific item."""
        try:
            zakya_products_df = crud.read_table('zakya_products')
            product_rows = zakya_products_df[zakya_products_df['item_id'] == item_id]
            
            if not product_rows.empty:
                return float(product_rows.iloc[0].get('stock_on_hand', 0))
            return 0
        except Exception as e:
            logger.error(f"Error getting stock for item {item_id}: {e}")
            return 0

    async def analyze_uploaded_products(self):
        """
        Analyze the uploaded Aza Excel data to identify mapped and unmapped products.
        Returns a structured dictionary with analysis results.
        """
        try:
            # Call the base class method to find existing products
            product_config = await self.find_existing_products()
            
            # Format results
            mapped_products = []
            unmapped_products = []
            product_mapping = product_config.get('existing_sku_item_id_mapping', {})
            
            # Process mapped products
            for sku in product_config.get('existing_products', []):
                item_id = product_config['existing_sku_item_id_mapping'].get(sku, None)
                
                if item_id:
                    # Get product details from mapping
                    product_data = product_config['existing_products_data_dict'].get(item_id, [{}])
                    
                    # Find matching row in Aza orders
                    matching_rows = self.sales_df[self.sales_df['SKU'] == sku]
                    po_data = {}
                    
                    if not matching_rows.empty:
                        row = matching_rows.iloc[0]
                        
                        po_data = {
                            'item_description': row.get('Item Description', ''),
                            'item_number': row.get('Item#', ''),
                            'total': row.get('Total', 0),
                            'quantity': row.get('Qty', 1),
                            'po_number': row.get('PO No.', '')
                        }
                    
                    # Combine product and Aza data
                    mapped_products.append({
                        'sku': sku,
                        'item_id': item_id,
                        'item_name': product_data[0].get('item_name', ''),
                        'available_stock': product_data[0].get('available_stock', 0),
                        'stock_on_hand': product_data[0].get('stock_on_hand', 0),
                        **po_data
                    })
            
            # Process unmapped products
            for sku in product_config.get('missing_products', []):
                # Find matching row in Aza orders
                matching_rows = self.sales_df[self.sales_df['SKU'] == sku]
                if not matching_rows.empty:
                    row = matching_rows.iloc[0]
                    unmapped_products.append({
                        'sku': sku,
                        'item_description': row.get('Item Description', ''),
                        'item_number': row.get('Item#', ''),
                        'total': row.get('Total', 0),
                        'quantity': row.get('Qty', 1),
                        'po_number': row.get('PO No.', ''),
                        'error': 'Product not found in Zakya'
                    })
            
            # Return the analysis results
            return {
                'mapped_products': mapped_products,
                'unmapped_products': unmapped_products,
                'product_mapping': product_mapping,
                'raw_results': product_config
            }
        except Exception as e:
            logger.error(f"Error analyzing Aza products: {e}")
            return {
                'mapped_products': [],
                'unmapped_products': [],
                'product_mapping': {},
                'error': str(e)
            }