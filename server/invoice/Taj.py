
import math
import pandas as pd
from collections import defaultdict
from server.invoice.main import InvoiceProcessor
from utils.zakya_api import post_record_to_zakya
from config.logger import logger

class TajInvoiceProcessor(InvoiceProcessor):
    """Invoice processor for Taj vendor."""
    
    def get_sku_field_name(self):
        """Return the field name for SKU in Taj dataframe."""
        return "Style"
    
    def preprocess_data_sync(self):
        """Preprocess Taj sales data."""
        self.sales_df["Style"] = self.sales_df["Style"].astype(str) 
        self.sales_df['Rounded_Total'] = self.sales_df['Total'].apply(
            lambda x: math.ceil(x) if x - int(x) >= 0.5 else math.floor(x)
        )
    
    async def preprocess_data(self):
        """No additional async preprocessing needed for Taj."""
        pass
    
    async def create_invoices(self, invoice_object):
        """Create invoices grouped by branch name for Taj."""
        # Group by branch name
        branch_to_customer_map = {}
        branch_to_invoice_payload = defaultdict(lambda: {"line_items": []})
        
        for _, row in self.sales_df.iterrows():
            try:
                sku = row.get("Style", "").strip()
                branch_name = row.get("Branch Name", "").strip()
                quantity = int(row.get("Qty", 0))
                total = math.ceil(row.get("Total", 0))
                prod_name = row.get("PrintName", "")
                
                # Skip empty rows
                if not branch_name or quantity <= 0:
                    continue
                    
                # Get customer data if not already cached
                if branch_name not in branch_to_customer_map:
                    customer_data = await self.find_customer_by_branch(branch_name)
                    
                    if len(customer_data) == 0 or "error" in customer_data:
                        logger.error(f"Customer not found for branch: {branch_name}")
                        continue
                        
                    # Cache customer data
                    branch_to_customer_map[branch_name] = {
                        "customer_id": customer_data[0]["contact_id"],
                        "gst": customer_data[0].get("gst_no", ""),
                        "invbr": customer_data[0].get("contact_number", ""),
                        "place_of_contact": customer_data[0].get("place_of_contact", "")
                    }
                
                # Prepare line item for invoice
                line_item = {
                    "name": prod_name,
                    "description": f"{sku} - {prod_name}",
                    "rate": total,
                    "quantity": quantity
                }
                
                # Check if this SKU exists and add item_id only if it does
                if sku in invoice_object.get('existing_sku_item_id_mapping', {}):
                    line_item["item_id"] = invoice_object['existing_sku_item_id_mapping'][sku]
                
                # Add line item to the invoice for this branch
                branch_to_invoice_payload[branch_name]["line_items"].append(line_item)
                
            except Exception as e:
                logger.error(f"Error processing row: {e}")
                continue
        
        # Create invoices (one per branch)
        invoice_summary = []
        
        for branch_name, data in branch_to_invoice_payload.items():
            if not data["line_items"]:
                logger.warning(f"No line items for branch {branch_name}, skipping invoice creation")
                continue
                
            customer_data = branch_to_customer_map[branch_name]
            
            # Create invoice payload
            invoice_payload = {
                "customer_id": int(customer_data["customer_id"]),
                "date": invoice_object['invoice_date'].strftime("%Y-%m-%d"),
                "payment_terms": 30,
                "exchange_rate": 1.0,
                "line_items": data["line_items"],
                "gst_treatment": "business_gst",
                "template_id": 1923531000000916001  # Hardcoded template ID
            }
            
            # Add GST number if available
            if customer_data.get("gst"):
                invoice_payload["gst_no"] = customer_data["gst"]
            
            try:
                logger.debug(f"Creating invoice for {branch_name} with {len(invoice_payload['line_items'])} items")
                invoice_response = post_record_to_zakya(
                    self.zakya_connection_object['base_url'],
                    self.zakya_connection_object['access_token'],
                    self.zakya_connection_object['organization_id'],
                    'invoices',
                    invoice_payload
                )
                
                if isinstance(invoice_response, dict) and "invoice" in invoice_response:
                    invoice_data = invoice_response["invoice"]
                    total_amount = sum(item["rate"] * item["quantity"] for item in data["line_items"])
                    
                    invoice_summary.append({
                        "invoice_id": invoice_data.get("invoice_id"),
                        "invoice_number": invoice_data.get("invoice_number"),
                        "customer_name": branch_name,
                        "date": invoice_payload["date"],
                        "due_date": invoice_data.get("due_date"),
                        "amount": total_amount,
                        "status": "Success"
                    })
                    logger.info(f"Successfully created invoice for {branch_name}: {invoice_data.get('invoice_number')}")
                else:
                    logger.error(f"Invalid invoice response for {branch_name}: {invoice_response}")
                    invoice_summary.append({
                        "customer_name": branch_name,
                        "date": invoice_payload["date"],
                        "status": "Failed",
                        "error": str(invoice_response)
                    })
            except Exception as e:
                logger.error(f"Error creating invoice for {branch_name}: {e}")
                invoice_summary.append({
                    "customer_name": branch_name,
                    "date": invoice_payload["date"],
                    "status": "Failed",
                    "error": str(e)
                })
        
        return pd.DataFrame(invoice_summary) if invoice_summary else pd.DataFrame()