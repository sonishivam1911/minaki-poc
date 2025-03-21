
import pandas as pd
from server.invoice.main import InvoiceProcessor
from utils.zakya_api import post_record_to_zakya
from config.logger import logger


class AzaInvoiceProcessor(InvoiceProcessor):
    """Invoice processor for Aza vendor."""
    
    def __init__(self, sales_df, invoice_date, zakya_connection_object, customer_name):
        """Initialize with Aza-specific parameters."""
        super().__init__(sales_df, invoice_date, zakya_connection_object)
        self.customer_name = customer_name
    
    def get_sku_field_name(self):
        """Return the field name for SKU in Aza dataframe."""
        return "SKU"
    
    def preprocess_data_sync(self):
        """Preprocess Aza sales data."""
        # Filter rows where Code2 is not null
        self.sales_df = self.sales_df[self.sales_df["SKU"].notnull()]
    
    async def preprocess_data(self):
        """No additional async preprocessing needed for Aza."""
        pass
    
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
        salesorder_product_mapping_dict = super().fetch_item_id_sales_order_mapping()
        
        # Create line items for invoice
        line_items = []
        for _, row in self.sales_df.iterrows():
            try:
                sku = row.get("SKU", "").strip()
                item_description = row.get("Item Description", "")
                quantity = int(row.get("Qty", 0))
                total = float(row.get("Total", 0))
                
                # Skip empty rows
                if not sku or quantity <= 0:
                    continue
                    
                # Prepare line item
                line_item = {
                    "name": item_description,
                    "description": f"{sku} - {item_description}",
                    "rate": total,
                    "quantity": quantity,
                    "hsn_or_sac": "711790"  # Default HSN code
                }
                
                # Check if this SKU exists and add item_id only if it does
                if sku in invoice_object.get('existing_sku_item_id_mapping', {}):
                    line_item["item_id"] = invoice_object['existing_sku_item_id_mapping'][sku]

                    if line_item["item_id"] in salesorder_product_mapping_dict:
                        logger.debug(f"Salesorder item id is : {salesorder_product_mapping_dict[line_item["item_id"]]}")
                        line_item["salesorder_item_id"] = salesorder_product_mapping_dict[line_item["item_id"]]               
                
                line_items.append(line_item)
                
            except Exception as e:
                logger.error(f"Error processing row: {e}")
                continue
        
        if not line_items:
            logger.warning(f"No valid line items for customer: {self.customer_name}")
            return pd.DataFrame([{
                "customer_name": self.customer_name,
                "status": "Failed",
                "error": "No valid line items"
            }])
        
        # Create invoice payload
        invoice_payload = {
            "customer_id": customer_id,
            "date": invoice_object['invoice_date'].strftime("%Y-%m-%d"),
            "payment_terms": 30,
            "exchange_rate": 1.0,
            "line_items": line_items,
            "gst_treatment": "business_gst",
            "is_inclusive_tax" : True,
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
                
                return pd.DataFrame([{
                    "invoice_id": invoice_data.get("invoice_id"),
                    "invoice_number": invoice_data.get("invoice_number"),
                    "customer_name": self.customer_name,
                    "date": invoice_payload["date"],
                    "due_date": invoice_data.get("due_date"),
                    "amount": total_amount,
                    "status": "Success"
                }])
            else:
                logger.error(f"Invalid invoice response for {self.customer_name}: {invoice_response}")
                return pd.DataFrame([{
                    "customer_name": self.customer_name,
                    "date": invoice_payload["date"],
                    "status": "Failed",
                    "error": str(invoice_response)
                }])
        except Exception as e:
            logger.error(f"Error creating invoice for {self.customer_name}: {e}")
            return pd.DataFrame([{
                "customer_name": self.customer_name,
                "date": invoice_payload["date"],
                "status": "Failed",
                "error": str(e)
            }])
