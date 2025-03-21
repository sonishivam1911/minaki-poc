import pandas as pd
from datetime import datetime
import re
from server.invoice.main import InvoiceProcessor
from utils.zakya_api import post_record_to_zakya
from utils.postgres_connector import crud
from config.logger import logger


class PerniaInvoiceProcessor(InvoiceProcessor):
    """Invoice processor for Pernia vendor."""
    
    def __init__(self, sales_df, invoice_date, zakya_connection_object, customer_name):
        """Initialize with Pernia-specific parameters."""
        super().__init__(sales_df, invoice_date, zakya_connection_object)
        self.customer_name = customer_name
        self.processed_po_numbers = []  # Track PO numbers that were included in invoice
    
    def get_sku_field_name(self):
        """Return the field name for SKU in Pernia dataframe."""
        return "Vendor Code"  # Using the column name from Pernia data structure
    
    def preprocess_data_sync(self):
        """Preprocess Pernia sales data."""
        logger.debug(f"Starting preprocessing of {len(self.sales_df)} Pernia records")
        
        # Filter rows where SKU Code is not null
        # sku_column_name = self.get_sku_field_name()
        self.sales_df = self.sales_df[self.sales_df["SKU Code"].notnull()]
        logger.debug(f"After filtering null SKUs: {len(self.sales_df)} records remaining")
        
        # Convert PO Date to datetime if it's a string
        if "PO Date" in self.sales_df.columns and self.sales_df["PO Date"].dtype == 'object':
            self.sales_df["PO Date"] = self.sales_df["PO Date"].apply(self.convert_date_format)
        
        # Filter based on Product Status - must be "Received" or "QC Pass"
        valid_statuses = ["Received and QC Pass"]
        self.sales_df = self.sales_df[self.sales_df["Product Status"].isin(valid_statuses)]
        logger.debug(f"After filtering by status: {len(self.sales_df)} records remaining with status in {valid_statuses}")
        
        # Check if there are any records left after filtering
        if len(self.sales_df) == 0:
            logger.warning("No records left after preprocessing filters. Check data or filter criteria.")
    
    def convert_date_format(self, date_str):
        """Convert date string to datetime object."""
        if not date_str or pd.isna(date_str):
            return None
            
        # If already a datetime object, return as is
        if isinstance(date_str, datetime):
            return date_str
            
        # If already in YYYY-MM-DD format, parse directly
        if isinstance(date_str, str) and re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            return datetime.strptime(date_str, "%Y-%m-%d")
        
        try:
            # Try parsing as "January 04, 2023" format
            return datetime.strptime(date_str, "%B %d, %Y")
        except ValueError:
            try:
                # Try parsing as "Jan 04, 2023" format (abbreviated month)
                return datetime.strptime(date_str, "%b %d, %Y")
            except ValueError:
                    logger.warning(f"Could not parse date: {date_str}")
                    return None
                    
    
    async def preprocess_data(self):
        """No additional async preprocessing needed for Pernia."""
        pass
    
    def save_po_invoice_mapping(self, invoice_id, invoice_number):
        """Save mapping between PO numbers and invoice ID in database."""
        if not self.processed_po_numbers or not invoice_id:
            logger.warning("No PO numbers to map or missing invoice ID")
            return
            
        try:
            # Prepare mapping records
            mapping_records = []
            for po_number in self.processed_po_numbers:
                mapping_records.append({
                    "po_number": po_number,
                    "invoice_id": invoice_id,
                    "invoice_number": invoice_number,
                    "created_at": datetime.now(),
                    "customer_name": self.customer_name
                })
                
            # Insert records into mapping table
            if mapping_records:
                crud.create_insert_statements(pd.DataFrame.from_records(mapping_records),"pernia_invoice_mapping")
                logger.debug(f"Saved {len(mapping_records)} PO-Invoice mappings")
        except Exception as e:
            logger.error(f"Error saving PO-Invoice mappings: {e}")
    
    async def create_invoices(self, invoice_object):
        """Create a single invoice for specified customer for Pernia."""
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
        
        # Create line items for invoice
        line_items = []
        self.processed_po_numbers = []  # Reset the list
        sku_field_name =self.get_sku_field_name()
        salesorder_product_mapping_dict = super().fetch_item_id_sales_order_mapping()
        
        for _, row in self.sales_df.iterrows():
            try:
                sku = row.get(sku_field_name, "").strip()
                vendor_sku = row.get("SKU Code")
                designer_name = row.get("Designer Name", "")
                po_number = str(row.get("PO Number", ""))
                po_value = float(row.get("PO Value", 0))
                po_type = row.get("PO Type", "")
                product_status = row.get("Product Status", "")
                
                # Create a meaningful description
                item_description = f"{designer_name} - {po_type} - {sku} - {vendor_sku}" if designer_name and po_type else "Pernia Order"
                
                # Skip empty rows
                if not sku or po_value <= 0:
                    continue
                    
                # Prepare line item
                line_item = {
                    "name": item_description,
                    "description": f"PO: {po_number} - {sku} - {product_status}",
                    "rate": po_value,
                    "quantity": 1,  # Assuming each PO is a single unit/item
                    "hsn_or_sac": "711790"  # Default HSN code - same as Aza
                }
                
                # Check if this SKU exists and add item_id only if it does
                if sku in invoice_object.get('existing_sku_item_id_mapping', {}):
                    line_item["item_id"] = invoice_object['existing_sku_item_id_mapping'][sku]
                    
                    if line_item["item_id"] in salesorder_product_mapping_dict:
                        logger.debug(f"Salesorder item id is : {salesorder_product_mapping_dict[line_item["item_id"]]}")
                        line_item["salesorder_item_id"] = salesorder_product_mapping_dict[line_item["item_id"]]                    
                
                line_items.append(line_item)
                self.processed_po_numbers.append(po_number)  # Add to tracking list
                
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
            "payment_terms": 30,  # 30-day payment terms, same as Aza
            "exchange_rate": 1.0,
            "line_items": line_items,
            "gst_treatment": "business_gst",
            "is_inclusive_tax": True,
            "template_id": 1923531000000916001  # Using the same template ID as Aza
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
                invoice_id = invoice_data.get("invoice_id")
                invoice_number = invoice_data.get("invoice_number")
                total_amount = sum(item["rate"] * item["quantity"] for item in line_items)
                
                # Save mapping between PO numbers and invoice ID
                self.save_po_invoice_mapping(invoice_id, invoice_number)
                
                return pd.DataFrame([{
                    "invoice_id": invoice_id,
                    "invoice_number": invoice_number,
                    "customer_name": self.customer_name,
                    "date": invoice_payload["date"],
                    "due_date": invoice_data.get("due_date"),
                    "amount": total_amount,
                    "po_count": len(self.processed_po_numbers),
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