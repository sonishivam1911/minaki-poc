
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from config.logger import logger
from utils.postgres_connector import crud
from utils.zakya_api import fetch_records_from_zakya, post_record_to_zakya, fetch_object_for_each_id
from core.helper_zakya import extract_record_list
from server.invoice.main import InvoiceProcessor


class AzaInvoiceProcessor(InvoiceProcessor):
    """Invoice processor for Aza vendor."""
    
    def __init__(self, sales_df, invoice_date, zakya_connection_object, customer_name):
        """Initialize with Aza-specific parameters."""
        super().__init__(sales_df, invoice_date, zakya_connection_object)
        self.customer_name = customer_name
    
    def get_sku_field_name(self):
        """Return the field name for SKU in Aza dataframe."""
        return "SKU"
    
    def get_vendor_field_name(self):
        return 'Item#'
    
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
                        #logger.debug(f"Salesorder item id is : {salesorder_product_mapping_dict[line_item["item_id"]]}")
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
            #logger.debug(f"Creating invoice for {self.customer_name} with {len(line_items)} items")
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
                            'quantity': row.get('Qty', 1)
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
                #logger.debug(f"Matched rows : {self.sales_df['SKU']}")
                if not matching_rows.empty:
                    row = matching_rows.iloc[0]
                    unmapped_products.append({
                        'sku': sku,
                        'item_description': row.get('Item Description', ''),
                        'item_number': row.get('Item#', ''),
                        'total': row.get('Total', 0),
                        'quantity': row.get('Qty', 1),
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



    def extract_po_from_reference(self,ref_string):
        """
        Extract PO value from reference string with improved type handling.
        
        Args:
            ref_string: The reference string to extract PO from
            
        Returns:
            str or None: Extracted PO value or None if not found
        """
        # First ensure we have a string to work with
        if ref_string is None:
            return None
            
        # Convert to string if not already a string
        if not isinstance(ref_string, str):
            try:
                ref_string = str(ref_string).strip()
            except:
                return None
        else:
            ref_string = ref_string.strip()
        
        # Return None for empty strings
        if not ref_string:
            return None
        
        # Check if the reference starts with "PO" using various formats
        if ref_string.upper().startswith("PO"):
            # Handle various formats: "PO:", "PO-", "PO ", etc.
            # logger.debug(f"ref_string is : {ref_string.split(':')[-1].strip()}")
            return ref_string.split(':')[-1].strip()
        
        return None

    async def find_existing_aza_salesorders(self, customer_id,include_inventory=True):
        """
        Fetch and analyze existing sales orders for Aza products.
        
        Args:
            customer_id: The Zakya customer ID
            include_inventory: Whether to include inventory data
            
        Returns:
            DataFrame containing sales order information
        """
        try:
            # Fetch sales order data from Zakya
            sales_orders_data = fetch_records_from_zakya(
                self.zakya_connection_object['base_url'],
                self.zakya_connection_object['access_token'],
                self.zakya_connection_object['organization_id'],
                '/salesorders'
            )
            
            # Get sales order line item mapping from the database
            salesorder_item_mapping_df = crud.read_table('salesorder_line_item_mapping')
            
            # Extract sales orders from the response
            all_orders = extract_record_list(sales_orders_data, "salesorders")
            
            # Convert to DataFrame for easier filtering
            sales_orders_df = pd.DataFrame(all_orders)

            # Extract PO values from reference numbers early in the process
            # Process reference numbers to extract PO values
            if 'reference_number' in sales_orders_df.columns:
                # logger.debug(f"Processing reference numbers. Sample types: {sales_orders_df['reference_number'].apply(type).value_counts()}")
                sales_orders_df['extracted_po'] = sales_orders_df['reference_number'].apply(self.extract_po_from_reference)
                # logger.debug(f"Extracted POs: {set(sales_orders_df['extracted_po'])}")

            # Ensure 'PO No.' column exists in aza order that is self.sales_orders
            if 'PO No.' in self.sales_df.columns:
                logger.debug(f"POs column is  : {self.sales_df.columns}")
                # Check for type consistency in PO Number column
                if not self.sales_df['PO No.'].empty:
                    logger.debug(f"Aza PO Number types: {self.sales_df['PO No.'].apply(type).value_counts()}")
                    
                    # Convert PO Number to string for consistency
                    self.sales_df['PO No.'] = self.sales_df['PO No.'].apply(
                        lambda x: str(x).strip() if pd.notna(x) else None
                    )
                

            # Join sales orders with aza orders based on matching PO numbers
            mapped_sales_order_with_product_df = pd.merge(
                left=sales_orders_df,
                right=self.sales_df,
                how='right',  # Only include matches
                left_on=['extracted_po'],
                right_on=['PO No.']
            )

            logger.debug(f"Mapped Sales Order is : {mapped_sales_order_with_product_df}")
            # Filter to only include the selected customer
            mapped_sales_order_with_product_df = mapped_sales_order_with_product_df[mapped_sales_order_with_product_df['customer_id'] == customer_id]

            start_date = self.invoice_date

            date_45_days_before = start_date - timedelta(days=60)
            
            # Convert dates for comparison
            start_date_str = start_date.strftime('%Y-%m-%d')
            date_45_days_before_str = date_45_days_before.strftime('%Y-%m-%d')
            
            # Filter sales orders to only include those between 45 days before start_date and start_date
            mapped_sales_order_with_product_df = mapped_sales_order_with_product_df[
                (mapped_sales_order_with_product_df['date'] >= date_45_days_before_str) & 
                (mapped_sales_order_with_product_df['date'] <= start_date_str)
            ]            

            logger.debug(f"Mapped Sales Order after date filtering : {mapped_sales_order_with_product_df}")
            
            # Join with the salesorder_item_mapping to get item details
            sales_orders_df = pd.merge(
                left=mapped_sales_order_with_product_df, 
                right=salesorder_item_mapping_df,
                how='left', 
                on=['salesorder_id']
            )
            
            # Get product mapping from instance (should be set after analyze_uploaded_products)
            product_mapping = getattr(self, 'aza_product_mapping', {})
            
            # Create a DataFrame of mapped products
            mapped_item_ids = list(product_mapping.values())
            mapped_products_df = pd.DataFrame({'item_id': mapped_item_ids})
            
            if not mapped_products_df.empty:
                # Filter to only include Aza items
                mapped_sales_order_with_product_df = pd.merge(
                    left=mapped_sales_order_with_product_df, 
                    right=mapped_products_df,
                    how='inner',  # Only include matches
                    on=['item_id']
                )
                logger.debug(f"Mapped Sales Order after merging with products df : {mapped_sales_order_with_product_df}")
            else:
                mapped_sales_order_with_product_df = sales_orders_df
            
            # Add invoice information - check if each sales order item has been invoiced
            if not mapped_sales_order_with_product_df.empty:
                # Load necessary data from database tables
                #logger.debug("Loading data from database tables")
                
                # Load invoice mappings
                try:
                    salesorder_invoice_mapping_df = crud.read_table('zakya_salesorder_invoice_mapping')
                    #logger.debug(f"Loaded {len(salesorder_invoice_mapping_df)} sales order-invoice mappings")
                except Exception as e:
                    logger.error(f"Error loading sales order invoice mappings: {str(e)}")
                    salesorder_invoice_mapping_df = pd.DataFrame()
                
                # Load sales order line item mappings
                try:
                    salesorder_line_item_mapping_df = crud.read_table('salesorder_line_item_mapping')
                    #logger.debug(f"Loaded {len(salesorder_line_item_mapping_df)} sales order line items")
                except Exception as e:
                    logger.error(f"Error loading sales order line item mappings: {str(e)}")
                    salesorder_line_item_mapping_df = pd.DataFrame()
                
                # Load invoice line item mappings
                try:
                    invoice_item_mapping_df = crud.read_table('zakya_invoice_line_item_mapping')
                    #logger.debug(f"Loaded {len(invoice_item_mapping_df)} invoice line items")
                except Exception as e:
                    logger.error(f"Error loading invoice line item mappings: {str(e)}")
                    invoice_item_mapping_df = pd.DataFrame()
                                
                # Create a dictionary to store invoice status and mapped salesorder_id for each row
                invoice_status_dict = {}
                mapped_salesorder_dict = {}
                
                # Function to check if an item is invoiced using reference numbers and database tables
                def check_if_invoiced(row):
                    row_id = row.name  # Use DataFrame index as unique identifier
                    
                    # Method 1: Check reference number against PO Number in pernia_orders
                    if 'extracted_po' in row and not pd.isna(row['extracted_po']) and self.sales_df is not None:
                        extracted_po = row['extracted_po']
                        
                        # Check if this extracted PO number matches any PO Number in pernia_orders
                        if 'PO No.' in self.sales_df.columns:
                            matching_po = self.sales_df[
                                self.sales_df['PO No.'] == extracted_po
                            ]
                            
                            if not matching_po.empty:
                                # Found a matching PO Number
                                mapped_salesorder_dict[row_id] = row.get('salesorder_id', '')
                                return f"Invoiced (PO: {extracted_po})"                    
                                        
                    # Method 2: Check using database tables
                    if not salesorder_invoice_mapping_df.empty and not invoice_item_mapping_df.empty and 'item_id' in row and not pd.isna(row['item_id']) and 'salesorder_id' in row and not pd.isna(row['salesorder_id']):
                        sales_order_id = row.get('salesorder_id')
                        item_id = row.get('item_id')
                        
                        # Filter for invoices associated with this sales order
                        so_invoices = salesorder_invoice_mapping_df[
                            salesorder_invoice_mapping_df['salesorder_id'] == sales_order_id
                        ]
                        
                        if not so_invoices.empty:
                            # For each invoice, check if the item is included
                            for _, invoice_row in so_invoices.iterrows():
                                invoice_id = invoice_row['invoice_id']
                                invoice_number = invoice_row['invoice_number']
                                
                                # Find matching invoice items
                                matching_items = invoice_item_mapping_df[
                                    (invoice_item_mapping_df['invoice_id'] == invoice_id) &
                                    (invoice_item_mapping_df['item_id'] == item_id)
                                ]
                                
                                if not matching_items.empty:
                                    # If there are multiple matches, sort by line_item_id and take the first
                                    if len(matching_items) > 1 and 'line_item_id' in matching_items.columns:
                                        matching_items = matching_items.sort_values('line_item_id').head(1)
                                    
                                    mapped_salesorder_dict[row_id] = sales_order_id
                                    return f"Invoiced (INV : {invoice_number})"
                    
                    # No invoice found through either method
                    if 'item_id' in row and not pd.isna(row['item_id']) and 'salesorder_id' in row and not pd.isna(row['salesorder_id']):
                        # Still store the salesorder_id for non-invoiced items
                        mapped_salesorder_dict[row_id] = row.get('salesorder_id', '')
                    
                    return "Not Invoiced"
                
                #logger.debug("Starting invoice status check")
                
                # Apply the check to each row in the DataFrame
                results = []
                for idx, row in mapped_sales_order_with_product_df.iterrows():
                    invoice_status = check_if_invoiced(row)
                    results.append(invoice_status)
                    invoice_status_dict[idx] = invoice_status
                
                # Add results back to DataFrame
                mapped_sales_order_with_product_df['Invoice Status'] = results
                
                # Add the mapped salesorder_id
                mapped_sales_order_with_product_df['Mapped Salesorder ID'] = mapped_sales_order_with_product_df.index.map(
                    lambda x: mapped_salesorder_dict.get(x, '')
                )
                
                #logger.debug(f"Invoice status check completed, status counts: {pd.Series(results).value_counts().to_dict()}")
                
            # Add inventory data if requested
            if include_inventory and not mapped_sales_order_with_product_df.empty:
                # Fetch product data from database
                zakya_products_df = crud.read_table('zakya_products')
                
                # Add inventory data
                for idx, row in mapped_sales_order_with_product_df.iterrows():
                    item_id = row.get('item_id')
                    if not pd.isna(item_id):
                        product_rows = zakya_products_df[zakya_products_df['item_id'] == item_id]
                        
                        if not product_rows.empty:
                            product_row = product_rows.iloc[0]
                            mapped_sales_order_with_product_df.at[idx, 'Available Stock'] = product_row.get('available_stock', 0)
                            mapped_sales_order_with_product_df.at[idx, 'Stock on Hand'] = product_row.get('stock_on_hand', 0)
            
            # Format the final DataFrame
            if not mapped_sales_order_with_product_df.empty:
                # Group by salesorder, item name, and date, then calculate aggregates
                logger.debug(f"Columns being mapped are : {mapped_sales_order_with_product_df.columns}")
                grouped_df = mapped_sales_order_with_product_df.groupby(
                    ['salesorder_number_x', 'name', 'date_x', 'item_id', 'Invoice Status', 'Mapped Salesorder ID', 'extracted_po']
                ).agg({
                    'quantity_y': 'sum',
                    'quantity_invoiced_y' : 'sum',
                    'rate': 'mean',
                    'total_x': 'sum'
                }).reset_index()
                
                # Rename columns for clarity
                renamed_df = grouped_df.rename(columns={
                    'salesorder_number_x': 'Order Number',
                    'name': 'Item Name',
                    'date_x': 'Order Date',
                    'quantity_y': 'Total Quantity',
                    'quantity_invoiced_y': 'Quantity Invoiced',
                    'rate': 'Average Rate',
                    'total_x': 'Total Amount',
                    'extracted_po' : 'Mapped POs',
                })
                
                # Add inventory columns if they exist
                if 'Available Stock' in mapped_sales_order_with_product_df.columns:
                    inventory_data = mapped_sales_order_with_product_df.groupby(
                        ['item_id']
                    ).agg({
                        'Available Stock': 'first',
                        'Stock on Hand': 'first'
                    }).reset_index()
                    
                    renamed_df = pd.merge(
                        left=renamed_df,
                        right=inventory_data,
                        how='left',
                        on=['item_id']
                    )
                
                return renamed_df
            else:
                return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Error fetching Aza sales orders: {str(e)}")
            return pd.DataFrame()

    async def analyze_missing_aza_salesorders(self, product_mapping, sales_orders):
        """
        Identify which Aza products need new sales orders and which are already in sales orders.
        
        Args:
            product_mapping: Dictionary mapping SKUs to item IDs
            sales_orders: DataFrame of existing sales orders
            
        Returns:
            tuple: (missing_df, present_df) - Missing and present sales order items with their details
        """
        # Prepare lists to store missing and present sales order items
        missing_items = []
        present_items = []
        
        # Get existing sales orders item_ids
        mapped_sales_order_items = {}
        
        if sales_orders is not None and not sales_orders.empty and 'item_id' in sales_orders.columns:
            # Only consider non-invoiced items as valid
            valid_orders = sales_orders[sales_orders['Invoice Status'] == 'Not Invoiced']
            if not valid_orders.empty:
                # Create a mapping of item_id to the lowest salesorder_id
                for _, row in valid_orders.iterrows():
                    item_id = row.get('item_id')
                    salesorder_id = row.get('Mapped Salesorder ID', '')
                    
                    if pd.isna(item_id) or not salesorder_id:
                        continue
                    
                    # If this item_id doesn't exist in our mapping or this salesorder_id is lower
                    if item_id not in mapped_sales_order_items or salesorder_id < mapped_sales_order_items[item_id]['salesorder_id']:
                        mapped_sales_order_items[item_id] = {
                            'salesorder_id': salesorder_id,
                            'row': row.to_dict()
                        }
        
        # Convert mapped_sales_order_items to a set for quick lookups
        mapped_item_ids = set(mapped_sales_order_items.keys())
        
        # Check each Aza order item
        for idx, row in self.sales_df.iterrows():
            sku = row.get('SKU', '').strip()
            
            if not sku:
                continue
            
            # Check if this SKU is mapped to a product
            is_mapped = sku in product_mapping
            item_id = product_mapping.get(sku, None)
            
            # Check if this item has a valid sales order
            has_sales_order = False
            if is_mapped and item_id in mapped_item_ids:
                has_sales_order = True
            
            # Convert Aza order row to dict for both cases
            aza_item = row.to_dict()
            aza_item['is_mapped'] = is_mapped
            aza_item['item_id'] = item_id
            
            if has_sales_order:
                # It's present - get the associated lowest sales order info
                so_info = mapped_sales_order_items.get(item_id, {}).get('row', {})
                
                # Add the selected salesorder_id
                aza_item['selected_salesorder_id'] = mapped_sales_order_items.get(item_id, {}).get('salesorder_id', '')
                
                # Merge Aza item data with sales order data
                present_item = {**aza_item, **so_info}
                present_items.append(present_item)
            else:
                # It's missing - add reason
                if not is_mapped:
                    aza_item['reason'] = "Not mapped in Zakya"
                elif item_id not in mapped_item_ids:
                    # Check if it's invoiced
                    if sales_orders is not None and not sales_orders.empty:
                        invoiced_orders = sales_orders[
                            (sales_orders['item_id'] == item_id) & 
                            (sales_orders['Invoice Status'] != 'Not Invoiced')
                        ]
                        if not invoiced_orders.empty:
                            aza_item['reason'] = "Already invoiced"
                        else:
                            aza_item['reason'] = "No sales order found"
                    else:
                        aza_item['reason'] = "No sales order found"
                
                missing_items.append(aza_item)
        
        # Create DataFrames
        missing_df = pd.DataFrame(missing_items) if missing_items else pd.DataFrame()
        present_df = pd.DataFrame(present_items) if present_items else pd.DataFrame()
        
        # Add inventory data for mapped items in missing_df
        if not missing_df.empty and 'item_id' in missing_df.columns:
            missing_df = await self.add_inventory_data(missing_df)
        
        # Add inventory data for mapped items in present_df
        if not present_df.empty and 'item_id' in present_df.columns:
            present_df = await self.add_inventory_data(present_df)
        
        return missing_df, present_df

    async def add_inventory_data(self, df):
        """
        Helper function to add inventory data to mapped items in a DataFrame.
        
        Args:
            df (DataFrame): DataFrame with item_id column
        
        Returns:
            DataFrame: Updated DataFrame with inventory data
        """
        mapped_items = df[df['is_mapped'] == True]
        if not mapped_items.empty:
            item_ids = mapped_items['item_id'].dropna().unique().tolist()
            
            if item_ids:
                # Fetch product data
                zakya_products_df = crud.read_table('zakya_products')
                
                # Add inventory data
                for idx, row in df.iterrows():
                    if row.get('is_mapped') and not pd.isna(row.get('item_id')):
                        item_id = row['item_id']
                        product_rows = zakya_products_df[zakya_products_df['item_id'] == item_id]
                        
                        if not product_rows.empty:
                            product_row = product_rows.iloc[0]
                            df.at[idx, 'available_stock'] = product_row.get('available_stock', 0)
                            df.at[idx, 'stock_on_hand'] = product_row.get('stock_on_hand', 0)
        
        return df

    async def create_missing_aza_salesorders(self, missing_orders, customer_id):
        """
        Create sales orders for Aza products that need them.
        
        Args:
            missing_orders: DataFrame of products needing sales orders
            customer_id: The Zakya customer ID
            
        Returns:
            Dictionary with creation results
        """
        # Define Aza-specific field mappings
        aza_options = {
            'ref_field': 'Item#',
            'date_field': 'Date',
            'delivery_date_field': None,  # Use current date if field doesn't exist
            'price_field': 'Total',
            'sku_field': 'SKU',
            'description_field': 'Item Description',
            'quantity_value': 1,
            'order_source': 'Aza'
        }
        
        # Initialize results
        results = {
            'success': False,
            'created_count': 0,
            'errors': [],
            'details': []
        }
        
        try:
            # Load product mappings
            mapping_product = crud.read_table("zakya_products")
            mapping_order = crud.read_table("zakya_sales_order")
            
            # Group orders by reference number or item#
            # If Item# doesn't exist, use designer name or create a batch ID
            ref_field = aza_options['ref_field']
            if ref_field in missing_orders.columns:
                grouped_orders = missing_orders.groupby(ref_field)
            else:
                # Use designer name or create a batch number
                if 'Item Description' in missing_orders.columns:
                    grouped_orders = missing_orders.groupby('Item Description')
                else:
                    # Add a batch field
                    batch_id = f"AZA_BATCH_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    missing_orders['batch_id'] = batch_id
                    grouped_orders = missing_orders.groupby('batch_id')
            
            # Track created sales orders
            created_count = 0
            
            # Process each group
            for ref_value, group in grouped_orders:
                # Skip if reference value is missing
                if pd.isna(ref_value) or not ref_value:
                    ref_value = f"AZA_ITEM_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                
                ref_value_str = str(ref_value)
                
                # Check if a sales order with this reference already exists
                existing_order = mapping_order[mapping_order["reference_number"] == ref_value_str]
                
                if not existing_order.empty:
                    #logger.debug(f"Sales Order with reference number {ref_value_str} already exists.")
                    results['details'].append({
                        'reference_number': ref_value_str,
                        'status': 'Skipped',
                        'reason': 'Already exists'
                    })
                    continue
                
                # Create line items for this reference group
                line_items = []
                
                # Process each item in this reference group
                for _, item in group.iterrows():
                    sku = str(item.get(aza_options['sku_field'], '')).strip()
                    description = str(item.get(aza_options['description_field'], '')).strip()
                    price = float(item.get(aza_options['price_field'], 0))
                    
                    # Skip if price is invalid
                    if price <= 0:
                        logger.warning(f"Skipping item with invalid price: {price}")
                        continue
                    
                    # Create line item
                    line_item = {
                        "description": f"Aza Item: {ref_value_str} - {description}",
                        "rate": price,
                        "quantity": aza_options['quantity_value'],
                        "item_total": price * aza_options['quantity_value']
                    }
                    
                    # Try to find item_id for this SKU
                    if sku:
                        filtered_products = mapping_product[mapping_product["sku"] == sku]
                        if not filtered_products.empty:
                            item_id = filtered_products["item_id"].iloc[0]
                            line_item["item_id"] = int(item_id)
                    
                    line_items.append(line_item)
                
                # Skip if no valid line items
                if not line_items:
                    results['errors'].append(f"No valid line items for reference {ref_value_str}")
                    results['details'].append({
                        'reference_number': ref_value_str,
                        'status': 'Skipped',
                        'reason': 'No valid line items'
                    })
                    continue
                
                # Get date from first item in group
                first_item = group.iloc[0]
                order_date = None
                if aza_options['date_field'] and aza_options['date_field'] in first_item:
                    order_date = first_item.get(aza_options['date_field'])
                
                # If date is missing, use current date
                if not order_date or pd.isna(order_date):
                    order_date = datetime.now().strftime('%Y-%m-%d')
                elif isinstance(order_date, datetime):
                    order_date = order_date.strftime('%Y-%m-%d')
                
                # Use current date for delivery date
                delivery_date = datetime.now().strftime('%Y-%m-%d')
                
                # Create sales order payload
                salesorder_payload = {
                    "customer_id": int(customer_id),
                    "date": order_date,
                    "shipment_date": delivery_date,
                    "reference_number": ref_value_str,
                    "line_items": line_items,
                    "notes": f"Order Source: {aza_options['order_source']}",
                    "terms": "Terms and Conditions"
                }
                
                # Create the sales order
                try:
                    #logger.debug(f"Creating sales order for reference {ref_value_str}")
                    response = post_record_to_zakya(
                        self.zakya_connection_object['base_url'],
                        self.zakya_connection_object['access_token'],
                        self.zakya_connection_object['organization_id'],
                        '/salesorders',
                        salesorder_payload
                    )
                    
                    # Check response
                    if response and 'salesorder' in response:
                        created_count += 1
                        results['details'].append({
                            'reference_number': ref_value_str,
                            'status': 'Success',
                            'salesorder_id': response['salesorder'].get('salesorder_id'),
                            'salesorder_number': response['salesorder'].get('salesorder_number'),
                            'line_item_count': len(line_items)
                        })
                        logger.info(f"Created sales order for reference {ref_value_str}")
                    else:
                        error_msg = f"Failed to create sales order for reference {ref_value_str}"
                        results['errors'].append(error_msg)
                        results['details'].append({
                            'reference_number': ref_value_str,
                            'status': 'Failed',
                            'error': str(response)
                        })
                        logger.error(f"{error_msg}: {response}")
                except Exception as e:
                    error_msg = f"Error creating sales order for reference {ref_value_str}"
                    results['errors'].append(f"{error_msg}: {str(e)}")
                    results['details'].append({
                        'reference_number': ref_value_str,
                        'status': 'Failed',
                        'error': str(e)
                    })
                    logger.error(f"{error_msg}: {e}")
            
            # Update results
            results['created_count'] = created_count
            results['success'] = created_count > 0
            
            return results
        
        except Exception as e:
            logger.error(f"Error in create_missing_aza_salesorders: {e}")
            results['errors'].append(f"General error: {str(e)}")
            return results

    def check_aza_invoice_readiness(self):
        """
        Verify if all products are ready for invoicing.
        
        Returns:
            Dictionary with readiness status and any blocking issues
        """
        readiness = {
            'is_ready': False,
            'issues': []
        }
        
        # Check if we have product mapping
        product_mapping = getattr(self, 'aza_product_mapping', {})
        if not product_mapping:
            readiness['issues'].append("Product mapping is missing. Please analyze product mapping first.")
            return readiness
        
        # Check if there are unmapped products
        unmapped_products = getattr(self, 'aza_unmapped_products', [])
        if unmapped_products and len(unmapped_products) > 0:
            readiness['issues'].append(f"Found {len(unmapped_products)} products not mapped in Zakya.")
        
        # Check if we have sales orders
        sales_orders = getattr(self, 'aza_sales_orders', None)
        if sales_orders is None:
            readiness['issues'].append("Sales orders not fetched. Please fetch sales orders first.")
            return readiness
        
        # Check if there are missing sales orders
        missing_orders = getattr(self, 'aza_missing_sales_orders', None)
        if missing_orders is not None and len(missing_orders) > 0:
            readiness['issues'].append(f"Found {len(missing_orders)} products without valid sales orders.")
        
        # Ready if no issues found
        if not readiness['issues']:
            readiness['is_ready'] = True
        
        return readiness