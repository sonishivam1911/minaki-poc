import asyncio
import pandas as pd
from collections import defaultdict
from abc import ABC, abstractmethod
from utils.postgres_connector import crud
from config.logger import logger
from schema.zakya_schemas.schema import ZakyaContacts, ZakyaProducts
from server.reports.update_salesorder_items_id_mapping_table import sync_salesorder_mappings_sync
from server.reports.update_invoice_item_ids_mapping_table import sync_invoice_mappings_sync
from queries.zakya import queries
from config.constants import (
    customer_mapping_zakya_contacts,
    products_mapping_zakya_products
)

class InvoiceProcessor(ABC):
    """Base class for invoice processing."""
    
    def __init__(self, sales_df, invoice_date, zakya_connection_object):
        """Initialize with sales data and connection info."""
        self.sales_df = sales_df
        self.invoice_date = invoice_date
        self.zakya_connection_object = zakya_connection_object
        self.product_config = None
        self.salesorder_config = None
    
    async def create_whereclause_fetch_data(self, pydantic_model, filter_dict, query):
        """Fetch data using where clause asynchronously."""
        try:
            whereClause = crud.build_where_clause(pydantic_model, filter_dict)
            formatted_query = query.format(whereClause=whereClause)
            data = await asyncio.to_thread(crud.execute_query, query=formatted_query, return_data=True)
            return data.to_dict('records')
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            return {"error": f"Error fetching data: {e}"}
    
    async def find_product(self, sku):
        """Find a product by SKU."""
        try:
            items_data = await self.create_whereclause_fetch_data(ZakyaProducts, {
                products_mapping_zakya_products['style']: {'op': 'eq', 'value': sku}
            }, queries.fetch_prodouct_records)    
            return items_data
        except Exception as e:
            #logger.debug(f"Error is {e}")
            return None
    
    async def find_customer_by_name(self, customer_name):
        """Find a customer by name."""
        customer_data = await self.create_whereclause_fetch_data(ZakyaContacts, {
            customer_mapping_zakya_contacts['display_name']: {'op': 'eq', 'value': customer_name}
        }, queries.fetch_customer_records)    
        return customer_data
    
    async def find_customer_by_branch(self, branch_name):
        """Find a customer by branch name."""
        customer_data = await self.create_whereclause_fetch_data(ZakyaContacts, {
            customer_mapping_zakya_contacts['branch_name']: {'op': 'eq', 'value': branch_name}
        }, queries.fetch_customer_records)    
        return customer_data
    
    async def run_limited_tasks(self, tasks, limit=10):
        """Run tasks with concurrency limit."""
        semaphore = asyncio.Semaphore(limit)
        
        async def run_with_semaphore(task):
            try:
                async with semaphore:
                    return await task
            except Exception as e:
                logger.debug(f" Error is {e}")
        
        return await asyncio.gather(*[run_with_semaphore(task) for task in tasks])
    
    @abstractmethod
    async def preprocess_data(self):
        """Preprocess the sales data. To be implemented by subclasses."""
        pass
    
    def fetch_item_id_sales_order_mapping(self):
        """Preprocess the sales data. To be implemented by subclasses."""
        zakya_salesorder_line_item_mapping_df = crud.read_table('zakya_salesorder_line_item_mapping')
        item_line_item_map = defaultdict(list)
        for item_id, line_item_id in zip(zakya_salesorder_line_item_mapping_df['item_id'], zakya_salesorder_line_item_mapping_df['line_item_id']):
            item_line_item_map[item_id].append(line_item_id)        
        return item_line_item_map


    def fetch_item_id_invoice_mapping_df(self):
        
        zakya_invoice_line_item_mapping_df = crud.read_table('zakya_invoice_line_item_mapping')
        return zakya_invoice_line_item_mapping_df

    @abstractmethod
    async def create_invoices(self):
        """Create invoices from processed data. To be implemented by subclasses."""
        pass

    def process(self):
        """Main processing method."""
        try:
            # Preprocess the data
            self.preprocess_data_sync()
            
            # Find existing products
            self.product_config = asyncio.run(self.find_existing_products())

            # if len(self.product_config['missing_products'])>0:
            #     return self.product_config

            # check missing products and then subsequently check for missing salesorder as well okay
            self.salesorder_config = asyncio.run(self.find_existing_salesorders())

            if len(self.salesorder_config['missing_items_without_salesorder']) > 0 or len(self.product_config['missing_products'])>0:
                return {'salesorder' : self.salesorder_config,'product':self.product_config}
            
            # Create invoices
            invoice_object = {
                'invoice_date': self.invoice_date,
                'existing_sku_item_id_mapping': self.product_config['existing_sku_item_id_mapping']
            }
            
            invoice_df = asyncio.run(self.create_invoices(invoice_object))
            return invoice_df
        except Exception as e:
            logger.error(f"Error in processing: {e}")
            return pd.DataFrame([{"status": "Failed", "error": str(e)}])
    
    @abstractmethod
    def preprocess_data_sync(self):
        """Synchronous data preprocessing. To be implemented by subclasses."""
        pass
    
    async def find_existing_products(self):
        """Find existing products and create mapping."""
        existing_products = []
        missing_products = []
        existing_sku_item_id_mapping = {}
        existing_products_data_dict = {}
        
        # Prepare product lookup tasks
        product_tasks = []
        product_skus = []
        
        # Get SKU field name based on child class
        sku_field = self.get_sku_field_name()
        vendor_sku_field = self.get_vendor_field_name()
        
        for _, row in self.sales_df.iterrows():
            sku = row.get(sku_field, "").strip()
            vendor_sku = row.get(vendor_sku_field, "")
            if not sku:
                missing_products.append(vendor_sku)
                continue
                
            product_tasks.append(self.find_product(sku))
            product_skus.append(sku)
        
        # Run product lookup tasks with concurrency limit
        product_results = await self.run_limited_tasks(product_tasks, limit=1)
        
        # Process product results
        for sku, items_data in zip(product_skus, product_results):
            if items_data and not isinstance(items_data, dict) and "error" not in items_data and len(items_data) > 0:
                existing_sku_item_id_mapping[sku] = items_data[0]["item_id"]
                existing_products.append(sku)
                existing_products_data_dict[items_data[0]["item_id"]]=items_data
            else:
                missing_products.append(sku)
        
        #logger.debug(f"missing_products: {missing_products}")
        #logger.debug(f"existing_products: {existing_products}")
        self.existing_products_data_dict = existing_products_data_dict
        
        return {
            "missing_products": missing_products,
            "existing_products": existing_products,
            "existing_sku_item_id_mapping": existing_sku_item_id_mapping,
            "existing_products_data_dict" : existing_products_data_dict
        }
    
    @abstractmethod
    def get_sku_field_name(self):
        """Return the field name for SKU in the dataframe."""
        pass

    @abstractmethod
    def get_vendor_field_name(self):
        """Return the field name for SKU in the dataframe."""
        pass

    async def find_existing_salesorders(self):
        """
        Find existing sales orders and check if they are already invoiced for specific items.
        Returns information about mapped and unmapped items with their sales order status.
        """
        # Synchronize salesorder and invoice mappings
        sync_salesorder_mappings_sync()
        sync_invoice_mappings_sync()
        
        # Get mapping for all salesorder and item id
        salesorder_item_mapping_dict = self.fetch_item_id_sales_order_mapping()
        
        # Initialize results containers
        missing_items_without_salesorder = []
        mapped_salesorder_with_item_id = {}
        mapped_items_with_inventory = {}

        # Process only if we have product mapping data
        if self.product_config and 'existing_sku_item_id_mapping' in self.product_config:
            # Fetch inventory data for all mapped items
            mapped_item_ids = list(self.product_config['existing_sku_item_id_mapping'].values())
            inventory_data = await self.fetch_inventory_data(mapped_item_ids)
            
            # Process each mapped product
            for sku, item_id in self.product_config['existing_sku_item_id_mapping'].items():
                # Store inventory data for this item
                if item_id in inventory_data:
                    mapped_items_with_inventory[item_id] = inventory_data[item_id]
                
                # Check if this item has a sales order
                if item_id in salesorder_item_mapping_dict:
                    # Get the sales order details
                    salesorder_line_item_id = salesorder_item_mapping_dict[item_id]
                    
                    # Check if this sales order is already invoiced for this item
                    is_invoiced = await self.check_if_item_invoiced(item_id, salesorder_line_item_id)
                    
                    if not is_invoiced:
                        # Item has a valid sales order that can be used for invoicing
                        mapped_salesorder_with_item_id[item_id] = salesorder_line_item_id
                    else:
                        # Item has a sales order but it's already invoiced
                        missing_items_without_salesorder.append({
                            'item_id': item_id,
                            'sku': sku,
                            'reason': 'Already invoiced'
                        })
                else:
                    # Item doesn't have any sales order
                    missing_items_without_salesorder.append({
                        'item_id': item_id,
                        'sku': sku,
                        'reason': 'No sales order found'
                    })
        
        return {
            'missing_items_without_salesorder': missing_items_without_salesorder,
            'mapped_salesorder_with_item_id': mapped_salesorder_with_item_id,
            'inventory_data': mapped_items_with_inventory
        }

    async def check_if_item_invoiced(self, item_id, salesorder_line_item_id):
        """Check if a specific item in a sales order is already invoiced."""
        # Fetch invoice mappings
        invoice_item_mapping_df = self.fetch_item_id_invoice_mapping_df()
        
        # Filter to find if this item_id is already in an invoice
        if not invoice_item_mapping_df.empty:
            filtered_df = invoice_item_mapping_df[
                invoice_item_mapping_df['item_id'] == item_id
            ]
            
            # If we found any invoice line items for this item_id, it's already invoiced
            return not filtered_df.empty
        
        return False

    async def fetch_inventory_data(self, item_ids):
        """Fetch inventory data for a list of item IDs."""
        inventory_data = {}
        
        try:
            # Read product data from database
            zakya_products_df = crud.read_table('zakya_products')
            
            # Filter to only include specified item IDs
            if not zakya_products_df.empty:
                filtered_products = zakya_products_df[zakya_products_df['item_id'].isin(item_ids)]
                
                # Create inventory data dictionary
                for _, row in filtered_products.iterrows():
                    item_id = row['item_id']
                    inventory_data[item_id] = {
                        'available_stock': row.get('available_stock', 0),
                        'actual_available_stock': row.get('actual_available_stock', 0),
                        'stock_on_hand': row.get('stock_on_hand', 0),
                        'reorder_level': row.get('reorder_level', None),
                        'track_inventory': row.get('track_inventory', False)
                    }
        except Exception as e:
            logger.error(f"Error fetching inventory data: {e}")
        
        return inventory_data


