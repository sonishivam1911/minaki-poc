import asyncio
import pandas as pd
from abc import ABC, abstractmethod
from utils.postgres_connector import crud
from config.logger import logger
from schema.zakya_schemas.schema import ZakyaContacts, ZakyaProducts
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
        items_data = await self.create_whereclause_fetch_data(ZakyaProducts, {
            products_mapping_zakya_products['style']: {'op': 'eq', 'value': sku}
        }, queries.fetch_prodouct_records)    
        return items_data
    
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
            async with semaphore:
                return await task
        
        return await asyncio.gather(*[run_with_semaphore(task) for task in tasks])
    
    @abstractmethod
    async def preprocess_data(self):
        """Preprocess the sales data. To be implemented by subclasses."""
        pass
    
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
        
        # Prepare product lookup tasks
        product_tasks = []
        product_skus = []
        
        # Get SKU field name based on child class
        sku_field = self.get_sku_field_name()
        
        for _, row in self.sales_df.iterrows():
            sku = row.get(sku_field, "").strip()
            if not sku:
                continue
                
            product_tasks.append(self.find_product(sku))
            product_skus.append(sku)
        
        # Run product lookup tasks with concurrency limit
        product_results = await self.run_limited_tasks(product_tasks, limit=10)
        
        # Process product results
        for sku, items_data in zip(product_skus, product_results):
            if items_data and not isinstance(items_data, dict) and "error" not in items_data and len(items_data) > 0:
                existing_sku_item_id_mapping[sku] = items_data[0]["item_id"]
                existing_products.append(sku)
            else:
                missing_products.append(sku)
        
        logger.debug(f"missing_products: {missing_products}")
        logger.debug(f"existing_products: {existing_products}")
        
        return {
            "missing_products": missing_products,
            "existing_products": existing_products,
            "existing_sku_item_id_mapping": existing_sku_item_id_mapping
        }
    
    @abstractmethod
    def get_sku_field_name(self):
        """Return the field name for SKU in the dataframe."""
        pass




