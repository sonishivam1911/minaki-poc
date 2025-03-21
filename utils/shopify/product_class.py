import shopify
import pandas as pd
from typing import List
from.shopify_base_class import BaseShopifyResource
from utils.shopify.collection_resource import CollectionResource


class ProductResource(BaseShopifyResource):
    """
    Resource-specific class for 'Product' objects,
    including logic for variants, etc.
    """
    
    def get_all(self):
        """
        Retrieves all Product objects, handles pagination,
        and returns them in a list.
        """
        # Make sure a session is active
        # (If you want to, you could check self.connector.session is not None)
        
        all_data = []
        product_data=getattr(shopify,"Product")
        data = product_data.find(since_id=0, limit=250)
        
        # Gather products with pagination
        while True:
            for d in data:
                all_data.append(d)
            if not data.has_next_page():
                break
            data = data.next_page()
        
        return all_data

    def create(self, title:str, vendor:str, handle:str, variants: list):
        """
        Create a single Product with variants.
        The parameters (title, vendor, handle, variants) are just examples.
        """
        new_product = shopify.Product()
        new_product.title = title
        new_product.vendor = vendor
        new_product.handle = handle
        # e.g. more fields: new_product.status, new_product.tags, etc.

        # Assign variants if provided
        variant_objects = []
        for variant_data in variants:
            variant_objects.append(shopify.Variant(variant_data))
        
        new_product.variants = variant_objects
        
        # Save to Shopify
        success = new_product.save()
        
        if success:
            print(f"Created product: {new_product.title} with ID {new_product.id}")
        else:
            print(f"Failed to create product: {new_product.errors.full_messages()}")

    def to_dataframe(self, products_list):
        """
        Convert products to a DataFrame, including variant data.
        """
        output = []
        for product in products_list:
            product_attrs = product.attributes
            
            # If product has variants, extract them
            if "variants" in product_attrs and product_attrs["variants"]:
                for v in product_attrs["variants"]:
                    variant_data = v.attributes if hasattr(v, 'attributes') else v
                    row = {
                        'product_id': product_attrs.get('id'),
                        'product_title': product_attrs.get('title'),
                        'product_handle': product_attrs.get('handle'),
                        'product_vendor': product_attrs.get('vendor'),
                        'product_type': product_attrs.get('product_type'),
                        'status': product_attrs.get('status'),
                        'tags': product_attrs.get('tags', ''),
                        'variant_id': variant_data.get('id'),
                        'variant_title': variant_data.get('title'),
                        'sku': variant_data.get('sku'),
                        'price': variant_data.get('price'),
                        'compare_at_price': variant_data.get('compare_at_price'),
                        'inventory_quantity': variant_data.get('inventory_quantity')
                    }
                    output.append(row)
            else:
                # If no variants, create a single row for the product
                row = {
                    'product_id': product_attrs.get('id'),
                    'product_title': product_attrs.get('title'),
                    'product_handle': product_attrs.get('handle'),
                    'product_vendor': product_attrs.get('vendor'),
                    'product_type': product_attrs.get('product_type'),
                    'status': product_attrs.get('status'),
                    'tags': product_attrs.get('tags', ''),
                    'variant_id': None,
                    'variant_title': None,
                    'sku': None,
                    'price': None,
                    'compare_at_price': None,
                    'inventory_quantity': None
                }
                output.append(row)
                
        return pd.DataFrame.from_records(output)
    

    def get_by_collection(self, collection_id: str) -> List:
        """
        Get all products that belong to a specific collection
        
        Args:
            collection_id: The collection ID to filter by
            
        Returns:
            List of product objects
        """
        collection_resource = CollectionResource(self.connector)
        return collection_resource.get_products_in_collection(collection_id)
    

    def get_by_ids(self, product_ids: List[str]) -> List:
        """
        Get products by their IDs
        
        Args:
            product_ids: List of product IDs
            
        Returns:
            List of product objects
        """
        if not product_ids:
            return []
        
        # Convert list of IDs to comma-separated string
        ids_string = ",".join(str(pid) for pid in product_ids)
        
        product_data = getattr(shopify, "Product")
        return list(product_data.find(ids=ids_string))