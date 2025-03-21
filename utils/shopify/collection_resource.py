import shopify
import pandas as pd
import os 

from config.logger import logger
from dotenv import load_dotenv
from .shopify_base_class import BaseShopifyResource

load_dotenv()

class CollectionResource(BaseShopifyResource):
    """
    Resource-specific class for 'Collection' objects,
    including logic for fetching collections and their products.
    """
    
    def get_all(self):
        """
        Retrieves all Collection objects, handles pagination,
        and returns them in a list.
        """
        all_collections = []
        collection_data = getattr(shopify, "CustomCollection")
        collections = collection_data.find(since_id=0, limit=250)
        
        # Gather collections with pagination
        while True:
            for collection in collections:
                all_collections.append(collection)
            if not collections.has_next_page():
                break
            collections = collections.next_page()
        
        # Also get smart collections
        smart_collection_data = getattr(shopify, "SmartCollection")
        smart_collections = smart_collection_data.find(since_id=0, limit=250)
        
        # Gather smart collections with pagination
        while True:
            for collection in smart_collections:
                all_collections.append(collection)
            if not smart_collections.has_next_page():
                break
            smart_collections = smart_collections.next_page()
        
        return all_collections
    
    def get_products_in_collection(self, collection_id):
        """
        Retrieves all products in a specific collection using Collect objects.
        Works for both custom and smart collections by using different query parameters.
        
        Args:
            collection_id: The ID of the collection to fetch products from
                
        Returns:
            List of product objects in the collection
        """
        try:
            # First determine if it's a smart collection
            is_smart_collection = False
            try:
                shopify.CustomCollection.find(collection_id)
            except:
                try:
                    shopify.SmartCollection.find(collection_id)
                    is_smart_collection = True
                except:
                    print(f"Collection {collection_id} not found")
                    return []
            
            collect_data = getattr(shopify, "Collect")
            url = "https://" + self.connector.shop_url + "/admin/api/" + self.connector.api_version + f"/collections/{collection_id}/products.json"
            # Make the request manually
            
            
            # Using requests library if available, or standard urllib
          
            
            # For smart collections, we need a different approach with the Collect resource
            # Instead of collection_id, we'll query for all collects and filter by collection_id
            if is_smart_collection:
                # Get all collects (potentially inefficient but necessary)
                # You may need to limit this for large stores
                import requests
                headers = shopify.ShopifyResource.headers
                response = requests.get(url, headers=headers)
                data = response.json() 

                # logger.debug(f"data is : {data}")
                
                # logger.debug(f"Found {len(data)} collects for smart collection {collection_id}")
                
                # Extract product IDs from the matching collects
                product_ids = [collect.get('id') for collect in data['products']]
                # logger.debug(f"product ids are : {product_ids}")
            else:
                # For custom collections, use the standard approach
                collects = collect_data.find(collection_id=collection_id, limit=250)
                
                all_collects = []
                while True:
                    for collect in collects:
                        all_collects.append(collect)
                    if not collects.has_next_page():
                        break
                    collects = collects.next_page()
                
                # Extract product IDs from collects
                product_ids = [collect.attributes.get('product_id') for collect in all_collects]
            
            # Fetch each product based on product_id in collects
            if product_ids:
                from .product_class import ProductResource
                product_resource = ProductResource(self.connector)
                return product_resource.get_by_ids(product_ids)
            else:
                return []
            
        except Exception as e:
            print(f"Error fetching products for collection {collection_id}: {e}")
            return []
    
    def get_all_collections_with_products(self):
        """
        Retrieves all collections and their associated products.
        
        Returns:
            Dictionary with collection objects as keys and lists of products as values
        """
        collections = self.get_all()
        collections_with_products = {}
        
        for collection in collections:
            products = self.get_products_in_collection(collection.id)
            collections_with_products[collection] = products
            
        return collections_with_products
    
    def collection_products_to_dataframe(self, collection, products):
        """
        Convert a collection and its products to a DataFrame.
        
        Args:
            collection: Collection object
            products: List of product objects in the collection
        
        Returns:
            DataFrame containing collection data joined with product data
        """
        output = []
        collection_data = collection.attributes
        
        for product in products:
            product_data = product.attributes
            
            # Create a row combining collection and product data
            row = {
                'collection_id': collection_data.get('id'),
                'collection_title': collection_data.get('title'),
                'product_id': product_data.get('id'),
                'product_title': product_data.get('title'),
                'product_handle': product_data.get('handle'),
                'product_vendor': product_data.get('vendor'),
                'product_type': product_data.get('product_type'),
                'status': product_data.get('status'),
                'tags': product_data.get('tags', ''),
                'created_at': product_data.get('created_at'),
                'updated_at': product_data.get('updated_at'),
                'published_at': product_data.get('published_at')
            }
            
            # Get price from first variant
            if 'variants' in product_data and product_data['variants']:
                row['price'] = product_data['variants'][0].get('price', 'N/A')
                row['sku'] = product_data['variants'][0].get('sku', 'N/A')
                row['inventory_quantity'] = product_data['variants'][0].get('inventory_quantity', 0)
            
            # If product has variants, include count
            if 'variants' in product_data:
                row['variant_count'] = len(product_data['variants'])
            
            output.append(row)
            
        return pd.DataFrame.from_records(output)
    
    def create(self, **kwargs):
        pass  