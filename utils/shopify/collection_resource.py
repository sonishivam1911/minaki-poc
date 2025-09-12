import shopify
import pandas as pd
from typing import List, Dict, Any, Optional, Union
from config.logger import logger
from utils.shopify.shopify_base_class import BaseShopifyResource

class CollectionResource(BaseShopifyResource):
    """
    Resource class for handling Shopify collections and their products.
    """
    
    def get_all(self) -> List[Dict[str, Any]]:
        """
        Retrieve all collections from Shopify.
        Returns a list of dictionaries instead of Shopify objects to avoid type issues.
        
        Returns:
            List of collection dictionaries
        """
        try:
            logger.info("Fetching all collections from Shopify")
            all_collections = []
            
            # Fetch custom collections
            custom_collections = shopify.CustomCollection.find(limit=250)
            while True:
                for collection in custom_collections:
                    collection_dict = {
                        'id': str(collection.id),
                        'title': collection.title,
                        'handle': collection.handle,
                        'type': 'custom',
                        'products_count': getattr(collection, 'products_count', 0),
                        'published_at': getattr(collection, 'published_at', None),
                        'created_at': getattr(collection, 'created_at', None),
                        'updated_at': getattr(collection, 'updated_at', None),
                        'shopify_object': collection  # Keep reference to original object
                    }
                    all_collections.append(collection_dict)
                    
                if not custom_collections.has_next_page():
                    break
                custom_collections = custom_collections.next_page()
            
            # Fetch smart collections
            smart_collections = shopify.SmartCollection.find(limit=250)
            while True:
                for collection in smart_collections:
                    collection_dict = {
                        'id': str(collection.id),
                        'title': collection.title,
                        'handle': collection.handle,
                        'type': 'smart',
                        'products_count': getattr(collection, 'products_count', 0),
                        'published_at': getattr(collection, 'published_at', None),
                        'created_at': getattr(collection, 'created_at', None),
                        'updated_at': getattr(collection, 'updated_at', None),
                        'shopify_object': collection  # Keep reference to original object
                    }
                    all_collections.append(collection_dict)
                    
                if not smart_collections.has_next_page():
                    break
                smart_collections = smart_collections.next_page()
            
            logger.info(f"Successfully fetched {len(all_collections)} collections from Shopify")
            return all_collections
            
        except Exception as e:
            logger.error(f"Error fetching collections from Shopify: {str(e)}")
            raise

    def get_collection_by_name(self, collection_name: str) -> Optional[Dict[str, Any]]:
        """
        Find a collection by its name.
        
        Args:
            collection_name: Name of the collection to find
            
        Returns:
            Collection dictionary if found, None otherwise
        """
        try:
            logger.info(f"Searching for collection: {collection_name}")
            all_collections = self.get_all()
            
            for collection in all_collections:
                if collection['title'].lower() == collection_name.lower():
                    logger.info(f"Found collection: {collection_name} (ID: {collection['id']})")
                    return collection
            
            logger.warning(f"Collection not found: {collection_name}")
            return None
            
        except Exception as e:
            logger.error(f"Error searching for collection {collection_name}: {str(e)}")
            return None

    def get_products_in_collection(self, collection_id: str) -> List[shopify.Product]:
        """
        Get all products in a specific collection.
        
        Args:
            collection_id: Shopify collection ID
            
        Returns:
            List of Shopify Product objects in the collection
        """
        try:
            logger.info(f"Fetching products for collection ID: {collection_id}")
            all_products = []
            
            # Get products from collection
            products = shopify.Product.find(collection_id=collection_id, limit=250)
            while True:
                for product in products:
                    all_products.append(product)
                if not products.has_next_page():
                    break
                products = products.next_page()
            
            if all_products:
                first_product = all_products[0]
                print(f"Product attributes keys: {list(first_product.attributes.keys())}")
                print(f"Product attributes: {first_product.attributes}")
            return all_products
            
        except Exception as e:
            logger.error(f"Error fetching products for collection {collection_id}: {str(e)}")
            raise

    def get_collection_names_for_dropdown(self) -> List[Dict[str, str]]:
        """
        Get collection names formatted for Streamlit dropdown.
        
        Returns:
            List of dictionaries with collection names and IDs
        """
        try:
            all_collections = self.get_all()
            dropdown_options = []
            
            for collection in all_collections:
                dropdown_options.append({
                    'name': collection['title'],
                    'id': collection['id'],
                    'product_count': collection.get('products_count', 'Unknown'),
                    'type': collection.get('type', 'unknown')
                })
            
            # Sort by name
            dropdown_options.sort(key=lambda x: x['name'].lower())
            
            logger.info(f"Prepared {len(dropdown_options)} collections for dropdown")
            return dropdown_options
            
        except Exception as e:
            logger.error(f"Error preparing collection dropdown: {str(e)}")
            return []

    def analyze_tags_in_collection(self, collection_id: str) -> Dict[str, Any]:
        """
        Analyze all tags used by products in a collection.
        
        Args:
            collection_id: Shopify collection ID
            
        Returns:
            Dictionary with tag analysis data
        """
        try:
            logger.info(f"Analyzing tags for collection {collection_id}")
            products = self.get_products_in_collection(collection_id)
            
            tag_analysis = {
                'total_products': len(products),
                'tag_frequency': {},
                'products_by_tag': {},
                'unique_tags': set(),
                'untagged_products': []
            }
            
            for product in products:
                product_tags = [tag.strip() for tag in product.tags.split(',') if tag.strip()]
                
                if not product_tags:
                    tag_analysis['untagged_products'].append({
                        'id': str(product.id),
                        'title': product.title,
                        'handle': product.handle
                    })
                
                for tag in product_tags:
                    tag_analysis['unique_tags'].add(tag)
                    
                    # Count frequency
                    if tag not in tag_analysis['tag_frequency']:
                        tag_analysis['tag_frequency'][tag] = 0
                        tag_analysis['products_by_tag'][tag] = []
                    
                    tag_analysis['tag_frequency'][tag] += 1
                    tag_analysis['products_by_tag'][tag].append({
                        'id': str(product.id),
                        'title': product.title,
                        'handle': product.handle,
                        'sku': product.variants[0].sku if product.variants else 'No SKU'
                    })
            
            # Convert set to sorted list
            tag_analysis['unique_tags'] = sorted(list(tag_analysis['unique_tags']))
            
            # Sort tags by frequency (most used first)
            tag_analysis['sorted_tags'] = sorted(
                tag_analysis['tag_frequency'].items(),
                key=lambda x: x[1],
                reverse=True
            )
            
            logger.info(f"Tag analysis complete: {len(tag_analysis['unique_tags'])} unique tags found")
            return tag_analysis
            
        except Exception as e:
            logger.error(f"Error analyzing tags for collection {collection_id}: {str(e)}")
            raise

    def create(self, **kwargs):
        """
        Create a new collection (not implemented for this use case).
        """
        raise NotImplementedError("Collection creation not implemented in this tool")

    def to_dataframe(self, collections_list: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Convert collections list to pandas DataFrame.
        
        Args:
            collections_list: List of collection dictionaries
            
        Returns:
            pandas DataFrame with collection information
        """
        if not collections_list:
            return pd.DataFrame()
        
        data = []
        for collection in collections_list:
            row = {
                'id': collection.get('id'),
                'title': collection.get('title'),
                'handle': collection.get('handle'),
                'products_count': collection.get('products_count', 'Unknown'),
                'collection_type': collection.get('type', 'unknown').title(),
                'published': collection.get('published_at') is not None,
                'created_at': collection.get('created_at', 'Unknown'),
                'updated_at': collection.get('updated_at', 'Unknown')
            }
            data.append(row)
        
        df = pd.DataFrame(data)
        logger.info(f"Created DataFrame with {len(df)} collections")
        return df

    def products_to_card_data(self, products_list: List[shopify.Product]) -> List[Dict[str, Any]]:
        """
        Convert products list to card display format.
        
        Args:
            products_list: List of Shopify Product objects
            
        Returns:
            List of dictionaries formatted for card display
        """
        if not products_list:
            return []
        
        card_data = []
        for product in products_list:
            # Get primary image
            image_url = None
            if hasattr(product, 'images') and product.images:
                image_url = product.images[0].src
            elif hasattr(product, 'image') and product.image:
                image_url = product.image.src
            
            # Get primary variant info
            primary_variant = product.variants[0] if product.variants else None
            price = primary_variant.price if primary_variant else 'N/A'
            sku = primary_variant.sku if primary_variant else 'No SKU'
            inventory = primary_variant.inventory_quantity if primary_variant else 'N/A'
            
            # Process tags
            product_tags = [tag.strip() for tag in product.tags.split(',') if tag.strip()]
            
            card_info = {
                'id': str(product.id),
                'title': product.title,
                'handle': product.handle,
                'image_url': image_url,
                'price': price,
                'sku': sku,
                'inventory_quantity': inventory,
                'tags': product_tags,
                'tags_string': product.tags,
                'status': getattr(product, 'status', 'unknown'),
                'product_type': getattr(product, 'product_type', ''),
                'vendor': getattr(product, 'vendor', ''),
                'variant_count': len(product.variants),
                'created_at': getattr(product, 'created_at', 'Unknown'),
                'updated_at': getattr(product, 'updated_at', 'Unknown')
            }
            
            card_data.append(card_info)
        
        logger.info(f"Converted {len(card_data)} products to card format")
        return card_data