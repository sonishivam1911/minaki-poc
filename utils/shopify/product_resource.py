import shopify
import pandas as pd
from typing import List, Dict, Any, Optional
from config.logger import logger
from utils.shopify.shopify_base_class import BaseShopifyResource

class ProductResource(BaseShopifyResource):
    """
    Resource class for handling Shopify products, their metadata, and images.
    """
    
    def get_all(self, limit: int = 250, published_status: str = 'any') -> List[Dict[str, Any]]:
        """
        Retrieve all products from Shopify.
        
        Args:
            limit: Number of products per API call (max 250)
            published_status: 'published', 'unpublished', or 'any'
            
        Returns:
            List of product dictionaries
        """
        try:
            logger.info(f"Fetching all products from Shopify (status: {published_status})")
            all_products = []
            
            # Set up parameters
            params = {'limit': limit}
            if published_status != 'any':
                params['published_status'] = published_status
            
            # Fetch products with pagination
            products = shopify.Product.find(**params)
            while True:
                for product in products:
                    product_dict = self._product_to_dict(product)
                    all_products.append(product_dict)
                    
                if not products.has_next_page():
                    break
                products = products.next_page()
            
            logger.info(f"Successfully fetched {len(all_products)} products from Shopify")
            return all_products
            
        except Exception as e:
            logger.error(f"Error fetching products from Shopify: {str(e)}")
            raise

    def get_by_id(self, product_id: str, include_metafields: bool = False) -> Optional[Dict[str, Any]]:
        """
        Get a specific product by ID.
        
        Args:
            product_id: Shopify product ID
            include_metafields: Whether to include metafields in the response
            
        Returns:
            Product dictionary if found, None otherwise
        """
        try:
            logger.info(f"Fetching product ID: {product_id}")
            product = shopify.Product.find(product_id)
            
            if product:
                product_dict = self._product_to_dict(product)
                
                if include_metafields:
                    product_dict['metafields'] = self.get_metafields(product_id)
                    product_dict['metafields_count'] = len(product_dict['metafields'])
                
                logger.info(f"Found product: {product.title}")
                return product_dict
            else:
                logger.warning(f"Product not found: {product_id}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching product {product_id}: {str(e)}")
            return None

    def get_by_handle(self, handle: str, include_metafields: bool = False) -> Optional[Dict[str, Any]]:
        """
        Get a product by its handle.
        
        Args:
            handle: Product handle/slug
            include_metafields: Whether to include metafields
            
        Returns:
            Product dictionary if found, None otherwise
        """
        try:
            logger.info(f"Fetching product by handle: {handle}")
            products = shopify.Product.find(handle=handle)
            
            if products:
                product = products[0]  # Handle should be unique
                product_dict = self._product_to_dict(product)
                
                if include_metafields:
                    product_dict['metafields'] = self.get_metafields(str(product.id))
                    product_dict['metafields_count'] = len(product_dict['metafields'])
                
                logger.info(f"Found product by handle: {product.title}")
                return product_dict
            else:
                logger.warning(f"Product not found by handle: {handle}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching product by handle {handle}: {str(e)}")
            return None

    def search_products(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Search for products by title, tag, or other criteria.
        
        Args:
            query: Search query string
            limit: Maximum number of results
            
        Returns:
            List of matching product dictionaries
        """
        try:
            logger.info(f"Searching products with query: {query}")
            products = shopify.Product.find(title=query, limit=limit)
            
            results = []
            for product in products:
                product_dict = self._product_to_dict(product)
                results.append(product_dict)
            
            logger.info(f"Found {len(results)} products matching query: {query}")
            return results
            
        except Exception as e:
            logger.error(f"Error searching products with query {query}: {str(e)}")
            return []

    def get_metafields(self, product_id: str, namespace: str = None, key: str = None) -> List[Dict[str, Any]]:
        """
        Get metafields for a specific product.
        
        Args:
            product_id: Shopify product ID
            namespace: Filter by namespace (optional)
            key: Filter by key (optional)
            
        Returns:
            List of metafield dictionaries
        """
        try:
            logger.info(f"Fetching metafields for product ID: {product_id}")
            
            # Get metafields using direct API call
            metafields = shopify.Metafield.find(resource='products', resource_id=product_id)
            print(f"[DEBUG] Metafields are : {metafields}")
            
            metafields_data = []
            for metafield in metafields:
                # Apply filters if provided
                if namespace and metafield.namespace != namespace:
                    continue
                if key and metafield.key != key:
                    continue
                
                metafield_info = {
                    'id': str(metafield.id),
                    'namespace': metafield.namespace,
                    'key': metafield.key,
                    'value': metafield.value,
                    'type': getattr(metafield, 'type', 'string'),
                    'description': getattr(metafield, 'description', ''),
                    'owner_resource': getattr(metafield, 'owner_resource', 'product'),
                    'owner_id': str(getattr(metafield, 'owner_id', product_id)),
                    'created_at': getattr(metafield, 'created_at', None),
                    'updated_at': getattr(metafield, 'updated_at', None)
                }
                metafields_data.append(metafield_info)
            
            logger.info(f"Found {len(metafields_data)} metafields for product {product_id}")
            return metafields_data
            
        except Exception as e:
            logger.error(f"Error fetching metafields for product {product_id}: {str(e)}")
            return []

    def create_metafield(self, product_id: str, namespace: str, key: str, value: Any, 
                        metafield_type: str = 'string') -> Optional[Dict[str, Any]]:
        """
        Create a new metafield for a product.
        
        Args:
            product_id: Shopify product ID
            namespace: Metafield namespace
            key: Metafield key
            value: Metafield value
            metafield_type: Metafield type (string, integer, json, etc.)
            
        Returns:
            Created metafield dictionary if successful, None otherwise
        """
        try:
            logger.info(f"Creating metafield {namespace}.{key} for product {product_id}")
            
            metafield = shopify.Metafield()
            metafield.resource = 'products'
            metafield.resource_id = product_id
            metafield.namespace = namespace
            metafield.key = key
            metafield.value = value
            metafield.type = metafield_type
            
            if metafield.save():
                metafield_dict = {
                    'id': str(metafield.id),
                    'namespace': metafield.namespace,
                    'key': metafield.key,
                    'value': metafield.value,
                    'type': metafield.type,
                    'owner_resource': 'product',
                    'owner_id': product_id,
                    'created_at': getattr(metafield, 'created_at', None),
                    'updated_at': getattr(metafield, 'updated_at', None)
                }
                logger.info(f"Successfully created metafield {namespace}.{key}")
                return metafield_dict
            else:
                logger.error(f"Failed to create metafield: {metafield.errors}")
                return None
                
        except Exception as e:
            logger.error(f"Error creating metafield for product {product_id}: {str(e)}")
            return None

    def update_metafield(self, metafield_id: str, value: Any) -> bool:
        """
        Update an existing metafield.
        
        Args:
            metafield_id: Shopify metafield ID
            value: New metafield value
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Updating metafield ID: {metafield_id}")
            
            metafield = shopify.Metafield.find(metafield_id)
            if metafield:
                metafield.value = value
                if metafield.save():
                    logger.info(f"Successfully updated metafield {metafield_id}")
                    return True
                else:
                    logger.error(f"Failed to update metafield: {metafield.errors}")
                    return False
            else:
                logger.error(f"Metafield not found: {metafield_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error updating metafield {metafield_id}: {str(e)}")
            return False

    def delete_metafield(self, metafield_id: str) -> bool:
        """
        Delete a metafield.
        
        Args:
            metafield_id: Shopify metafield ID
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Deleting metafield ID: {metafield_id}")
            
            metafield = shopify.Metafield.find(metafield_id)
            if metafield:
                if metafield.destroy():
                    logger.info(f"Successfully deleted metafield {metafield_id}")
                    return True
                else:
                    logger.error(f"Failed to delete metafield: {metafield.errors}")
                    return False
            else:
                logger.error(f"Metafield not found: {metafield_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error deleting metafield {metafield_id}: {str(e)}")
            return False

    def update_product(self, product_id: str, **kwargs) -> bool:
        """
        Update product information.
        
        Args:
            product_id: Shopify product ID
            **kwargs: Fields to update (title, body_html, tags, etc.)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Updating product ID: {product_id}")
            
            product = shopify.Product.find(product_id)
            if product:
                # Update provided fields
                for key, value in kwargs.items():
                    if hasattr(product, key):
                        setattr(product, key, value)
                    else:
                        logger.warning(f"Product attribute '{key}' not found, skipping")
                
                if product.save():
                    logger.info(f"Successfully updated product {product_id}")
                    return True
                else:
                    logger.error(f"Failed to update product: {product.errors}")
                    return False
            else:
                logger.error(f"Product not found: {product_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error updating product {product_id}: {str(e)}")
            return False

    def get_product_images(self, product_id: str) -> List[Dict[str, Any]]:
        """
        Get all images for a product.
        
        Args:
            product_id: Shopify product ID
            
        Returns:
            List of image dictionaries
        """
        try:
            logger.info(f"Fetching images for product ID: {product_id}")
            
            product = shopify.Product.find(product_id)
            if not product:
                logger.error(f"Product not found: {product_id}")
                return []
            
            images_data = []
            if hasattr(product, 'images') and product.images:
                for image in product.images:
                    image_info = {
                        'id': str(image.id),
                        'src': image.src,
                        'alt': getattr(image, 'alt', ''),
                        'position': getattr(image, 'position', 0),
                        'width': getattr(image, 'width', None),
                        'height': getattr(image, 'height', None),
                        'variant_ids': getattr(image, 'variant_ids', []),
                        'created_at': getattr(image, 'created_at', None),
                        'updated_at': getattr(image, 'updated_at', None)
                    }
                    images_data.append(image_info)
            
            logger.info(f"Found {len(images_data)} images for product {product_id}")
            return images_data
            
        except Exception as e:
            logger.error(f"Error fetching images for product {product_id}: {str(e)}")
            return []

    def add_product_image(self, product_id: str, image_src: str, alt_text: str = '', 
                         position: int = None) -> Optional[Dict[str, Any]]:
        """
        Add an image to a product.
        
        Args:
            product_id: Shopify product ID
            image_src: Image URL or base64 encoded image
            alt_text: Alt text for the image
            position: Position of the image (optional)
            
        Returns:
            Created image dictionary if successful, None otherwise
        """
        try:
            logger.info(f"Adding image to product ID: {product_id}")
            
            product = shopify.Product.find(product_id)
            if not product:
                logger.error(f"Product not found: {product_id}")
                return None
            
            # Create new image
            image = shopify.Image()
            image.product_id = product_id
            image.src = image_src
            if alt_text:
                image.alt = alt_text
            if position:
                image.position = position
            
            if image.save():
                image_dict = {
                    'id': str(image.id),
                    'src': image.src,
                    'alt': getattr(image, 'alt', ''),
                    'position': getattr(image, 'position', 0),
                    'width': getattr(image, 'width', None),
                    'height': getattr(image, 'height', None),
                    'variant_ids': getattr(image, 'variant_ids', []),
                    'created_at': getattr(image, 'created_at', None),
                    'updated_at': getattr(image, 'updated_at', None)
                }
                logger.info(f"Successfully added image to product {product_id}")
                return image_dict
            else:
                logger.error(f"Failed to add image: {image.errors}")
                return None
                
        except Exception as e:
            logger.error(f"Error adding image to product {product_id}: {str(e)}")
            return None

    def update_product_image(self, product_id: str, image_id: str, **kwargs) -> bool:
        """
        Update a product image.
        
        Args:
            product_id: Shopify product ID
            image_id: Shopify image ID
            **kwargs: Fields to update (alt, position, etc.)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Updating image {image_id} for product {product_id}")
            
            image = shopify.Image.find(image_id, product_id=product_id)
            if image:
                # Update provided fields
                for key, value in kwargs.items():
                    if hasattr(image, key):
                        setattr(image, key, value)
                
                if image.save():
                    logger.info(f"Successfully updated image {image_id}")
                    return True
                else:
                    logger.error(f"Failed to update image: {image.errors}")
                    return False
            else:
                logger.error(f"Image not found: {image_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error updating image {image_id}: {str(e)}")
            return False

    def delete_product_image(self, product_id: str, image_id: str) -> bool:
        """
        Delete a product image.
        
        Args:
            product_id: Shopify product ID
            image_id: Shopify image ID
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Deleting image {image_id} from product {product_id}")
            
            image = shopify.Image.find(image_id, product_id=product_id)
            if image:
                if image.destroy():
                    logger.info(f"Successfully deleted image {image_id}")
                    return True
                else:
                    logger.error(f"Failed to delete image: {image.errors}")
                    return False
            else:
                logger.error(f"Image not found: {image_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error deleting image {image_id}: {str(e)}")
            return False

    def get_product_keys(self) -> List[str]:
        """
        Get all available product attribute keys by examining a sample product.
        
        Returns:
            List of product attribute keys
        """
        try:
            logger.info("Fetching product attribute keys")
            
            # Get a sample product to examine its structure
            products = shopify.Product.find(limit=1)
            if not products:
                logger.warning("No products found to examine keys")
                return []
            
            sample_product = products[0]
            
            # Get all attribute keys
            if hasattr(sample_product, 'attributes'):
                keys = list(sample_product.attributes.keys())
            else:
                # Fallback to common product attributes
                keys = [
                    'id', 'title', 'body_html', 'vendor', 'product_type', 'handle',
                    'created_at', 'updated_at', 'published_at', 'template_suffix',
                    'status', 'published_scope', 'tags', 'admin_graphql_api_id',
                    'variants', 'options', 'images', 'image'
                ]
            
            logger.info(f"Found {len(keys)} product attribute keys")
            return sorted(keys)
            
        except Exception as e:
            logger.error(f"Error fetching product keys: {str(e)}")
            return []

    def create(self, **kwargs):
        """
        Create a new product.
        
        Args:
            **kwargs: Product attributes (title, body_html, etc.)
            
        Returns:
            Created product dictionary if successful, None otherwise
        """
        try:
            logger.info("Creating new product")
            
            product = shopify.Product()
            
            # Set provided attributes
            for key, value in kwargs.items():
                if hasattr(product, key):
                    setattr(product, key, value)
                else:
                    logger.warning(f"Product attribute '{key}' not found, skipping")
            
            if product.save():
                product_dict = self._product_to_dict(product)
                logger.info(f"Successfully created product: {product.title}")
                return product_dict
            else:
                logger.error(f"Failed to create product: {product.errors}")
                return None
                
        except Exception as e:
            logger.error(f"Error creating product: {str(e)}")
            return None

    def to_dataframe(self, products_list: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Convert products list to pandas DataFrame.
        
        Args:
            products_list: List of product dictionaries
            
        Returns:
            pandas DataFrame with product information
        """
        if not products_list:
            return pd.DataFrame()
        
        data = []
        for product in products_list:
            row = {
                'id': product.get('id'),
                'title': product.get('title'),
                'handle': product.get('handle'),
                'vendor': product.get('vendor', ''),
                'product_type': product.get('product_type', ''),
                'tags': product.get('tags_string', ''),
                'status': product.get('status', 'unknown'),
                'variants_count': product.get('variant_count', 0),
                'images_count': product.get('image_count', 0),
                'created_at': product.get('created_at', 'Unknown'),
                'updated_at': product.get('updated_at', 'Unknown'),
                'published_at': product.get('published_at', 'Unknown')
            }
            data.append(row)
        
        df = pd.DataFrame(data)
        logger.info(f"Created DataFrame with {len(df)} products")
        return df

    def _product_to_dict(self, product: shopify.Product) -> Dict[str, Any]:
        """
        Convert a Shopify Product object to a dictionary.
        
        Args:
            product: Shopify Product object
            
        Returns:
            Product dictionary
        """
        # Get primary image
        image_url = None
        image_count = 0
        if hasattr(product, 'images') and product.images:
            image_url = product.images[0].src
            image_count = len(product.images)
        elif hasattr(product, 'image') and product.image:
            image_url = product.image.src
            image_count = 1
        
        # Get primary variant info
        primary_variant = product.variants[0] if product.variants else None
        price = primary_variant.price if primary_variant else 'N/A'
        sku = primary_variant.sku if primary_variant else 'No SKU'
        inventory = primary_variant.inventory_quantity if primary_variant else 'N/A'
        
        # Process tags
        product_tags = [tag.strip() for tag in product.tags.split(',') if tag.strip()]
        
        return {
            'id': str(product.id),
            'title': product.title,
            'body_html': getattr(product, 'body_html', ''),
            'handle': product.handle,
            'vendor': getattr(product, 'vendor', ''),
            'product_type': getattr(product, 'product_type', ''),
            'tags': product_tags,
            'tags_string': product.tags,
            'status': getattr(product, 'status', 'unknown'),
            'published_at': getattr(product, 'published_at', None),
            'template_suffix': getattr(product, 'template_suffix', ''),
            'published_scope': getattr(product, 'published_scope', 'web'),
            'admin_graphql_api_id': getattr(product, 'admin_graphql_api_id', ''),
            'image_url': image_url,
            'image_count': image_count,
            'price': price,
            'primary_sku': sku,
            'primary_inventory': inventory,
            'variant_count': len(product.variants),
            'created_at': getattr(product, 'created_at', 'Unknown'),
            'updated_at': getattr(product, 'updated_at', 'Unknown'),
            'shopify_object': product  # Keep reference to original object
        }