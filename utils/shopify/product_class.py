import shopify
import pandas as pd

from.shopify_base_class import BaseShopifyResource

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
        Overriding the base method to handle variants or other product details.
        """
        output = []
        for product in products_list:
            # product.attributes is a dictionary containing product data
            product_attrs = product.attributes
            
            # If product has variants, extract them
            if "variants" in product_attrs:
                for v in product_attrs["variants"]:
                    output.append(v.attributes)
            # else:
            #     # If no variants, consider just appending product_attrs
            #     output.append(product_attrs)
                
        return pd.DataFrame.from_records(output)