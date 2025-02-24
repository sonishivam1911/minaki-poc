import shopify
import pandas as pd

from.shopify_base_class import BaseShopifyResource

class OrderResource(BaseShopifyResource):
    """
    Resource-specific class for 'Order' objects.
    Orders might not have 'variants' logic, but have line_items, etc.
    """
    
    def get_all(self):
        all_data = []
        order_data=getattr(shopify,"Order")
        data = order_data.find(since_id=0, limit=250)
        
        while True:
            for d in data:
                all_data.append(d)
            if not data.has_next_page():
                break
            data = data.next_page()
        
        return all_data

    def create(self, **kwargs):
        """
        You'd implement the logic for creating an Order here.
        Shopify might have specific fields needed, e.g. line items, addresses, etc.
        """
        new_order = shopify.Order()
        # Fill in new_order fields from kwargs
        # ...
        success = new_order.save()
        if success:
            print(f"Created order with ID {new_order.id}")
        else:
            print(f"Failed to create order: {new_order.errors.full_messages()}")

    def to_dataframe(self, orders_list):
        """
        Overriding the base method to transform order data 
        (like line_items, shipping info, etc.) into a DataFrame.
        """
        output = []
        for order in orders_list:
            # Example: flatten out some attributes
            oattrs = order.attributes
            
            # If you want line items in separate rows:
            if 'line_items' in oattrs:
                for item in oattrs['line_items']:
                    row = {
                        'order_id': oattrs['id'],
                        'created_at': oattrs.get('created_at'),
                        'customer_id': oattrs.get('customer', {}).get('id'),
                        'line_item_name': item.attributes.get('name'),
                        'line_item_sku': item.attributes.get('sku'),
                        'line_item_quantity': item.attributes.get('quantity'),
                        # etc.
                    }
                    output.append(row)
            else:
                # No line items, or handle differently
                pass
        
        return pd.DataFrame(output)