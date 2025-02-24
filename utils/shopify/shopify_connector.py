import shopify

# session = shopify.Session(os.getenv("SHOPIFY_SHOP_URL"), os.getenv("SHOPIFY_API_VERSION"), os.getenv("SHOPIFY_ACCESS_TOKEN"))
# shopify.ShopifyResource.activate_session(session)

# def create_product_object():
#     new_product = shopify.Product()
#     new_product.title = "Shivam testing"
#     new_product.vendor = 'Minaki'
#     new_product.product_type = ''
#     new_product.created_at = ''
#     new_product.handle = "shivam-testing"
#     new_product.status = "draft"
#     new_product.tags = ""
#     new_product.template_suffix = None
#     new_product.body_html = None

#     variant1 = shopify.Variant({
#         "option1": "Small",
#         "option2": "Red",
#         "price": "19.99",
#         "sku": "SKU-RED-S"
#     })

#     variant2 = shopify.Variant({
#         "option1": "Medium",
#         "option2": "Blue",
#         "price": "21.99",
#         "sku": "SKU-BLU-M"
#     })

#     variant3 = shopify.Variant({
#         "option1": "Large",
#         "option2": "Green",
#         "price": "23.99",
#         "sku": "SKU-GRN-L"
#     })

#     # Assign the list of variant objects to the product
#     new_product.variants = [variant1, variant2, variant3]

#     new_product.save() 

#     print(new_product.get_id())

# def get_object(object_name):
#     all_data=[]
#     attribute=getattr(shopify,object_name)
#     data=attribute.find(since_id=0, limit=250)
#     for d in data:
#         all_data.append(d)
#     while data.has_next_page():
#         data=data.next_page()
#         for d in data:
#             all_data.append(d)
    
#     return all_data  

# def add_objects(data):
#     output=[]
#     for index in range(len(data)):
#         if data[index].attributes['variants']:
#             for variants_index in range(len(data[index].attributes['variants'])):
#                 output.append(data[index].attributes['variants'][variants_index].attributes)
#         else:
#             print(data)

#     return pd.DataFrame.from_records(output)



# shop = shopify.Shop.current()
# product = shopify.Product().find()
# print(shop)
# print(product)

# orders = get_object("Order")
# orders_df = add_objects(orders)
# create_product_object()
# products = get_object("Product")
# print(products[0].attributes)
# print(products[0].attributes['options'][0].attributes)
# print(products[0].attributes['variants'][0].attributes)
# products_df = add_objects(products)
# # crud.create_insert_statements(products_df,"Shopify_Product_Master")
# update_products_df = products_df[['sku','title','price','created_at']]
# # crud.create_insert_statements(update_products_df,"Shopify_Product_Master")
# print(products_df.columns)
# create class to create connector and get access token 


class ShopifyConnector:
    """Encapsulates the Shopify session and authentication logic."""
    
    def __init__(self, shop_url: str, api_version: str, access_token: str):
        self.shop_url = shop_url
        self.api_version = api_version
        self.access_token = access_token
        self.session = None

    def connect(self):
        """Create and activate a Shopify session."""
        self.session = shopify.Session(
            self.shop_url,
            self.api_version,
            self.access_token
        )
        shopify.ShopifyResource.activate_session(self.session)
        
    def disconnect(self):
        """Clear the active Shopify session."""
        shopify.ShopifyResource.clear_session()