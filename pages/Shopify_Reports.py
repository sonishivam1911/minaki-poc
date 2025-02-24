import streamlit as st
import os
from dotenv import load_dotenv

from utils.postgres_connector import crud
from utils.shopify.shopify_connector import ShopifyConnector
from utils.shopify.product_class import ProductResource

load_dotenv()

def main():
    connector = ShopifyConnector(os.getenv("SHOPIFY_SHOP_URL"), os.getenv("SHOPIFY_API_VERSION"), os.getenv("SHOPIFY_ACCESS_TOKEN"))
    connector.connect()

    with st.container():
        st.header("Shopify Products")
        product_resource = ProductResource(connector)
        products_list = product_resource.get_all()
        
        
        show_preview = st.checkbox("Show/Hide Products",value=True)
        if show_preview:                
            products_df = product_resource.to_dataframe(products_list)
            st.dataframe(products_df)
            if st.button("Save to Database",on_click=crud.create_table,args=('shopify_product_master',products_df)):
                st.success("shopify_product_master saved to database successfully!")

main()