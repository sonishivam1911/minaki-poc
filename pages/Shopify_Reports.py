import streamlit as st
import os
import pandas as pd
from dotenv import load_dotenv

from utils.postgres_connector import crud
from config.logger import logger
from utils.shopify.shopify_connector import ShopifyConnector
from utils.shopify.product_class import ProductResource
from utils.shopify.collection_resource import CollectionResource

load_dotenv()

def main():
    connector = ShopifyConnector(os.getenv("SHOPIFY_SHOP_URL"), os.getenv("SHOPIFY_API_VERSION"), os.getenv("SHOPIFY_ACCESS_TOKEN"))
    connector.connect()
    shopify_product_controller(connector)
    shopify_collection_controller(connector)

def shopify_product_controller(connector):
    with st.container():
        st.header("Shopify Products")
        product_resource = ProductResource(connector)
        products_list = product_resource.get_all()
        
        
        show_preview = st.checkbox(key=1,label="Show/Hide Products",value=True)
        if show_preview:                
            products_df = product_resource.to_dataframe(products_list)
            st.dataframe(products_df)
            if st.button(key=2,label="Save to Database",on_click=crud.create_table,args=('shopify_product_master',products_df)):
                st.success("shopify_product_master saved to database successfully!")

def shopify_collection_controller(connector):
    with st.container():
        st.header("Shopify Custom Collection")
        custom_collection_resource = CollectionResource(connector)
        custom_collection_list = custom_collection_resource.get_all()

        # Create a list of collection titles and IDs for the dropdown
        collection_options = [(c.attributes.get('title', f"Collection {i}"), c.id) 
                            for i, c in enumerate(custom_collection_list)]    

        collection_titles = [title for title, _ in collection_options]
        selected_title = st.selectbox("Select a collection:", collection_titles)        

        # Find the selected collection ID
        selected_id = next((id for title, id in collection_options
                          if title == selected_title), None)    

        if selected_id:
            # Fetch products for the selected collection
            with st.spinner("Loading products..."):
                products = custom_collection_resource.get_products_in_collection(selected_id)    
                # #logger.debug(f"Products fetched from collection : {products}")

            # Display products
            st.subheader(f"Products in {selected_title}")
            
            if products:
                # Create a DataFrame for easier display
                product_data = []
                for product in products:
                    p = product.attributes
                    product_data.append({
                        "Product ID": p.get("id"),
                        "Title": p.get("title"),
                        "Product Type" : p.get("product_type"),
                        "Tags" : p.get("tags"),
                        "Vendor": p.get("vendor", "N/A"),
                        "Handle": p.get("handle"),
                        "Variants": len(p.get("variants", [])),
                        "Status": p.get("status", "N/A")
                    })
                
                # Display as a table
                if product_data:
                    df = pd.DataFrame(product_data)
                    st.dataframe(df)
                else:
                    st.info("No product data available for this collection.")
            else:
                st.info("No products found in this collection.")                            

        
        
        show_preview = st.checkbox(key=3,label="Show/Hide Custom Collection",value=True)
        if show_preview:                
            custom_collection_df = custom_collection_resource.to_dataframe(custom_collection_list)
            st.dataframe(custom_collection_df)
            if st.button(key=4,label="Save to Database",on_click=crud.create_table,args=('shopify_custom_collection_master',custom_collection_df)):
                st.success("shopify_product_master saved to database successfully!")


main()