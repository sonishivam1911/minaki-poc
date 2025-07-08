import streamlit as st
import os
from dotenv import load_dotenv
from utils.shopify.shopify_connector import ShopifyConnector
from utils.shopify.collection_tag_dashboard import CollectionTagDashboard
from config.logger import logger

load_dotenv()

def main():
    """
    Collection Tag Manager page for your Streamlit multipage app
    """
    
    # Page configuration
    st.set_page_config(
        page_title="Collection Tag Manager",
        page_icon="🏷️",
        layout="wide"
    )
    
    # Page header
    st.title("🏷️ Collection Tag Manager")
    st.write("Manage and remove tags from products in your Shopify collections")
    
    # Check if environment variables are set
    required_env_vars = ["SHOPIFY_SHOP_URL", "SHOPIFY_API_VERSION", "SHOPIFY_ACCESS_TOKEN"]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    
    if missing_vars:
        st.error(f"❌ Missing environment variables: {', '.join(missing_vars)}")
        st.info("Please check your .env file and ensure all Shopify credentials are set.")
        st.stop()
    
    # Initialize Shopify connector
    try:
        with st.spinner("Connecting to Shopify..."):
            connector = ShopifyConnector(
                os.getenv("SHOPIFY_SHOP_URL"), 
                os.getenv("SHOPIFY_API_VERSION"), 
                os.getenv("SHOPIFY_ACCESS_TOKEN")
            )
            connector.connect()
            
        st.success("✅ Connected to Shopify successfully!")
        
        # Initialize and run the tag management dashboard
        dashboard = CollectionTagDashboard(connector)
        dashboard.run_dashboard()
        
    except Exception as e:
        st.error(f"❌ Failed to connect to Shopify: {str(e)}")
        logger.error(f"Shopify connection error in tag manager: {str(e)}")
        
        # Show troubleshooting info
        with st.expander("🔧 Troubleshooting"):
            st.write("**Check these common issues:**")
            st.write("1. Verify your .env file contains correct Shopify credentials")
            st.write("2. Ensure your Shopify store URL is in format: `your-store.myshopify.com`")
            st.write("3. Check that your access token has the required permissions")
            st.write("4. Verify your API version (e.g., '2023-10' or '2024-01')")
            
            st.write("**Current environment variables:**")
            st.write(f"- SHOPIFY_SHOP_URL: {'✅ Set' if os.getenv('SHOPIFY_SHOP_URL') else '❌ Missing'}")
            st.write(f"- SHOPIFY_API_VERSION: {'✅ Set' if os.getenv('SHOPIFY_API_VERSION') else '❌ Missing'}")
            st.write(f"- SHOPIFY_ACCESS_TOKEN: {'✅ Set' if os.getenv('SHOPIFY_ACCESS_TOKEN') else '❌ Missing'}")

if __name__ == "__main__":
    main()

