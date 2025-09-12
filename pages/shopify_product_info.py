import streamlit as st
import os
from typing import Dict, List, Any, Optional
import json

# Import your GraphQL connector (using the code I shared earlier)
from utils.shopify.shopify_graphql import ShopifyGraphQLConnector

class ProductViewerApp:
    """
    Simple Streamlit app to view Shopify products with metafields using GraphQL.
    """
    
    def __init__(self):
        """Initialize the app."""
        self.connector = None
        self.setup_page_config()
    
    def setup_page_config(self):
        """Configure Streamlit page settings."""
        st.set_page_config(
            page_title="Shopify Product Viewer",
            page_icon="📦",
            layout="wide"
        )
    
    def setup_connection(self):
        """Setup Shopify connection using environment variables."""
        try:
            # Get credentials from environment or Streamlit secrets
            shop_url = os.getenv("SHOPIFY_SHOP_URL") or st.secrets.get("SHOPIFY_SHOP_URL")
            api_version = os.getenv("SHOPIFY_API_VERSION") or st.secrets.get("SHOPIFY_API_VERSION") 
            access_token = os.getenv("SHOPIFY_ACCESS_TOKEN") or st.secrets.get("SHOPIFY_ACCESS_TOKEN")
            
            if not all([shop_url, api_version, access_token]):
                st.error("Missing Shopify credentials. Please check your environment variables.")
                st.stop()
            
            # Initialize GraphQL connector
            self.connector = ShopifyGraphQLConnector(shop_url, api_version, access_token)
            
            st.success("Connected to Shopify successfully!")
            return True
            
        except Exception as e:
            st.error(f"Connection failed: {str(e)}")
            return False
    
    def fetch_products(self, limit: int = 20, search_query: Optional[str] = None):
        """Fetch products using GraphQL."""
        if not self.connector:
            return None
        
        try:
            with st.spinner("Loading products..."):
                result = self.connector.get_products(
                    first=limit, 
                    query_filter=search_query
                )
                
                if 'data' in result and 'products' in result['data']:
                    return result['data']['products']
                else:
                    st.error("No products data found in response")
                    return None
                    
        except Exception as e:
            st.error(f"Error fetching products: {str(e)}")
            return None
    
    def render_product_card(self, product: Dict[str, Any]):
        """Render a single product card with metafields."""
        with st.container():
            # Product image
            featured_image = product.get('featuredImage')
            if featured_image and featured_image.get('url'):
                st.image(featured_image['url'], use_container_width=True)
            else:
                st.write("📷 No Image")
            
            # Product title
            title = product.get('title', 'No Title')
            st.write(f"**{title[:50]}{'...' if len(title) > 50 else ''}**")
            
            # Basic product info
            variants = product.get('variants', {}).get('edges', [])
            if variants:
                first_variant = variants[0]['node']
                price = first_variant.get('price', 'N/A')
                sku = first_variant.get('sku', 'No SKU')
                inventory = first_variant.get('inventoryQuantity', 'N/A')
                
                st.write(f"💰 **Price:** ${price}")
                st.write(f"📦 **SKU:** {sku}")
                st.write(f"📊 **Stock:** {inventory}")
            
            # Product type and vendor
            product_type = product.get('productType', 'N/A')
            vendor = product.get('vendor', 'N/A')
            st.write(f"🏷️ **Type:** {product_type}")
            st.write(f"🏢 **Vendor:** {vendor}")
            
            # Tags
            tags = product.get('tags', [])
            if tags:
                # Show first 3 tags
                display_tags = tags[:3]
                tag_text = ", ".join(display_tags)
                if len(tags) > 3:
                    tag_text += f" (+{len(tags) - 3} more)"
                st.write(f"🏷️ **Tags:** {tag_text}")
            else:
                st.write("🏷️ **Tags:** No tags")
            
            # Metafields section
            metafields = product.get('metafields', {}).get('edges', [])
            if metafields:
                with st.expander(f"🔧 View Metafields ({len(metafields)})"):
                    self.render_metafields(metafields)
            else:
                st.write("🔧 **Metafields:** None")
            
            # Product details expandable
            with st.expander("📋 Product Details"):
                # Extract clean product ID (remove GraphQL prefix)
                product_id = product.get('id', '').replace('gid://shopify/Product/', '')
                
                st.write(f"**Product ID:** {product_id}")
                st.write(f"**Handle:** {product.get('handle', 'N/A')}")
                st.write(f"**Status:** {product.get('status', 'Unknown')}")
                st.write(f"**Created:** {product.get('createdAt', 'Unknown')}")
                st.write(f"**Updated:** {product.get('updatedAt', 'Unknown')}")
                
                # All variants info
                if variants:
                    st.write("**Variants:**")
                    for i, variant_edge in enumerate(variants[:5]):  # Show first 5 variants
                        variant = variant_edge['node']
                        variant_title = variant.get('title', f'Variant {i+1}')
                        variant_price = variant.get('price', 'N/A')
                        variant_sku = variant.get('sku', 'N/A')
                        st.write(f"  • {variant_title} - ${variant_price} (SKU: {variant_sku})")
                    
                    if len(variants) > 5:
                        st.write(f"  ... and {len(variants) - 5} more variants")
                
                # All tags
                if tags:
                    st.write("**All Tags:**")
                    for tag in tags:
                        st.write(f"  • {tag}")
            
            st.write("---")
    
    def render_metafields(self, metafields: List[Dict]):
        """Render metafields in an organized way."""
        if not metafields:
            st.write("No metafields found")
            return
        
        # Group metafields by namespace
        namespaces = {}
        for metafield_edge in metafields:
            metafield = metafield_edge['node']
            namespace = metafield.get('namespace', 'unknown')
            
            if namespace not in namespaces:
                namespaces[namespace] = []
            
            namespaces[namespace].append(metafield)
        
        # Display metafields by namespace
        for namespace, fields in namespaces.items():
            st.write(f"**Namespace: {namespace}**")
            
            for field in fields:
                key = field.get('key', 'unknown')
                value = field.get('value', 'N/A')
                field_type = field.get('type', 'string')
                description = field.get('description', '')
                
                # Format value based on type
                if field_type == 'json':
                    try:
                        # Try to parse and pretty print JSON
                        json_value = json.loads(value)
                        formatted_value = json.dumps(json_value, indent=2)
                        st.code(formatted_value, language='json')
                    except:
                        st.write(f"  • **{key}:** {value}")
                elif field_type in ['url', 'single_line_text_field']:
                    if value.startswith('http'):
                        st.write(f"  • **{key}:** [Link]({value})")
                    else:
                        st.write(f"  • **{key}:** {value}")
                else:
                    st.write(f"  • **{key}:** {value}")
                
                # Show type and description if available
                if description:
                    st.caption(f"    Type: {field_type} | {description}")
                else:
                    st.caption(f"    Type: {field_type}")
            
            st.write("")  # Add spacing between namespaces
    
    def run(self):
        """Run the main Streamlit app."""
        st.title("📦 Shopify Product Viewer")
        st.write("View your Shopify products with metafields using GraphQL")
        
        # Setup connection
        if not self.setup_connection():
            return
        
        # Search and filter section
        st.subheader("🔍 Product Search & Filters")
        
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col1:
            search_query = st.text_input(
                "Search products:",
                placeholder="e.g., title:shirt, tag:summer, vendor:nike",
                help="Use Shopify search syntax: title:keyword, tag:value, vendor:name"
            )
        
        with col2:
            limit = st.selectbox(
                "Products to load:",
                [10, 20, 50, 100],
                index=1
            )
        
        with col3:
            if st.button("🔄 Load Products", type="primary"):
                st.session_state['trigger_load'] = True
        
        # Load products
        products_data = None
        
        if st.session_state.get('trigger_load', False) or 'products_data' not in st.session_state:
            search_filter = search_query.strip() if search_query.strip() else None
            products_data = self.fetch_products(limit=limit, search_query=search_filter)
            
            if products_data:
                st.session_state['products_data'] = products_data
                st.session_state['trigger_load'] = False
        
        # Use cached data if available
        if 'products_data' in st.session_state:
            products_data = st.session_state['products_data']
        
        if not products_data:
            st.info("Click 'Load Products' to fetch products from your Shopify store")
            return
        
        # Display products
        products = products_data.get('edges', [])
        
        if not products:
            st.warning("No products found matching your criteria")
            return
        
        # Products overview
        st.subheader(f"📦 Products ({len(products)} found)")
        
        # Display pagination info if applicable
        page_info = products_data.get('pageInfo', {})
        if page_info.get('hasNextPage'):
            st.info("📄 More products available. Increase the limit or use pagination for more results.")
        
        # Product statistics
        total_variants = sum(
            len(product['node'].get('variants', {}).get('edges', [])) 
            for product in products
        )
        
        products_with_metafields = sum(
            1 for product in products 
            if product['node'].get('metafields', {}).get('edges', [])
        )
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Products", len(products))
        with col2:
            st.metric("Total Variants", total_variants)
        with col3:
            st.metric("With Metafields", products_with_metafields)
        with col4:
            metafield_percentage = (products_with_metafields / len(products)) * 100 if products else 0
            st.metric("Metafield Coverage", f"{metafield_percentage:.1f}%")
        
        # Render products in grid
        st.subheader("🗂️ Product Cards")
        
        # Display products in 4-column grid
        cols_per_row = 4
        for i in range(0, len(products), cols_per_row):
            cols = st.columns(cols_per_row)
            
            for j in range(cols_per_row):
                if i + j < len(products):
                    product = products[i + j]['node']
                    with cols[j]:
                        self.render_product_card(product)

# Main function to run the app
def main():
    """Main function to run the product viewer app."""
    app = ProductViewerApp()
    app.run()

if __name__ == "__main__":
    main()