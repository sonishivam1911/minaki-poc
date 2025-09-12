import streamlit as st
import pandas as pd
import time
from datetime import datetime
from typing import Dict, List, Any, Optional

from config.logger import logger
from utils.shopify.shopify_connector import ShopifyConnector
from utils.shopify.collection_resource import CollectionResource
from utils.shopify.shopify_tag_manager import ShopifyTagManager

class CollectionTagDashboard:
    """
    Main dashboard for managing tags in Shopify collections.
    """
    
    def __init__(self, shopify_connector: ShopifyConnector):
        """
        Initialize the dashboard.
        
        Args:
            shopify_connector: Connected Shopify connector
        """
        self.connector = shopify_connector
        self.collection_resource = CollectionResource(shopify_connector)
        self.tag_manager = ShopifyTagManager()
        
        # Session state keys
        self.COLLECTIONS_KEY = "available_collections"
        self.SELECTED_COLLECTION_KEY = "selected_collection"
        self.COLLECTION_PRODUCTS_KEY = "collection_products"
        self.TAG_ANALYSIS_KEY = "tag_analysis"
        self.SELECTED_TAG_KEY = "selected_tag_for_removal"
        self.TAG_REMOVAL_RESULTS_KEY = "tag_removal_results"
        self.TAG_ADDITION_RESULTS_KEY = "tag_addition_results"
        self.NEW_TAG_KEY = "new_tag_to_add"
        
        logger.info("CollectionTagDashboard initialized")

    def run_dashboard(self):
        """Run the main Streamlit dashboard."""
        st.title("🏷️ Shopify Collection Tag Manager")
        st.write("Manage, analyze, add, and remove tags from products in your Shopify collections")
        
        # Create the 5-step workflow tabs
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📦 Select Collection", 
            "👀 View Products", 
            "🏷️ Analyze Tags", 
            "🗑️ Remove Tags",
            "➕ Add Tags"
        ])
        
        with tab1:
            self._render_collection_selection_tab()
        
        with tab2:
            self._render_products_view_tab()
        
        with tab3:
            self._render_tag_analysis_tab()
        
        with tab4:
            self._render_tag_removal_tab()
        
        with tab5:
            self._render_tag_addition_tab()

    def _render_collection_selection_tab(self):
        """Render the collection selection tab."""
        st.header("📦 Step 1: Select Collection")
        st.write("Choose a collection to manage tags for its products")
        
        # Load collections if not already loaded
        if self.COLLECTIONS_KEY not in st.session_state:
            with st.spinner("Loading collections from Shopify..."):
                try:
                    collections = self.collection_resource.get_collection_names_for_dropdown()
                    st.session_state[self.COLLECTIONS_KEY] = collections
                    st.success(f"✅ Loaded {len(collections)} collections")
                except Exception as e:
                    st.error(f"❌ Error loading collections: {str(e)}")
                    return
        
        collections = st.session_state[self.COLLECTIONS_KEY]
        
        if not collections:
            st.warning("⚠️ No collections found in your store")
            return
        
        # Collection selection dropdown
        st.subheader("🔍 Choose Collection")
        
        collection_options = [f"{c['name']} ({c['product_count']} products)" for c in collections]
        collection_names = [c['name'] for c in collections]
        
        selected_index = st.selectbox(
            "Select Collection:",
            range(len(collection_options)),
            format_func=lambda i: collection_options[i],
            key="collection_dropdown"
        )
        
        selected_collection = collections[selected_index]
        
        # Display selected collection info
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Collection Name", selected_collection['name'])
        with col2:
            st.metric("Collection ID", selected_collection['id'])
        with col3:
            st.metric("Product Count", selected_collection['product_count'])
        
        # Load products button
        if st.button("🔄 Load Products from Collection", type="primary", use_container_width=True):
            with st.spinner(f"Loading products from '{selected_collection['name']}'..."):
                try:
                    # Store selected collection
                    st.session_state[self.SELECTED_COLLECTION_KEY] = selected_collection
                    
                    # Load products
                    products = self.collection_resource.get_products_in_collection(selected_collection['id'])
                    product_cards = self.collection_resource.products_to_card_data(products)
                    st.session_state[self.COLLECTION_PRODUCTS_KEY] = product_cards
                    
                    # Analyze tags
                    tag_analysis = self.collection_resource.analyze_tags_in_collection(selected_collection['id'])
                    st.session_state[self.TAG_ANALYSIS_KEY] = tag_analysis
                    
                    st.success(f"✅ Loaded {len(product_cards)} products and found {len(tag_analysis['unique_tags'])} unique tags!")
                    
                except Exception as e:
                    st.error(f"❌ Error loading products: {str(e)}")
                    logger.error(f"Error loading products for collection {selected_collection['id']}: {str(e)}")

    def _render_products_view_tab(self):
        """Render the products view tab."""
        st.header("👀 Step 2: View Products")
        st.write("Browse products in the selected collection")
        
        if self.SELECTED_COLLECTION_KEY not in st.session_state:
            st.warning("⚠️ Please select a collection first (Step 1)")
            return
        
        if self.COLLECTION_PRODUCTS_KEY not in st.session_state:
            st.warning("⚠️ Please load products from the collection first (Step 1)")
            return
        
        selected_collection = st.session_state[self.SELECTED_COLLECTION_KEY]
        products = st.session_state[self.COLLECTION_PRODUCTS_KEY]
        
        st.subheader(f"📦 Products in '{selected_collection['name']}'")
        st.write(f"**{len(products)} products** found in this collection")

        # print product

        for product in products:
            print(f"Product is : {product}")
            break


        
        if not products:
            st.info("ℹ️ No products found in this collection")
            return
        
        # Product statistics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            active_count = len([p for p in products if p.get('status') == 'active'])
            st.metric("Active Products", active_count)
        with col2:
            tagged_count = len([p for p in products if p.get('tags')])
            st.metric("Tagged Products", tagged_count)
        with col3:
            avg_tags = sum(len(p.get('tags', [])) for p in products) / len(products) if products else 0
            st.metric("Avg Tags/Product", f"{avg_tags:.1f}")
        with col4:
            variants_count = sum(p.get('variant_count', 1) for p in products)
            st.metric("Total Variants", variants_count)
        
        # Search and filter options
        col1, col2 = st.columns([2, 1])
        with col1:
            search_term = st.text_input("🔍 Search products by name:", key="product_search")
        with col2:
            show_untagged = st.checkbox("Show only untagged products", key="show_untagged_filter")
        
        # Filter products based on search and options
        filtered_products = products
        
        if search_term:
            filtered_products = [p for p in filtered_products 
                               if search_term.lower() in p.get('title', '').lower()]
        
        if show_untagged:
            filtered_products = [p for p in filtered_products 
                               if not p.get('tags') or len(p.get('tags', [])) == 0]
        
        st.write(f"**Showing {len(filtered_products)} of {len(products)} products**")
        
        # Display products in card format (4 columns)
        self._render_product_cards(filtered_products)

    def _render_product_cards(self, products: List[Dict]):
        """Render products in card format with 4 columns."""
        if not products:
            st.info("ℹ️ No products match the current filters")
            return
        
        # Pagination
        items_per_page = 20
        total_pages = (len(products) + items_per_page - 1) // items_per_page
        
        if total_pages > 1:
            page = st.selectbox("Page:", range(1, total_pages + 1), key="product_page") - 1
            start_idx = page * items_per_page
            end_idx = min(start_idx + items_per_page, len(products))
            products_to_show = products[start_idx:end_idx]
            st.write(f"Showing products {start_idx + 1}-{end_idx} of {len(products)}")
        else:
            products_to_show = products
        
        # Render cards in 4-column grid
        cols_per_row = 4
        for i in range(0, len(products_to_show), cols_per_row):
            cols = st.columns(cols_per_row)
            
            for j in range(cols_per_row):
                if i + j < len(products_to_show):
                    product = products_to_show[i + j]
                    with cols[j]:
                        self._render_single_product_card(product)

    def _render_single_product_card(self, product: Dict):
        """Render a single product card."""
        with st.container():
            # Product image
            if product.get('image_url'):
                st.image(product['image_url'], use_container_width=True)
            else:
                st.write("📷 No Image")
            
            # Product title
            st.write(f"**{product['title'][:50]}{'...' if len(product['title']) > 50 else ''}**")
            
            # Product details
            st.write(f"💰 **Price:** ${product.get('price', 'N/A')}")
            st.write(f"📦 **SKU:** {product.get('sku', 'No SKU')}")
            st.write(f"📊 **Stock:** {product.get('inventory_quantity', 'N/A')}")
            
            # Tags
            tags = product.get('tags', [])
            if tags:
                # Show first 3 tags + count if more
                display_tags = tags[:3]
                tag_text = ", ".join(display_tags)
                if len(tags) > 3:
                    tag_text += f" (+{len(tags) - 3} more)"
                st.write(f"🏷️ **Tags:** {tag_text}")
            else:
                st.write("🏷️ **Tags:** No tags")
            
            # Expandable details
            with st.expander("View Details"):
                st.write(f"**Product ID:** {product.get('id')}")
                st.write(f"**Handle:** {product.get('handle')}")
                st.write(f"**Status:** {product.get('status', 'Unknown')}")
                st.write(f"**Product Type:** {product.get('product_type', 'N/A')}")
                st.write(f"**Vendor:** {product.get('vendor', 'N/A')}")
                st.write(f"**Variants:** {product.get('variant_count', 1)}")
                
                if tags:
                    st.write("**All Tags:**")
                    for tag in tags:
                        st.write(f"  • {tag}")
            
            st.write("---")

    def _render_tag_analysis_tab(self):
        """Render the tag analysis tab."""
        st.header("🏷️ Step 3: Analyze Tags")
        st.write("Review and analyze tags used in the collection")
        
        if self.TAG_ANALYSIS_KEY not in st.session_state:
            st.warning("⚠️ Please load products and analyze tags first (Step 1)")
            return
        
        selected_collection = st.session_state.get(self.SELECTED_COLLECTION_KEY, {})
        tag_analysis = st.session_state[self.TAG_ANALYSIS_KEY]
        
        st.subheader(f"📊 Tag Analysis for '{selected_collection.get('name', 'Unknown Collection')}'")
        
        # Tag statistics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Products", tag_analysis['total_products'])
        with col2:
            st.metric("Unique Tags", len(tag_analysis['unique_tags']))
        with col3:
            st.metric("Tagged Products", 
                     tag_analysis['total_products'] - len(tag_analysis['untagged_products']))
        with col4:
            st.metric("Untagged Products", len(tag_analysis['untagged_products']))
        
        # Tag frequency analysis
        st.subheader("📈 Tag Frequency (Most Used First)")
        
        if tag_analysis['sorted_tags']:
            # Create frequency DataFrame
            freq_data = []
            for tag, count in tag_analysis['sorted_tags']:
                percentage = (count / tag_analysis['total_products']) * 100
                freq_data.append({
                    'Tag': tag,
                    'Product Count': count,
                    'Percentage': f"{percentage:.1f}%"
                })
            
            freq_df = pd.DataFrame(freq_data)
            st.dataframe(freq_df, use_container_width=True)
            
            # Tag selection for removal
            st.subheader("🎯 Select Tag for Removal")
            st.write("Choose a tag to remove from all products in this collection")
            
            # Create tag options with frequency
            tag_options = [f"{tag} ({count} products)" 
                          for tag, count in tag_analysis['sorted_tags']]
            tag_names = [tag for tag, count in tag_analysis['sorted_tags']]
            
            selected_tag_index = st.selectbox(
                "Select tag to remove:",
                range(len(tag_options)),
                format_func=lambda i: tag_options[i],
                key="tag_removal_selection"
            )
            
            selected_tag = tag_names[selected_tag_index]
            tag_count = tag_analysis['sorted_tags'][selected_tag_index][1]
            
            # Store selected tag for next step
            st.session_state[self.SELECTED_TAG_KEY] = {
                'tag_name': selected_tag,
                'product_count': tag_count,
                'products': tag_analysis['products_by_tag'][selected_tag]
            }
            
            # Show preview of affected products
            st.write(f"**Tag Selected:** `{selected_tag}`")
            st.write(f"**Products Affected:** {tag_count}")
            
            # Show sample of affected products
            affected_products = tag_analysis['products_by_tag'][selected_tag]
            if affected_products:
                st.write("**Sample of affected products:**")
                sample_products = affected_products[:5]
                for product in sample_products:
                    st.write(f"• {product['title']} (SKU: {product['sku']})")
                
                if len(affected_products) > 5:
                    st.write(f"... and {len(affected_products) - 5} more products")
        
        else:
            st.info("ℹ️ No tags found in this collection")
        
        # Show untagged products if any
        if tag_analysis['untagged_products']:
            with st.expander(f"👁️ View {len(tag_analysis['untagged_products'])} Untagged Products"):
                for product in tag_analysis['untagged_products'][:10]:
                    st.write(f"• {product['title']} (SKU: {product.get('sku', 'No SKU')})")
                
                if len(tag_analysis['untagged_products']) > 10:
                    st.write(f"... and {len(tag_analysis['untagged_products']) - 10} more")

    def _render_tag_removal_tab(self):
        """Render the tag removal tab."""
        st.header("🗑️ Step 4: Remove Tags")
        st.write("Preview and execute tag removal from products")
        
        if self.SELECTED_TAG_KEY not in st.session_state:
            st.warning("⚠️ Please select a tag for removal first (Step 3)")
            return
        
        selected_tag_info = st.session_state[self.SELECTED_TAG_KEY]
        tag_name = selected_tag_info['tag_name']
        product_count = selected_tag_info['product_count']
        affected_products = selected_tag_info['products']
        
        # Tag removal preview
        st.subheader("🔍 Tag Removal Preview")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Tag to Remove", f'"{tag_name}"')
        with col2:
            st.metric("Products Affected", product_count)
        with col3:
            time_estimate = self.tag_manager.estimate_operation_time(product_count)
            st.metric("Estimated Time", time_estimate['formatted_time'])
        
        # Create preview
        preview = self.tag_manager.preview_tag_removal(affected_products, tag_name)
        
        # Show warnings if any
        if preview['warnings']:
            for warning in preview['warnings']:
                st.warning(f"⚠️ {warning}")
        
        # Show preview of affected products
        st.write("**Products that will be affected:**")
        preview_products = preview['products_preview']
        
        # Display in a nice table
        preview_data = []
        for product in preview_products:
            preview_data.append({
                'Product Name': product['title'],
                'SKU': product.get('sku', 'No SKU'),
                'Product ID': product['id']
            })
        
        preview_df = pd.DataFrame(preview_data)
        st.dataframe(preview_df, use_container_width=True)
        
        if preview['additional_products'] > 0:
            st.write(f"... and **{preview['additional_products']} more products**")
        
        # Validation
        is_valid, validation_errors = self.tag_manager.validate_tag_operation_request(
            tag_name, affected_products, 'remove'
        )
        
        if validation_errors:
            for error in validation_errors:
                if error.startswith("Warning:"):
                    st.warning(error)
                else:
                    st.error(error)
        
        # Tag removal execution
        st.subheader("🚀 Execute Tag Removal")
        
        if is_valid:
            # Confirmation checkbox
            confirm_removal = st.checkbox(
                f'I confirm I want to remove tag "{tag_name}" from {product_count} products',
                key="confirm_tag_removal"
            )
            
            col1, col2 = st.columns([1, 1])
            
            with col1:
                if st.button("🗑️ Remove Tag from All Products", 
                           type="primary", 
                           disabled=not confirm_removal,
                           use_container_width=True):
                    
                    # Create backup
                    backup = self.tag_manager.backup_product_tags(affected_products)
                    st.session_state['tag_backup'] = backup
                    
                    # Execute removal
                    with st.spinner(f"Removing tag '{tag_name}' from {product_count} products..."):
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        product_ids = [p['id'] for p in affected_products]
                        
                        # Process in batches for progress tracking
                        batch_size = 10
                        all_results = []
                        
                        for i in range(0, len(product_ids), batch_size):
                            batch = product_ids[i:i + batch_size]
                            batch_num = (i // batch_size) + 1
                            total_batches = (len(product_ids) + batch_size - 1) // batch_size
                            
                            status_text.text(f"Processing batch {batch_num}/{total_batches}...")
                            
                            batch_results = self.tag_manager.remove_tag_from_products(batch, tag_name)
                            all_results.extend(batch_results)
                            
                            # Update progress
                            progress = min(1.0, (i + len(batch)) / len(product_ids))
                            progress_bar.progress(progress)
                        
                        # Store results
                        st.session_state[self.TAG_REMOVAL_RESULTS_KEY] = all_results
                        
                        # Show completion
                        progress_bar.progress(1.0)
                        status_text.text("✅ Tag removal completed!")
                        
                        successful = len([r for r in all_results if r.success])
                        failed = len(all_results) - successful
                        
                        if failed == 0:
                            st.success(f"🎉 Successfully removed tag '{tag_name}' from all {successful} products!")
                        else:
                            st.warning(f"⚠️ Completed with mixed results: {successful} successful, {failed} failed")
            
            with col2:
                if st.button("📊 Create Removal Report", 
                           use_container_width=True,
                           disabled=self.TAG_REMOVAL_RESULTS_KEY not in st.session_state):
                    self._show_removal_results()
        
        else:
            st.error("❌ Cannot proceed with tag removal due to validation errors above")
        
        # Show results if available
        if self.TAG_REMOVAL_RESULTS_KEY in st.session_state:
            self._show_removal_results()

    def _render_tag_addition_tab(self):
        """Render the tag addition tab."""
        st.header("➕ Step 5: Add Tags to Collection")
        st.write("Add a new tag to all products in the selected collection")
        
        if self.SELECTED_COLLECTION_KEY not in st.session_state:
            st.warning("⚠️ Please select a collection first (Step 1)")
            return
        
        if self.COLLECTION_PRODUCTS_KEY not in st.session_state:
            st.warning("⚠️ Please load products from the collection first (Step 1)")
            return
        
        selected_collection = st.session_state[self.SELECTED_COLLECTION_KEY]
        products = st.session_state[self.COLLECTION_PRODUCTS_KEY]
        
        st.subheader(f"➕ Add Tag to '{selected_collection['name']}'")
        st.write(f"Add a new tag to **{len(products)} products** in this collection")
        
        if not products:
            st.info("ℹ️ No products found in this collection")
            return
        
        # Tag input section
        st.subheader("🏷️ New Tag Details")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            new_tag = st.text_input(
                "Enter new tag name:",
                key="new_tag_input",
                placeholder="e.g., summer-2024, new-arrival, featured",
                help="Enter the tag you want to add to all products in this collection"
            )
        
        with col2:
            add_to_all = st.checkbox(
                "Add to ALL products",
                value=True,
                help="Add tag to all products in collection"
            )
        
        if new_tag:
            # Store the new tag
            st.session_state[self.NEW_TAG_KEY] = new_tag.strip()
            
            # Validate tag name
            if ',' in new_tag:
                st.error("❌ Tag cannot contain commas")
                return
            
            if len(new_tag) > 255:
                st.error("❌ Tag name too long (maximum 255 characters)")
                return
            
            # Get products that don't have this tag already
            products_without_tag = self.tag_manager.get_products_without_tag(products, new_tag.strip())
            products_with_tag = [p for p in products if p not in products_without_tag]
            
            # Show tag statistics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Products", len(products))
            with col2:
                st.metric("Already Have Tag", len(products_with_tag))
            with col3:
                st.metric("Will Get Tag", len(products_without_tag))
            with col4:
                time_estimate = self.tag_manager.estimate_operation_time(len(products_without_tag))
                st.metric("Estimated Time", time_estimate['formatted_time'])
            
            if products_with_tag:
                st.info(f"ℹ️ {len(products_with_tag)} products already have the tag '{new_tag}'")
            
            if not products_without_tag:
                st.warning("⚠️ All products already have this tag!")
                return
            
            # Preview section
            st.subheader("🔍 Addition Preview")
            
            # Create preview
            preview = self.tag_manager.preview_tag_addition(products_without_tag, new_tag.strip())
            
            # Show warnings if any
            if preview['warnings']:
                for warning in preview['warnings']:
                    st.warning(f"⚠️ {warning}")
            
            # Show preview of affected products
            st.write("**Products that will get the new tag:**")
            preview_products = preview['products_preview']
            
            # Display in a nice table
            preview_data = []
            for product in preview_products:
                current_tags = ", ".join(product.get('tags', [])) if product.get('tags') else "No tags"
                preview_data.append({
                    'Product Name': product['title'],
                    'SKU': product.get('sku', 'No SKU'),
                    'Current Tags': current_tags,
                    'Product ID': product['id']
                })
            
            preview_df = pd.DataFrame(preview_data)
            st.dataframe(preview_df, use_container_width=True)
            
            if preview['additional_products'] > 0:
                st.write(f"... and **{preview['additional_products']} more products**")
            
            # Validation
            is_valid, validation_errors = self.tag_manager.validate_tag_operation_request(
                new_tag.strip(), products_without_tag, 'add'
            )
            
            if validation_errors:
                for error in validation_errors:
                    if error.startswith("Warning:"):
                        st.warning(error)
                    else:
                        st.error(error)
            
            # Tag addition execution
            st.subheader("🚀 Execute Tag Addition")
            
            if is_valid:
                # Confirmation checkbox
                confirm_addition = st.checkbox(
                    f'I confirm I want to add tag "{new_tag}" to {len(products_without_tag)} products',
                    key="confirm_tag_addition"
                )
                
                col1, col2 = st.columns([1, 1])
                
                with col1:
                    if st.button("➕ Add Tag to All Products", 
                               type="primary", 
                               disabled=not confirm_addition,
                               use_container_width=True):
                        
                        # Create backup
                        backup = self.tag_manager.backup_product_tags(products_without_tag)
                        st.session_state['tag_addition_backup'] = backup
                        
                        # Execute addition
                        with st.spinner(f"Adding tag '{new_tag}' to {len(products_without_tag)} products..."):
                            progress_bar = st.progress(0)
                            status_text = st.empty()
                            
                            product_ids = [p['id'] for p in products_without_tag]
                            
                            # Process in batches for progress tracking
                            batch_size = 10
                            all_results = []
                            
                            for i in range(0, len(product_ids), batch_size):
                                batch = product_ids[i:i + batch_size]
                                batch_num = (i // batch_size) + 1
                                total_batches = (len(product_ids) + batch_size - 1) // batch_size
                                
                                status_text.text(f"Processing batch {batch_num}/{total_batches}...")
                                
                                batch_results = self.tag_manager.add_tag_to_products(batch, new_tag.strip())
                                all_results.extend(batch_results)
                                
                                # Update progress
                                progress = min(1.0, (i + len(batch)) / len(product_ids))
                                progress_bar.progress(progress)
                            
                            # Store results
                            st.session_state[self.TAG_ADDITION_RESULTS_KEY] = all_results
                            
                            # Show completion
                            progress_bar.progress(1.0)
                            status_text.text("✅ Tag addition completed!")
                            
                            successful = len([r for r in all_results if r.success])
                            failed = len(all_results) - successful
                            
                            if failed == 0:
                                st.success(f"🎉 Successfully added tag '{new_tag}' to all {successful} products!")
                            else:
                                st.warning(f"⚠️ Completed with mixed results: {successful} successful, {failed} failed")
                
                with col2:
                    if st.button("📊 Create Addition Report", 
                               use_container_width=True,
                               disabled=self.TAG_ADDITION_RESULTS_KEY not in st.session_state):
                        self._show_addition_results()
            
            else:
                st.error("❌ Cannot proceed with tag addition due to validation errors above")
            
            # Show results if available
            if self.TAG_ADDITION_RESULTS_KEY in st.session_state:
                self._show_addition_results()

    def _show_removal_results(self):
        """Display tag removal results."""
        if self.TAG_REMOVAL_RESULTS_KEY not in st.session_state:
            return
        
        results = st.session_state[self.TAG_REMOVAL_RESULTS_KEY]
        report = self.tag_manager.create_tag_operation_report(results)
        
        st.write("---")
        st.subheader("📊 Tag Removal Results")
        
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Processed", report['summary']['total_processed'])
        with col2:
            st.metric("✅ Successful", report['summary']['successful_operations'])
        with col3:
            st.metric("❌ Failed", report['summary']['failed_operations'])
        with col4:
            st.metric("Success Rate", f"{report['summary']['success_rate']:.1f}%")
        
        # Detailed results
        if report['successful_products']:
            with st.expander(f"✅ View {len(report['successful_products'])} Successful Removals"):
                success_df = pd.DataFrame(report['successful_products'])
                st.dataframe(success_df, use_container_width=True)
        
        if report['failed_products']:
            with st.expander(f"❌ View {len(report['failed_products'])} Failed Removals"):
                failed_df = pd.DataFrame(report['failed_products'])
                st.dataframe(failed_df, use_container_width=True)
        
        # Error analysis
        if report['error_analysis']:
            st.write("**Error Analysis:**")
            for error_type, count in report['error_analysis'].items():
                st.write(f"• {error_type}: {count} products")
        
        # Download options
        st.subheader("📥 Export Results")
        col1, col2 = st.columns(2)
        
        with col1:
            # Full results download
            if results:
                results_data = []
                for result in results:
                    results_data.append({
                        'Product ID': result.product_id,
                        'Product Title': result.product_title,
                        'Tag Removed': result.tag_operated,
                        'Success': 'Yes' if result.success else 'No',
                        'Error Message': result.error_message,
                        'Updated At': result.updated_at.strftime('%Y-%m-%d %H:%M:%S') if result.updated_at else 'N/A'
                    })
                
                results_df = pd.DataFrame(results_data)
                csv_data = results_df.to_csv(index=False)
                
                st.download_button(
                    label="📄 Download Full Report",
                    data=csv_data,
                    file_name=f"tag_removal_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
        
        with col2:
            # Backup download
            if 'tag_backup' in st.session_state:
                backup_data = st.session_state['tag_backup']
                backup_csv = pd.DataFrame([
                    {'Product ID': pid, 'Original Tags': tags} 
                    for pid, tags in backup_data.items() 
                    if pid not in ['backup_timestamp', 'backup_count']
                ]).to_csv(index=False)
                
                st.download_button(
                    label="💾 Download Tag Backup",
                    data=backup_csv,
                    file_name=f"tag_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )

    def _show_addition_results(self):
        """Display tag addition results."""
        if self.TAG_ADDITION_RESULTS_KEY not in st.session_state:
            return
        
        results = st.session_state[self.TAG_ADDITION_RESULTS_KEY]
        report = self.tag_manager.create_tag_operation_report(results)
        
        st.write("---")
        st.subheader("📊 Tag Addition Results")
        
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Processed", report['summary']['total_processed'])
        with col2:
            st.metric("✅ Successful", report['summary']['successful_operations'])
        with col3:
            st.metric("❌ Failed", report['summary']['failed_operations'])
        with col4:
            st.metric("Success Rate", f"{report['summary']['success_rate']:.1f}%")
        
        # Detailed results
        if report['successful_products']:
            with st.expander(f"✅ View {len(report['successful_products'])} Successful Additions"):
                success_df = pd.DataFrame(report['successful_products'])
                st.dataframe(success_df, use_container_width=True)
        
        if report['failed_products']:
            with st.expander(f"❌ View {len(report['failed_products'])} Failed Additions"):
                failed_df = pd.DataFrame(report['failed_products'])
                st.dataframe(failed_df, use_container_width=True)
        
        # Error analysis
        if report['error_analysis']:
            st.write("**Error Analysis:**")
            for error_type, count in report['error_analysis'].items():
                st.write(f"• {error_type}: {count} products")
        
        # Download options
        st.subheader("📥 Export Results")
        col1, col2 = st.columns(2)
        
        with col1:
            # Full results download
            if results:
                results_data = []
                for result in results:
                    results_data.append({
                        'Product ID': result.product_id,
                        'Product Title': result.product_title,
                        'Tag Added': result.tag_operated,
                        'Success': 'Yes' if result.success else 'No',
                        'Error Message': result.error_message,
                        'Updated At': result.updated_at.strftime('%Y-%m-%d %H:%M:%S') if result.updated_at else 'N/A'
                    })
                
                results_df = pd.DataFrame(results_data)
                csv_data = results_df.to_csv(index=False)
                
                st.download_button(
                    label="📄 Download Addition Report",
                    data=csv_data,
                    file_name=f"tag_addition_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
        
        with col2:
            # Backup download
            if 'tag_addition_backup' in st.session_state:
                backup_data = st.session_state['tag_addition_backup']
                backup_csv = pd.DataFrame([
                    {'Product ID': pid, 'Original Tags': tags} 
                    for pid, tags in backup_data.items() 
                    if pid not in ['backup_timestamp', 'backup_count']
                ]).to_csv(index=False)
                
                st.download_button(
                    label="💾 Download Tag Backup",
                    data=backup_csv,
                    file_name=f"tag_addition_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
        
        # Clear results button
        if st.button("🧹 Clear Addition Results", type="secondary", key="clear_addition_results"):
            # Clear relevant session state
            keys_to_clear = [
                self.TAG_ADDITION_RESULTS_KEY,
                self.NEW_TAG_KEY,
                'tag_addition_backup'
            ]
            for key in keys_to_clear:
                if key in st.session_state:
                    del st.session_state[key]
            
            st.success("✅ Addition results cleared!")
            st.rerun()

    def clear_all_results(self):
        """Clear all results and start over."""
        keys_to_clear = [
            self.TAG_REMOVAL_RESULTS_KEY,
            self.SELECTED_TAG_KEY,
            self.TAG_ADDITION_RESULTS_KEY,
            self.NEW_TAG_KEY,
            'tag_backup',
            'tag_addition_backup'
        ]
        for key in keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]
        
        st.success("✅ All results cleared! You can now start over with tag operations.")
        st.rerun()

# Main function to run the dashboard
def main():
    """Main function to run the collection tag management dashboard."""
    st.set_page_config(
        page_title="Shopify Tag Manager",
        page_icon="🏷️",
        layout="wide"
    )
    
    st.info("💡 To use this dashboard, initialize it with your Shopify connector in your main app")
    
    # Example usage shown in comments
    st.code("""
# Example usage in your main app:
from utils.shopify.shopify_connector import ShopifyConnector
from utils.shopify.collection_tag_dashboard import CollectionTagDashboard

# Initialize connector
connector = ShopifyConnector(shop_url, api_version, access_token)
connector.connect()

# Create and run dashboard
dashboard = CollectionTagDashboard(connector)
dashboard.run_dashboard()
""")

if __name__ == "__main__":
    main()