import streamlit as st
from datetime import datetime
from config.logger import logger
from server.aza_invoice_service import analyze_aza_products

def aza_orders_section():
    """Create the Aza orders display section UI."""
    st.subheader("2️⃣ Aza Orders")
    
    if st.session_state.get('aza_orders') is None:
        st.info("No Aza orders to display. Please upload an Aza sales file above.")
        return
    
    df = st.session_state['aza_orders']
    
    # Display the orders table
    st.dataframe(df, use_container_width=True)
    
    # Display summary statistics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Orders", len(df))
    
    with col2:
        if "Total" in df.columns:
            total_value = df["Total"].sum()
            st.metric("Total Value", f"₹{total_value:,.2f}")
        elif "Rate" in df.columns:
            total_value = df["Rate"].sum()
            st.metric("Total Value", f"₹{total_value:,.2f}")
    
    with col3:
        unique_skus = df["SKU"].nunique() if "SKU" in df.columns else 0
        st.metric("Unique SKUs", unique_skus)
    
    # Download button and Analyze button
    col1, col2 = st.columns(2)
    with col1:
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download as CSV",
            data=csv,
            file_name=f"aza_orders_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )
    
    # Analyze Products button
    with col2:
        if st.button("Analyze Product Mapping", type="primary"):
            with st.spinner("Analyzing product mapping..."):
                # Call the backend service to analyze product mapping
                results = analyze_aza_products(df, sku_field="SKU")

                # logger.debug(f"Result after analyxe products is : {results}")
                
                # Store results in session state
                st.session_state['aza_mapped_products'] = results.get('mapped_products')
                st.session_state['aza_unmapped_products'] = results.get('unmapped_products')
                st.session_state['aza_product_mapping'] = results.get('product_mapping', {})
                
                # Display summary of analysis
                mapped_count = len(results.get('mapped_products', []))
                unmapped_count = len(results.get('unmapped_products', []))
                total_count = mapped_count + unmapped_count
                
                if total_count > 0:
                    mapped_percent = (mapped_count / total_count) * 100
                    st.success(f"Analysis complete: {mapped_count} products mapped ({mapped_percent:.1f}%), {unmapped_count} products not mapped in Zakya.")
                else:
                    st.warning("No products found to analyze.")