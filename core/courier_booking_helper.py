import streamlit as st
from config.logger import logger
from utils.bhavvam.shiprocket import shiprocket_auth
from server.create_shiprocket_for_sales_orders import generate_manifest_service, generate_label_service

def display_shipment_results(shiprocket_result, zakya_shipment_result, zakya_packages_result):
    """
    Display the results of shipment creation in a well-formatted container
    
    Parameters:
    - shiprocket_result: Dictionary containing Shiprocket API response
    - zakya_shipment_result: Dictionary containing Zakya shipment order response
    - zakya_packages_result: Dictionary containing Zakya packages response
    """
    # Create main container for results
    with st.container():
        st.markdown("## Shipment Results")
        
        # Display tabs for different results
        tab1, tab2, tab3 = st.tabs(["Shiprocket", "Zakya Shipment", "Zakya Package"])
        
        # Shiprocket tab
        with tab1:
            if shiprocket_result and 'status' in shiprocket_result and shiprocket_result['status'] == 1:
                payload = shiprocket_result.get('payload', {})
                
                # Success message with shipment ID
                st.success("✅ Shiprocket shipment created successfully!")
                
                
                # Display details in columns
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("**Shipment Information**")
                    st.write(f"**Shipment ID:** {payload.get('shipment_id', 'N/A')}")
                    st.write(f"**Order ID:** {payload.get('order_id', 'N/A')}")
                    st.write(f"**AWB Code:** {payload.get('awb_code', 'N/A')}")
                    st.write(f"**Courier:** {payload.get('courier_name', 'N/A')}")
                    
                with col2:
                    st.markdown("**Pickup Information**")
                    st.write(f"**Pickup Scheduled:** {payload.get('pickup_scheduled_date', 'N/A')}")
                    st.write(f"**Pickup Token:** {payload.get('pickup_token_number', 'N/A')}")
                    st.write(f"**Routing Code:** {payload.get('routing_code', 'N/A')}")
                
                # Add download buttons for available documents
                if payload.get('label_url'):
                    st.markdown(f'[Download Shipping Label]({payload["label_url"]})')
                if payload.get('manifest_url'):
                    st.markdown(f'[Download Manifest]({payload["manifest_url"]})')
                # Store shipment_id in session state for use with the manifest and label generation
                if 'shipment_id' in payload:
                    st.session_state['shipment_id'] = payload['shipment_id']  

# Add manifest and label generation buttons
                st.markdown("---")
                st.markdown("**Generate Documents**")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("Generate Manifest"):
                        with st.spinner("Generating manifest..."):
                            try:
                                # Get the token from either the session state or Shiprocket auth
                                token = st.session_state.get('token')
                                if not token:
                                    auth_data = shiprocket_auth()
                                    token = auth_data['token']
                                
                                # Configure manifest generation
                                config = {
                                    'token': token,
                                    'shipment_ids': [st.session_state['shipment_id']]
                                }
                                
                                manifest_result = generate_manifest_service(config)
                                
                                # Handle the result
                                if manifest_result and isinstance(manifest_result, dict) and manifest_result.get('status') == 1:
                                    st.success("✅ Manifest generated successfully!")
                                    if 'manifest_url' in manifest_result:
                                        st.markdown(f'[Download Manifest]({manifest_result["manifest_url"]})')
                                    else:
                                        st.info("Manifest URL will be available shortly. Check Shiprocket dashboard.")
                                else:
                                    st.error("❌ Failed to generate manifest")
                                    st.json(manifest_result)
                            except Exception as e:
                                st.error(f"Error generating manifest: {str(e)}")
                                logger.error(f"Error generating manifest: {str(e)}")
                
                with col2:
                    if st.button("Generate Label"):
                        with st.spinner("Generating label..."):
                            try:
                                # Get the token from either the session state or Shiprocket auth
                                token = st.session_state.get('token')
                                if not token:
                                    from utils.bhavvam.shiprocket import shiprocket_auth
                                    auth_data = shiprocket_auth()
                                    token = auth_data['token']
                                
                                # Configure label generation
                                config = {
                                    'token': token,
                                    'shipment_ids': [st.session_state['shipment_id']]
                                }
                                
                                label_result = generate_label_service(config)
                                
                                # Handle the result
                                if label_result and isinstance(label_result, dict) and label_result.get('status') == 1:
                                    st.success("✅ Label generated successfully!")
                                    if 'label_url' in label_result:
                                        st.markdown(f'[Download Label]({label_result["label_url"]})')
                                    else:
                                        st.info("Label URL will be available shortly. Check Shiprocket dashboard.")
                                else:
                                    st.error("❌ Failed to generate label")
                                    st.json(label_result)
                            except Exception as e:
                                st.error(f"Error generating label: {str(e)}")
                                logger.error(f"Error generating label: {str(e)}")                                      
            else:
                st.error("❌ Shiprocket shipment creation failed")
                st.json(shiprocket_result)
        
        # Zakya Shipment tab
        with tab2:
            if zakya_shipment_result and "code" in zakya_shipment_result and zakya_shipment_result["code"] == 0:
                st.success("✅ Zakya shipment created successfully!")
                
                if "shipment_order" in zakya_shipment_result:
                    shipment = zakya_shipment_result["shipment_order"]
                    
                    # Display essential info in a highlighted box
                    st.info(f"**Shipment Number:** {shipment.get('shipment_number', 'N/A')}")
                    
                    # Display details in columns
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown("**Basic Information**")
                        st.write(f"**Shipment ID:** {shipment.get('shipment_id', 'N/A')}")
                        st.write(f"**Date:** {shipment.get('date', 'N/A')}")
                        st.write(f"**Status:** {shipment.get('status', 'N/A')}")
                        st.write(f"**Reference:** {shipment.get('reference_number', 'N/A')}")
                        
                    with col2:
                        st.markdown("**Delivery Information**")
                        st.write(f"**Delivery Method:** {shipment.get('delivery_method', 'N/A')}")
                        st.write(f"**Tracking Number:** {shipment.get('tracking_number', 'N/A')}")
                        st.write(f"**Shipping Charge:** {shipment.get('currency_symbol', '$')}{shipment.get('shipping_charge', 'N/A')}")
                    
                    # Display line items if available
                    if 'line_items' in shipment and shipment['line_items']:
                        st.markdown("**Line Items**")
                        for i, item in enumerate(shipment['line_items']):
                            st.write(f"**{i+1}.** {item.get('name', 'N/A')} - Qty: {item.get('quantity', 'N/A')}")
                    
                    # Display total amount
                    st.metric(
                        "Total Amount", 
                        f"{shipment.get('currency_symbol', '$')}{shipment.get('total', 'N/A')}"
                    )
                    
                    # Display any notes
                    if shipment.get('notes'):
                        st.markdown("**Notes**")
                        st.text(shipment.get('notes'))
            else:
                st.error("❌ Zakya shipment creation failed")
                st.json(zakya_shipment_result)
        
        # Zakya Package tab
        with tab3:
            if zakya_packages_result and isinstance(zakya_packages_result, dict):
                if "code" in zakya_packages_result and zakya_packages_result["code"] == 0:
                    st.success("✅ Zakya package created successfully!")
                    
                    # If there's a specific package structure in the result
                    if "package" in zakya_packages_result:
                        package = zakya_packages_result["package"]
                    else:
                        # If the package info is directly in the result
                        package = zakya_packages_result
                    
                    # Display essential info
                    st.info(f"**Package Number:** {package.get('package_number', 'N/A')}")
                    st.write(f"**Date:** {package.get('date', 'N/A')}")
                    
                    # Display line items if available
                    if 'line_items' in package and package['line_items']:
                        st.markdown("**Line Items**")
                        for i, item in enumerate(package['line_items']):
                            st.write(f"**{i+1}.** SO Line Item ID: {item.get('so_line_item_id', 'N/A')} - Qty: {item.get('quantity', 'N/A')}")
                    
                    # Display any notes
                    if package.get('notes'):
                        st.markdown("**Notes**")
                        st.text(package.get('notes'))
                else:
                    st.error("❌ Zakya package creation failed")
                    st.json(zakya_packages_result)
            else:
                st.warning("⚠️ No package information available")
                st.json(zakya_packages_result)


# Example usage in the main function:
# if booking_submit and result:
#     shiprocket_result, zakya_shipment_result, zakya_packages_result = result
#     display_shipment_results(shiprocket_result, zakya_shipment_result, zakya_packages_result)