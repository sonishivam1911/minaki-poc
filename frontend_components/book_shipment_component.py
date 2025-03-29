
import streamlit as st
from server.create_shiprocket_for_sales_orders import create_shiprocket_for_sales_orders, create_shiprocket_sr_forward
from core.courier_booking_helper import display_shipment_results

def book_shipment_component(selected_so_id):
    length, breadth, height, weight, contact_name, contact_phone, contact_email, check_couriers_submitted = shipment_form_component()
    
    # Process form submission
    if check_couriers_submitted:
        # Store values in session state for later use
        st.session_state['selected_so_id'] = selected_so_id
        st.session_state['package_length'] = length
        st.session_state['package_breadth'] = breadth
        st.session_state['package_height'] = height
        st.session_state['package_weight'] = weight
        st.session_state['contact_name'] = contact_name
        st.session_state['contact_phone'] = contact_phone
        st.session_state['contact_email'] = contact_email
        
        # Fetch couriers
        with st.spinner('Fetching available couriers...'):
            try:
                courier_df, token = create_shiprocket_for_sales_orders(selected_so_id, weight)
                
                # Save in session state
                st.session_state['courier_df'] = courier_df
                st.session_state['token'] = token
                st.session_state['courier_fetched'] = True
            except Exception as e:
                st.error(f"Error fetching couriers: {str(e)}")
                #logger.error(f"Error fetching couriers: {str(e)}")    
        
        # Display courier selection if couriers have been fetched
        if st.session_state.get('courier_fetched', False) and st.session_state.get('courier_df') is not None:
            st.subheader("3. Select Courier Service")
            # Get courier selection without triggering reload on each input change
            with st.form(key="courier_selection_form"):
                selected_courier_id = display_and_select_couriers(st.session_state['courier_df'])
                booking_submit = st.form_submit_button("Confirm Courier Booking")
            
            if booking_submit:
                # Get values from session state
                selected_so_id = st.session_state['selected_so_id']
                length = st.session_state['package_length']
                breadth = st.session_state['package_breadth']
                height = st.session_state['package_height']
                weight = st.session_state['package_weight']
                contact_name = st.session_state['contact_name']
                contact_phone = st.session_state['contact_phone']
                contact_email = st.session_state['contact_email']
                
                # Validate contact information
                if not contact_name or not contact_phone:
                    st.error("Contact name and phone are required")
                else:
                    # Create contact person JSON
                    contact_person = {
                        "name": contact_name,
                        "phone": contact_phone,
                        "email": contact_email
                    }
                    
                    # Add to config
                    config = {
                        'salesorder_id': selected_so_id,
                        'length': length,
                        'breadth': breadth,
                        'height': height,
                        'weight': weight,
                        'contact_person': contact_person,
                        'courier_id' : selected_courier_id
                    }
                    
                    # Store selected courier ID in session state
                    st.session_state['selected_courier'] = selected_courier_id
                    
                    ##logger.debug(f"Contact person: {contact_person}")
                    ##logger.debug(f"Selected courier: {selected_courier_id}")
                    
                    with st.spinner('Creating shipments and packages...'):
                        # Call function that now returns multiple results
                        result = create_shiprocket_sr_forward(config)
                        
                        if result and isinstance(result, tuple) and len(result) == 4:
                            # Unpack the result tuple
                            shiprocket_result, zakya_shipment_result, zakya_packages_result, crud_result = result
                            
                            if crud_result['status']:
                                st.success(f"{crud_result['message']}")
                            else:
                                st.warning(f"{crud_result['message']}")

                            # Display all results using the new function
                            display_shipment_results(shiprocket_result, zakya_shipment_result, zakya_packages_result)
                        elif result and isinstance(result, dict) and 'status' in result and result['status'] == 1:
                            # Handle case where only Shiprocket result is returned (backward compatibility)
                            st.success(f'Booking Confirmed! Shipment ID: {result["payload"]["shipment_id"]}')
                            
                            # Display more details about the Shiprocket result
                            with st.expander("View Shiprocket Details", expanded=True):
                                st.json(result)
                                
                                # Add download buttons for available documents
                                if result.get('payload', {}).get('label_url'):
                                    st.markdown(f'[Download Shipping Label]({result["payload"]["label_url"]})')
                                if result.get('payload', {}).get('manifest_url'):
                                    st.markdown(f'[Download Manifest]({result["payload"]["manifest_url"]})')
                        else:
                            st.error(f'Error creating shipment: {result}')

def shipment_form_component():
    st.subheader("2. Package and Contact Details")
    with st.form(key="shipment_details_form"):
        # Package details
        st.markdown("**Package Dimensions**")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            length = st.number_input("Length (cm)", min_value=1, max_value=100, value=25)
        with col2:
            breadth = st.number_input("Breadth (cm)", min_value=1, max_value=100, value=15)
        with col3:
            height = st.number_input("Height (cm)", min_value=1, max_value=100, value=10)
        with col4:
            weight = st.number_input("Weight (kg)", min_value=0.100, max_value=15.000, value=1.000)
        
        # Contact person details
        st.markdown("**Contact Person Details**")
        col1, col2 = st.columns(2)
        with col1:
            contact_name = st.text_input("Contact Name", value="")
        with col2:
            contact_phone = st.text_input("Contact Phone", value="")
            contact_email = 'noreply@shiprocket.in'
        
        # Submit button for the form
        check_couriers_submitted = st.form_submit_button("Check Available Couriers")
    return length,breadth,height,weight,contact_name,contact_phone,contact_email,check_couriers_submitted


def display_and_select_couriers(courier_df):
    """Display courier options and get user selection"""
    if courier_df is None or len(courier_df) == 0:
        st.warning("No couriers available for this destination and weight")
        return None

    # Display courier options in a user-friendly format    
    # Create a selection dataframe with relevant columns
    display_df = courier_df[['courier_company_id','courier_name', 'freight_charge', 
                                'charge_weight','city', 'rate', 
                                'etd'
                            ]]

    # Show the dataframe
    st.dataframe(display_df)

    # Create courier selection dropdown
    courier_options = courier_df['courier_company_id'].tolist()
    if not courier_options:
        return None

    def format_courier_option(courier_id):
        """Format the courier option for display in the selectbox"""
        courier_row = courier_df[courier_df['courier_company_id'] == courier_id]
        courier_name = courier_row['courier_name'].iloc[0]
        rate = courier_row['rate'].iloc[0]
        etd = courier_row['etd'].iloc[0]
        return f"{courier_name} - ₹{rate} (ETD -> {etd})"

    selected_courier = st.selectbox(
        "Select Courier Service",
        options=courier_options,
        format_func=format_courier_option
    )

    return selected_courier