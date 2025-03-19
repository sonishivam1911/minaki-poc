import streamlit as st
from config.logger import logger
from server.create_shiprocket_for_sales_orders import sales_order_id_number_mapping_dict, create_shiprocket_for_sales_orders, create_shiprocket_sr_forward


def shiprocket_streamlit_interface():
    st.title("Create Shiprocket Shipment")
    
    # Initialize state variables if they don't exist
    if 'courier_df' not in st.session_state:
        st.session_state['courier_df'] = None
    if 'courier_fetched' not in st.session_state:
        st.session_state['courier_fetched'] = False
    
    sales_order_mapping = sales_order_id_number_mapping_dict()
    
    # Use a form for the initial input to prevent reloading on each change
    with st.form(key="shipment_details_form"):
        selected_so_number = st.selectbox(
            "Select Sales Order",
            options=list(sales_order_mapping.values()),
            format_func=lambda x: f"SO-{x}"
        )
        
        # Package details
        st.subheader("Package Details")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            length = st.number_input("Length (cm)", min_value=1, value=10)
        with col2:
            breadth = st.number_input("Breadth (cm)", min_value=1, value=10)
        with col3:
            height = st.number_input("Height (cm)", min_value=1, value=10)
        with col4:
            weight = st.number_input("Weight (kg)", min_value=0.1, value=0.5)
        
        # Contact person details
        st.subheader("Contact Person Details")
        col1, col2 = st.columns(2)
        with col1:
            contact_name = st.text_input("Contact Name", value="")
            contact_phone = st.text_input("Contact Phone", value="")
        with col2:
            contact_email = st.text_input("Contact Email", value="")
        
        # Submit button for the form
        check_couriers_submitted = st.form_submit_button("Check Available Couriers")
    
    # Process form submission
    if check_couriers_submitted:
        # Get the selected sales order ID from the mapping
        selected_so_id = [k for k, v in sales_order_mapping.items() if v == selected_so_number][0]
        
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
                logger.error(f"Error fetching couriers: {str(e)}")
    
    # Display courier selection if couriers have been fetched
    if st.session_state.get('courier_fetched', False) and st.session_state.get('courier_df') is not None:
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
                
                logger.debug(f"Contact person: {contact_person}")
                logger.debug(f"Selected courier: {selected_courier_id}")
                
                result = create_shiprocket_sr_forward(config)
                
                if result and 'shipment_id' in result:
                    st.success(f'Booking Confirmed! Shipment ID: {result["shipment_id"]}')
                    # Show additional shipment details if available
                    if 'tracking_number' in result:
                        st.info(f'Tracking Number: {result["tracking_number"]}')
                    if 'label_url' in result:
                        st.markdown(f'[Download Label]({result["label_url"]})')
                else:
                    st.error(f'Error creating shipment: {result.get("message", "Unknown error")}')


def display_and_select_couriers(courier_df):
    """Display courier options and get user selection"""
    if courier_df is None or len(courier_df) == 0:
        st.warning("No couriers available for this destination and weight")
        return None
    
    # Display courier options in a user-friendly format
    st.subheader("Available Couriers")
    
    # Create a selection dataframe with relevant columns
    display_df = courier_df[['id','courier_name', 'freight_charge', 
                                'base_courier_id','pickup_availability',  'air_max_weight',
                                'charge_weight','city', 'cod', 'rate', 
                                'etd', 'rating', 
                                'cod_charges', 'cod_multiplier', 
                                'cost','courier_company_id',
                                'pickup_performance', 'pickup_priority', 'surface_max_weight', 
                                'tracking_performance','volumetric_max_weight', 'weight_cases',
                                ]]
    
    # Add formatted display column
    # display_df['Price & ETA'] = display_df.apply(
    #     lambda x: f"₹{x['rate']} - {x['etd']} days", axis=1
    # )
    
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
        return f"{courier_name} - ₹{rate} ({etd} days)"
    
    selected_courier = st.selectbox(
        "Select Courier Service",
        options=courier_options,
        format_func=format_courier_option
    )
    
    return selected_courier


if __name__ == "__main__":
    shiprocket_streamlit_interface()