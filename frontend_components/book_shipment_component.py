import streamlit as st
from server.create_shiprocket_for_sales_orders import create_shiprocket_for_sales_orders, create_shiprocket_sr_forward
from core.courier_booking_helper import display_shipment_results

def book_shipment_component(selected_so_id):
    length, breadth, height, weight, contact_name, contact_phone, contact_email, check_couriers_submitted = shipment_form_component()

    if check_couriers_submitted:
        try:
            st.session_state['selected_so_id'] = selected_so_id
            st.session_state['package_length'] = length
            st.session_state['package_breadth'] = breadth
            st.session_state['package_height'] = height
            st.session_state['package_weight'] = weight
            st.session_state['contact_name'] = contact_name
            st.session_state['contact_phone'] = contact_phone
            st.session_state['contact_email'] = contact_email

            with st.spinner('Fetching available couriers...'):
                courier_df, token = create_shiprocket_for_sales_orders(selected_so_id, weight)
                st.session_state['courier_df'] = courier_df
                st.session_state['token'] = token
                st.session_state['courier_fetched'] = True
        except Exception as e:
            st.error(f"Error fetching couriers: {str(e)}")
            return

    if st.session_state.get('courier_fetched', False) and st.session_state.get('courier_df') is not None:
        st.subheader("3. Select Courier Service")
        with st.form(key="courier_selection_form"):
            selected_courier_id = display_and_select_couriers(st.session_state['courier_df'])
            booking_submit = st.form_submit_button("Confirm Courier Booking")

        if booking_submit:
            if not contact_name or not contact_phone:
                st.error("Contact name and phone are required")
            else:
                config = {
                    'salesorder_id': selected_so_id,
                    'length': length,
                    'breadth': breadth,
                    'height': height,
                    'weight': weight,
                    'contact_person': {
                        "name": contact_name,
                        "phone": contact_phone,
                        "email": contact_email
                    },
                    'courier_id': selected_courier_id
                }
                try:
                    with st.spinner('Creating shipments and packages...'):
                        result = create_shiprocket_sr_forward(config)

                    if isinstance(result, tuple) and len(result) == 4:
                        shiprocket_result, zakya_shipment_result, zakya_packages_result, crud_result = result
                        if crud_result['status']:
                            st.success(f"{crud_result['message']}")
                        else:
                            st.warning(f"{crud_result['message']}")
                        display_shipment_results(shiprocket_result, zakya_shipment_result, zakya_packages_result)
                    elif isinstance(result, dict) and result.get('status') == 1:
                        st.success(f"Booking Confirmed! Shipment ID: {result['payload']['shipment_id']}")
                        with st.expander("View Shiprocket Details", expanded=True):
                            st.json(result)
                            if result.get('payload', {}).get('label_url'):
                                st.markdown(f"[Download Shipping Label]({result['payload']['label_url']})")
                            if result.get('payload', {}).get('manifest_url'):
                                st.markdown(f"[Download Manifest]({result['payload']['manifest_url']})")
                    else:
                        st.error(f"Error creating shipment: {result}")
                except Exception as e:
                    st.error(f"Exception during shipment creation: {str(e)}")

def shipment_form_component():
    st.subheader("2. Package and Contact Details")
    with st.form(key="shipment_details_form"):
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

        st.markdown("**Contact Person Details**")
        col1, col2 = st.columns(2)
        with col1:
            contact_name = st.text_input("Contact Name", value="")
        with col2:
            contact_phone = st.text_input("Contact Phone", value="")
            contact_email = 'noreply@shiprocket.in'

        check_couriers_submitted = st.form_submit_button("Check Available Couriers")
    return length, breadth, height, weight, contact_name, contact_phone, contact_email, check_couriers_submitted

def display_and_select_couriers(courier_df):
    if courier_df is None or len(courier_df) == 0:
        st.warning("No couriers available for this destination and weight")
        return None

    display_df = courier_df[['courier_company_id', 'courier_name', 'freight_charge', 'charge_weight', 'city', 'rate', 'etd']]
    st.dataframe(display_df)

    courier_options = courier_df['courier_company_id'].tolist()
    if not courier_options:
        return None

    def format_courier_option(courier_id):
        row = courier_df[courier_df['courier_company_id'] == courier_id].iloc[0]
        st.session_state['shipping_rate'] = row['rate']
        return f"{row['courier_name']} - ₹{row['rate']} (ETD -> {row['etd']})"

    return st.selectbox("Select Courier Service", options=courier_options, format_func=format_courier_option)
