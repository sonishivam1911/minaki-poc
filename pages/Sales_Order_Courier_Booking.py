import streamlit as st
from config.logger import logger
from server.create_shiprocket_for_sales_orders import sales_order_id_number_mapping_dict, create_shiprocket_for_sales_orders, create_shiprocket_sr_forward


def shiprocket_streamlit_interface():
    st.title("Create Shiprocket Shipment")
    
    sales_order_mapping = sales_order_id_number_mapping_dict()
    selected_so_number = st.selectbox(
        "Select Sales Order",
        options=list(sales_order_mapping.values()),
        format_func=lambda x: f"SO-{x}"
    )
    
    if selected_so_number:
        selected_so_id = [k for k, v in sales_order_mapping.items() if v == selected_so_number][0]
        
        # Package details
        st.subheader("Package Details")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            length = st.number_input("Length (cm)", min_value=1)
        with col2:
            breadth = st.number_input("Breadth (cm)", min_value=1)
        with col3:
            height = st.number_input("Height (cm)", min_value=1)
        with col4:
            weight = st.number_input("Weight (kg)", min_value=0.1)
        
        st.button("Check Available Couriers",on_click=fetch_selected_courier,args=(selected_so_id, weight))
        config = {
                'length' : length,
                'breadth' : breadth,
                'height' : height,
                'weight' : weight
            }
        if 'selected_courier' in st.session_state:
            
            st.button('Confirm Courier Booking',on_click=create_shiprocket_sr_forward,args=(config))
            st.success('Booking Confirmed')



def fetch_selected_courier(selected_so_id, weight):
    courier_df = create_shiprocket_for_sales_orders(selected_so_id, weight)            
    # Display courier options in a more user-friendly format
    st.subheader("Available Couriers")
    # Create a selection dataframe with relevant columns
    display_df = courier_df[['courier_name','courier_type' , 'rate', 'etd', 'rating', 'is_hyperlocal', 'id']]
    # Show the dataframe
    st.dataframe(display_df)
    # Create courier selection dropdown
    selected_courier = st.selectbox(
                    "Select Courier Service",
                    options=courier_df['id'].tolist(),
                    format_func=lambda x: f"{courier_df[courier_df['id']==x]['courier_name'].iloc[0]} - ₹{courier_df[courier_df['id']==x]['rate'].iloc[0]} ({courier_df[courier_df['id']==x]['etd'].iloc[0]} days)"
                )
    
    st.session_state['selected_courier'] = selected_courier
    return selected_courier

if __name__ == "__main__":
    shiprocket_streamlit_interface()