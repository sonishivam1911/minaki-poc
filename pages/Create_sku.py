import streamlit as st
from server.create_sku import fetch_category_mapping, create_xuping_sku
from config.settings import VENDOR_MAPPING


def generate_sku_on_click(selected_category,selected_vendor):
        new_sku = None
        if selected_vendor == "vendor 1":
            new_sku = create_xuping_sku(selected_category)
        st.success(f"Generated SKU: {new_sku}")
        st.code(new_sku)

def main():
    st.title("SKU Generator")
    
    # Fetch category mapping
    category_dict, _ = fetch_category_mapping()
    
    # Create dropdown for category selection
    selected_category = st.selectbox(
        "Select Product Category",
        options=list(category_dict.keys())
    )

    selected_vendor = st.selectbox(
        "Select Vendor",
        options=list(VENDOR_MAPPING.keys())
    )
    
    # Generate SKU button
    st.button("Generate SKU",on_click=generate_sku_on_click,args=(selected_category,selected_vendor))


if __name__ == "__main__":
    main()