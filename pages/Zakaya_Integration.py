import streamlit as st
import requests
from utils.zakya_api import get_authorization_url,get_access_token,fetch_organizations,fetch_inventory_items

st.title("Zakya Inventory Management")

    # Check if authorization code is present in the URL
auth_code = st.session_state['code'] if 'code' in st.session_state else None
if auth_code:
    auth_code = auth_code
    print(f'auth data is {auth_code}')
    token_data = get_access_token(auth_code=auth_code)
    print(f'token data is {token_data}')
    access_token = token_data["access_token"]
    refresh_token = token_data.get("refresh_token")

    st.success("Authentication successful!")

    # Fetch and display organizations
    st.header("Organizations")
    try:
        org_data = fetch_organizations(access_token)
        organizations = org_data.get("organizations", [])
        for org in organizations:
            st.write(f"Organization Name: {org['name']}")
            st.write(f"Organization ID: {org['organization_id']}")
            st.write("---")

        # Fetch and display inventory items for the first organization
        if organizations:
            first_org_id = organizations[0]['organization_id']
            st.header("Inventory Items")
            items_data = fetch_inventory_items(access_token, first_org_id)
            items = items_data.get("items", [])
            for item in items:
                st.write(f"Item Name: {item['name']}")
                st.write(f"Item ID: {item['item_id']}")
                st.write(f"SKU: {item['sku']}")
                st.write(f"Available Stock: {item['available_stock']}")
                st.write("---")
    except requests.exceptions.RequestException as e:
        st.error(f"API request failed: {e}")
else:
    # Display login button
    auth_url = get_authorization_url()
    val=st.markdown(f"[Login with Zakya]({auth_url})")
    print(val)
