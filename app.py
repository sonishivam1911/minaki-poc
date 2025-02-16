import streamlit as st
import pandas as pd
from utils.zakya_api import get_access_token,get_authorization_url,fetch_organizations, fetch_contacts, fetch_records_from_zakya
from utils.postgres_connector import crud

# Initialize session state variables
if "is_authenticated" not in st.session_state:
    st.session_state["is_authenticated"] = False
    st.session_state["username"] = None

def login_page():
    """Login page for user authentication."""
    st.title("Login")
    
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Login"):
        # Use PostgresCRUD's authenticate_user method for validation
        if crud.authenticate_user(username, password):
            st.session_state["is_authenticated"] = True
            st.session_state["username"] = username
            st.success("Login successful!")
            st.rerun()  # Reload the app after login success
        else:
            st.error("Invalid username or password.")

def logout():
    """Logout functionality."""
    if st.button("Logout"):
        st.session_state["is_authenticated"] = False
        st.session_state["username"] = None
        st.rerun()

def main():
    """Main application logic."""
    code=st.query_params.get("code")
    if code:
        st.session_state['code'] = code
        print(f"code is : {st.session_state['code']}")

        if 'access_token' not in st.session_state:
            try: 
                token_authentication()
            except KeyError as e:
                st.error(f"Error getting access token: Missing key {e} in token response. Check your Zakya API credentials/setup.")
                del st.session_state['code']  
                fetch_zakya_code()
                
            except Exception as e: 
                st.error(f"An unexpected error occurred during authentication: {e}")
                del st.session_state['code']  
                fetch_zakya_code()

        # Fetch and display organizations
        
        if 'organization_id' not in st.session_state:
            st.header("Organizations")
            org_data = fetch_organizations(st.session_state['api_domain'],st.session_state['access_token'])            
            st.session_state['organization_id'] = org_data['organizations'][0]['organization_id']
        
        if st.button("Show Contacts"):
            with st.container():
                st.header("Contacts")
                contact_data = fetch_contacts(
                        st.session_state['api_domain'],
                        st.session_state['access_token'],
                        st.session_state['organization_id']
                    )
                contacts_df = pd.DataFrame.from_records(contact_data['contacts'])
                st.dataframe(contacts_df)
                if st.button("Save to Database"):
                    crud.create_table('zakya_contacts',contacts_df)

        if st.button("Show Item Groups"):
            with st.container():
                st.header("Item Groups")
                item_groups_data = fetch_records_from_zakya(
                        st.session_state['api_domain'],
                        st.session_state['access_token'],
                        st.session_state['organization_id'],
                        '/itemgroups'
                )
                itemgroups_df = pd.DataFrame.from_records(item_groups_data['itemgroups'])
                st.dataframe(itemgroups_df)
                if st.button("Save to Database"):
                    crud.create_table('zakya_item_groups',itemgroups_df)

        if st.button("Show Items"):
            with st.container():
                st.header("Items")
                items_data = fetch_records_from_zakya(
                        st.session_state['api_domain'],
                        st.session_state['access_token'],
                        st.session_state['organization_id'],
                        '/items'                
                )
                product_df = pd.DataFrame.from_records(items_data['items'])
                st.dataframe(product_df)
                if st.button("Save to Database"):
                    crud.create_table('zakya_products',product_df)

        if st.button("Show Sales Order"):
            with st.container():
                st.header("Sales Order")
                sales_order_data = fetch_records_from_zakya(
                        st.session_state['api_domain'],
                        st.session_state['access_token'],
                        st.session_state['organization_id'],
                        '/salesorders'                  
                )
                sales_order_df = pd.DataFrame.from_records(sales_order_data['salesorders'])
                st.dataframe(sales_order_df)
                if st.button("Save to Database"):
                    crud.create_table('zakya_sales_order',sales_order_df)            

    else:
        fetch_zakya_code()

def fetch_zakya_code():
    print(f'state variables are : {st.session_state}')
    auth_url = get_authorization_url()
    st.markdown(f"[Login with Zakya]({auth_url})")

def token_authentication():
    token_data = get_access_token(auth_code=st.session_state['code'])
    print(f'token url is {token_data}')
    st.session_state['access_token'] = token_data["access_token"]
    st.session_state['api_domain'] = token_data["api_domain"]
    st.success("Authentication successful!")



main()
