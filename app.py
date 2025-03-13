import streamlit as st
from dotenv import load_dotenv

from utils.zakya_api import get_access_token,get_authorization_url,fetch_organizations
from utils.postgres_connector import crud
from config.logger import logger

load_dotenv()

# Initialize session state variables
if "is_authenticated" not in st.session_state:
    st.session_state["is_authenticated"] = False
    st.session_state["username"] = None

def login_page():
    """Login page for user authentication."""
    st.title("Login")
    
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    st.button("Login",on_click=authenticate_user,args=(username, password))
        # Use PostgresCRUD's authenticate_user method for validation


def authenticate_user(username, password):
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
    
    
    logger.debug(f"Session state variables are : {st.session_state}")
    code=st.query_params.get("code")
    logger.debug(f"code is {code}")
    if "code" not in st.session_state and code and len(code)>0:
        code=st.query_params.get("code")
        st.session_state['code'] = code

        if 'access_token' not in st.session_state:
            try: 
                token_authentication()
            except KeyError as e:
                print(f"Error getting access token: Missing key {e} in token response. Check your Zakya API credentials/setup.")
                # st.error(f"Error getting access token: Missing key {e} in token response. Check your Zakya API credentials/setup.")
                del st.session_state['code']  
                fetch_zakya_code()
                
            except Exception as e:
                print(f"An unexpected error occurred during authentication: {e}") 
                # st.error(f"An unexpected error occurred during authentication: {e}")
                del st.session_state['code']  
                fetch_zakya_code()

        # Fetch and display organizations
        
        if 'organization_id' not in st.session_state:
            # st.header("Organizations")
            org_data = fetch_organizations(st.session_state['api_domain'],st.session_state['access_token'])            
            st.session_state['organization_id'] = org_data['organizations'][0]['organization_id']
            # st.switch_page('Zakaya_Integration.py')
        
    elif "code" not in st.session_state:
        fetch_zakya_code()


def fetch_zakya_code():
    auth_url = get_authorization_url()
    st.markdown(f"[Login with Zakya]({auth_url})")

def token_authentication():
    token_data = get_access_token(auth_code=st.session_state['code'])
    print(f'token url is {token_data}')
    st.session_state['access_token'] = token_data["access_token"]
    st.session_state['api_domain'] = token_data["api_domain"]
    st.success("Authentication successful!")
    



main()
