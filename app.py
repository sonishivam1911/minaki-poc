import streamlit as st
import pandas as pd
from dotenv import load_dotenv

from utils.zakya_api import get_access_token, get_authorization_url, fetch_organizations
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

    st.button("Login", on_click=authenticate_user, args=(username, password))
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
    
    if "refresh_token" in st.session_state:
        try:
            set_access_token_via_refresh_token()
        except Exception as e:
            logger.error(f"Error refreshing token: {e}")
            st.error("Could not refresh authentication token. Please try logging in again.")
            fetch_zakya_code()
    else:   
        set_refresh_token()

    try:
        if 'access_token' in st.session_state:
            if 'organization_id' not in st.session_state:
                # st.header("Organizations")
                org_data = fetch_organizations(st.session_state['access_token'])
                if org_data and 'organizations' in org_data and len(org_data['organizations']) > 0:            
                    st.session_state['organization_id'] = org_data['organizations'][0]['organization_id']
                else:
                    logger.error("No organizations found in response")
                    st.error("No organizations found. Please check your Zakya account.")
    except Exception as e:
        logger.error(f"Error fetching organizations: {e}")
        st.error("Error connecting to Zakya. Please check your connection and try again.")

def set_access_token_via_refresh_token():
    # Use consistent table name - zakya_auth
    zakya_auth_df = crud.read_table("zakya_auth")
    
    if zakya_auth_df is None or zakya_auth_df.empty:
        logger.error("No authentication data found in database")
        raise Exception("No authentication data found")
    
    # Correctly access the refresh token value
    refresh_token = zakya_auth_df["refresh_token"].iloc[0]
    logger.debug(f"Refresh token is {refresh_token}")
    st.session_state["refresh_token"] = refresh_token
    
    refresh_token_data = get_access_token(refresh_token=refresh_token)
    
    if 'access_token' not in refresh_token_data:
        logger.error("Failed to get access token from refresh token")
        raise Exception("Failed to refresh token")
    
    st.session_state['access_token'] = refresh_token_data['access_token']
    
    # Set API domain consistently
    st.session_state['api_domain'] = 'https://api.zakya.in/'

    logger.debug(f"State variables are as follows : {st.session_state}")


def set_refresh_token():
    logger.debug(f"Session state variables are : {st.session_state}")
    code = st.query_params.get("code")
    logger.debug(f"code is {code}")
    
    if "code" not in st.session_state and code and len(code) > 0:
        st.session_state['code'] = code

        if 'access_token' not in st.session_state:
            try: 
                token_authentication()
            except KeyError as e:
                logger.error(f"Error getting access token: Missing key {e} in token response")
                st.error(f"Error getting access token: Missing key {e} in token response. Check your Zakya API credentials/setup.")
                if 'code' in st.session_state:
                    del st.session_state['code']  
                fetch_zakya_code()
                
            except Exception as e:
                logger.error(f"An unexpected error occurred during authentication: {e}")
                st.error(f"An unexpected error occurred during authentication: {e}")
                if 'code' in st.session_state:
                    del st.session_state['code']  
                fetch_zakya_code()
        
    elif "code" not in st.session_state:
        fetch_zakya_code()


def fetch_zakya_code():
    auth_url = get_authorization_url()
    st.markdown(f"[Login with Zakya]({auth_url})")

def token_authentication():
    token_data = get_access_token(auth_code=st.session_state['code'])
    logger.debug(f"Token data is {token_data}")
    
    if 'access_token' not in token_data:
        raise KeyError('access_token')
    
    st.session_state['access_token'] = token_data["access_token"]
    st.session_state['refresh_token'] = token_data["refresh_token"]
    
    # Remove access_token for security before storing
    token_data_for_storage = token_data.copy()
    token_data_for_storage.pop('access_token', None)
    
    # Convert to DataFrame and store in database
    # Use consistent table name - zakya_auth
    zakya_auth_df = pd.DataFrame([token_data_for_storage])
    crud.create_table("zakya_auth", zakya_auth_df)
    
    # Set API domain
    st.session_state['api_domain'] = 'https://api.zakya.in/'
    
    st.success("Authentication successful!")


# Run the main function
main()