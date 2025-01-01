import streamlit as st
from functools import wraps

def require_auth(func):
    """Decorator to enforce authentication on a page."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not st.session_state.get("is_authenticated", False):
            st.warning("Please log in to access this page.")
            st.stop()  # Stop further execution of the page
        return func(*args, **kwargs)
    return wrapper