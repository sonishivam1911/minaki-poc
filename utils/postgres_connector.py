import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import os
import bcrypt
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

POSTGRES_URI = os.getenv("POSTGRES_URI")

class PostgresCRUD:
    def __init__(self):
        self.engine = create_engine(POSTGRES_URI)

    def create_table(self, table_name, dataframe):
        """Create a table in PostgreSQL from a pandas DataFrame."""
        try:
            dataframe.to_sql(table_name, self.engine, if_exists='replace', index=False)
            return f"Table '{table_name}' created successfully."
        except Exception as e:
            return f"Error creating table '{table_name}': {e}"

    def read_table(self, table_name):
        """Read a table from PostgreSQL into a pandas DataFrame."""
        try:
            query = f"SELECT * FROM {table_name}"
            return pd.read_sql(query, self.engine)
        except Exception as e:
            return f"Error reading table '{table_name}': {e}"

    def update_table(self, table_name, set_clause, condition):
        """Update rows in a PostgreSQL table."""
        try:
            with self.engine.connect() as connection:
                query = f"UPDATE {table_name} SET {set_clause} WHERE {condition}"
                connection.execute(query)
            return f"Table '{table_name}' updated successfully."
        except Exception as e:
            return f"Error updating table '{table_name}': {e}"

    def delete_rows(self, table_name, condition):
        """Delete rows from a PostgreSQL table."""
        try:
            with self.engine.connect() as connection:
                query = f"DELETE FROM {table_name} WHERE {condition}"
                connection.execute(query)
            return f"Rows deleted from table '{table_name}' where {condition}."
        except Exception as e:
            return f"Error deleting rows from table '{table_name}': {e}"

    def delete_table(self, table_name):
        """Delete a table from PostgreSQL."""
        try:
            with self.engine.connect() as connection:
                query = f"DROP TABLE IF EXISTS {table_name}"
                connection.execute(query)
            return f"Table '{table_name}' deleted successfully."
        except Exception as e:
            return f"Error deleting table '{table_name}': {e}"
        
    def authenticate_user(self, username, password):
        """
        Authenticate a user by validating their username and password.
        
        Args:
            username (str): The username of the user.
            password (str): The plain-text password entered by the user.
        
        Returns:
            bool: True if authentication is successful, False otherwise.
        """
        try:
            # Query to fetch the hashed password for the given username
            # Query to fetch the hashed password for the given username
            query = text("SELECT hashed_password FROM users WHERE username = :username")

            # Execute query with parameterized input
            with self.engine.connect() as connection:
                user_data = connection.execute(query, {"username": username}).fetchone()
            
        

            print(f"User data is {user_data}")

            if user_data:  # If user exists
                stored_hashed_password = user_data[0]
                
                # Compare hashed passwords using bcrypt
                if bcrypt.checkpw(password.encode('utf-8'), stored_hashed_password.encode('utf-8')):
                    return True  # Authentication successful

            return False  # Authentication failed
        except Exception as e:
            print(f"Error during authentication: {e}")
            return False
        

crud = PostgresCRUD()        