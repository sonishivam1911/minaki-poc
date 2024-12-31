import pandas as pd
from sqlalchemy import create_engine
import os
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
        

crud = PostgresCRUD()        