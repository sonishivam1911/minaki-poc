import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import os
import json
import bcrypt
from dotenv import load_dotenv
from pydantic import BaseModel

from config.constants import OPERATORS

# Load environment variables from .env
load_dotenv()

POSTGRES_URI = os.getenv("POSTGRES_SESSION_POOL_URI")

class PostgresCRUD:
    def __init__(self):
        self.engine = create_engine(POSTGRES_URI)
        

    def create_table(self, table_name, dataframe):
        """Create a table in PostgreSQL from a pandas DataFrame."""
        for col in dataframe.columns:
                if dataframe[col].apply(lambda x: isinstance(x, (dict,list))).any():
                    dataframe[col] = dataframe[col].apply(json.dumps) 

        print(f"dataframe is : {dataframe.columns}")        
        try:
            dataframe.to_sql(table_name, con=self.engine, schema="public" ,if_exists='replace', index=False)
        except Exception as e:
            return f"Error creating table '{table_name}': {e}"
        
        return f"Table '{table_name}' created successfully."

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
        
    def execute_query(self,query,return_data=False):
        try:
            with self.engine.connect() as connection:
                if return_data:
                    cursor_result = connection.execute(text(query))
                    rows = cursor_result.fetchall()
                    columns = cursor_result.keys()
                    return pd.DataFrame(rows, columns=columns)
                else:
                    connection.execute(text(query))
        except Exception as e:
            print(f"Error running query : {query} and error : {e}")
        
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
        
    def create_insert_statements(self, df: pd.DataFrame, table_name: str) -> list:
        """
        Generates a list of SQL INSERT statements for each row in a Pandas DataFrame.
        
        Args:
            df (pd.DataFrame): The DataFrame containing the data to be inserted.
            table_name (str): The name of the table to insert into.
            
        Returns:
            list: A list of SQL INSERT statements (strings).
        """
        insert_statements = []
        columns = df.columns.tolist()
        columns_str = ", ".join(columns)

        for _, row in df.iterrows():
            values = []
            for value in row:
                # Handle NULLs
                if not isinstance(value, list):
                    if pd.isna(value):
                        values.append("NULL")
                    # Escape single quotes in strings
                    elif isinstance(value, str):
                        safe_val = value.replace("'", "''")
                        values.append(f"'{safe_val}'")
                    elif isinstance(value, dict) or isinstance(value, list):
                        # Convert dict or list to valid JSON string
                        json_val = json.dumps(value).replace("'", "''")  # escape single quotes
                        values.append(f"'{json_val}'")  # wrap in quotes for SQL
                    else:
                        # Convert other types to string
                        values.append(f"'{value}'") 
                else:
                    continue

            values_str = ", ".join(values)
            statement = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str});"
            insert_statements.append(statement)

        return insert_statements
        
    def python_type_to_postgres(self, field) -> str:

    
        """
        Derive a Postgres column type from:
        1) custom field metadata (field.field_info.extra['pg_type']), or
        2) standard Python type.
        """
        # If 'pg_type' is explicitly specified, use that
        pg_type = field.field_info.extra.get('pg_type')
        if pg_type:
            return pg_type

        # Otherwise, map from Python type
        py_type = field.outer_type_
        if isinstance(py_type,int):
            return "INTEGER"
        elif isinstance(py_type,float):
            # If you want to handle decimal places, read from field info
            decimal_places = field.field_info.extra.get('decimal_places', 2)
            # for example DECIMAL(10, 2). You can make the (precision, scale) configurable
            return f"DECIMAL(10, {decimal_places})"
        elif isinstance(py_type,str):
            max_length = field.field_info.max_length or 255
            return f"VARCHAR({max_length})"
        elif py_type.__name__ in ["datetime", "date", "time"]:
            # You could refine separate date/time/datetime checks
            return "TIMESTAMP"
        elif isinstance(py_type,bool):
            return "BOOLEAN"
        
        # Default fallback (could also use TEXT for unknown strings)
        return "TEXT"


    def create_table_ddl_query(self,model: type[BaseModel], table_name: str) -> str:
        """
        Generate a CREATE TABLE statement from a Pydantic model.
        
        Args:
            model (Type[BaseModel]): The Pydantic model class.
            table_name (str): Name of the table to create.
        
        Returns:
            str: A 'CREATE TABLE' statement for PostgreSQL.
        """
        columns_sql = []
        primary_keys = []

        for field_name, field in model.model_fields.items():
            pg_col_type = self.python_type_to_postgres(field)
            
            # Build the column definition
            col_def = f"{field_name} {pg_col_type}"

            # If the field is marked as the primary key
            if field.field_info.extra.get('primary_key'):
                primary_keys.append(field_name)

            columns_sql.append(col_def)

        # If we have a primary key (single or composite), add it to the SQL
        pk_sql = ""
        if primary_keys:
            pk_cols = ", ".join(primary_keys)
            pk_sql = f",\n  PRIMARY KEY ({pk_cols})"

        # Final CREATE TABLE statement
        create_table_statement = f"CREATE TABLE {table_name} (\n  {',\n  '.join(columns_sql)}{pk_sql}\n);"
        return create_table_statement
    
    def build_where_clause(self, model: BaseModel, filters: dict) -> str:
        valid_columns = model.model_fields.keys()  # Infer table columns from Pydantic model
        clauses = []

        for column, condition in filters.items():
            if column not in valid_columns:
                raise ValueError(f"Invalid column: {column}")

            operator = condition.get("op")
            value = condition.get("value")

            if operator not in OPERATORS:
                raise ValueError(f"Invalid operator '{operator}' for column '{column}'")

            sql_operator = OPERATORS[operator]

            # Handling different data types
            if operator == "in" and isinstance(value, list):
                formatted_values = ", ".join(f"'{v}'" for v in value)
                clause = f"{column} {sql_operator} ({formatted_values})"
            elif operator == "between" and isinstance(value, list) and len(value) == 2:
                clause = f"{column} {sql_operator} '{value[0]}' AND '{value[1]}'"
            else:
                clause = f"{column} {sql_operator} '{value}'"

            clauses.append(clause)


        return "WHERE " + " AND ".join(clauses) if len(clauses) > 0 else clause



crud = PostgresCRUD()        