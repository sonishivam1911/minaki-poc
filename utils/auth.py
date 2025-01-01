import psycopg2
import bcrypt

def get_db_connection():
    """Connect to PostgreSQL database."""
    conn = psycopg2.connect(
        dbname=st.secrets["postgres"]["dbname"],
        user=st.secrets["postgres"]["user"],
        password=st.secrets["postgres"]["password"],
        host=st.secrets["postgres"]["host"],
        port=st.secrets["postgres"]["port"]
    )
    return conn

def validate_login(username, password):
    """Validate username and password against the database."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT hashed_password FROM users WHERE username = %s", (username,))
    result = cur.fetchone()
    conn.close()

    if result:
        stored_hashed_password = result[0]
        return bcrypt.checkpw(password.encode('utf-8'), stored_hashed_password.encode('utf-8'))
    return False
