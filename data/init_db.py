import os
from databricks import sql
from dotenv import load_dotenv

def run_sql_file(path, cursor):
    """
    Read a .sql file containing one or more SQL statements separated by ';'
    and execute them sequentially using the provided Databricks SQL cursor.

    Notes:
      - This uses a very simple splitter on ';'. It will treat any semicolon
        as a statement terminator (so avoid semicolons inside strings).
      - Errors are caught and printed per-statement so one bad statement
        doesn't stop the rest from running.
    """
    with open(path, 'r') as f:
        sql_file = f.read()

    # Split on ';', strip whitespace, and drop empty fragments
    queries = [q.strip() for q in sql_file.split(';') if q.strip()]
    
    # Execute each statement individually
    for query in queries:
        try:
            cursor.execute(query)
        except Exception as e:
            # Log (but don't re-raise) so the rest of the file still runs
            print(f"Error executing query: {e}")

def main():
    """
    Load Databricks SQL connection details from environment variables,
    open a connection, and run all statements in 'schema.sql'.

    Expected environment variables:
      - DB_SERVER_HOSTNAME : your Databricks SQL Warehouse host
      - DB_HTTP_PATH       : the HTTP path for the Warehouse/endpoint
      - DB_TOKEN           : a Personal Access Token or service token
    """
    # Load variables from a local .env file if present
    load_dotenv()

    DB_HOST = os.getenv('DB_SERVER_HOSTNAME')
    DB_HTTP = os.getenv('DB_HTTP_PATH')
    DB_TOKEN = os.getenv('DB_TOKEN')
    
    # Create a Databricks SQL connection and run the schema file
    with sql.connect(server_hostname=DB_HOST, access_token=DB_TOKEN, http_path=DB_HTTP) as conn:
        with conn.cursor() as cursor:
            run_sql_file('data/schema.sql', cursor)
            print("Database initialized successfully")

if __name__ == "__main__":
    main()
