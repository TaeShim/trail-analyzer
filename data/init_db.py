import os
from databricks import sql
from dotenv import load_dotenv

def run_sql_file(path, cursor):
    with open(path, 'r') as f:
        sql_file = f.read()

    queries = [q.strip() for q in sql_file.split(';') if q.strip()]
    
    for query in queries:
        try:
            cursor.execute(query)
        except Exception as e:
            print(f"Error executing query: {e}")

def main():
    load_dotenv()
    DB_HOST = os.getenv('DB_SERVER_HOSTNAME')
    DB_HTTP = os.getenv('DB_HTTP_PATH')
    DB_TOKEN = os.getenv('DB_TOKEN')
    
    with sql.connect(server_hostname=DB_HOST, access_token=DB_TOKEN, http_path=DB_HTTP) as conn:
        with conn.cursor() as cursor:
            run_sql_file('schema.sql', cursor)
            print("Database initialized successfully")

if __name__ == "__main__":
    main()




