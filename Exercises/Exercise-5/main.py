from db_setup.db_connect import get_connection
from db_setup.db_exe import execute_sql
from db_setup.csv_loader import insert_csv
from db_setup.logs_config import get_logger
import os

logger = get_logger(__name__)

def main():
    try:
        # Establish connection
        conn = get_connection()
        cur = conn.cursor()

        # --- Fix: Dynamically locate schema.sql file ---
        base_dir = os.path.dirname(os.path.abspath(__file__))
        schema_path = os.path.join(base_dir, "schema.sql")
        execute_sql(cur, schema_path)

        # --- Fix: Use absolute path for data folder too ---
        data_dir = os.path.join(base_dir, "data")
        tables = ["accounts", "products", "transactions"]

        print(base_dir)
        print(data_dir)
        # Load CSV data
        for table in tables:
            csv_path = os.path.join(data_dir, f"{table}.csv")
            insert_csv(cur, table, csv_path)

        # Verify table row counts
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            logger.info(f"Table '{table}' loaded successfully with {count} rows.")

        logger.info("All operations completed successfully.")

    except Exception as e:
        logger.error(f"Main Execution Error: {e}")

if __name__ == "__main__":
    main()


# import psycopg2


# def main():
#     host = "postgres"
#     database = "postgres"
#     user = "postgres"
#     pas = "postgres"
#     conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
#     # your code here
#     cur = conn.cursor()


# if __name__ == "__main__":
#     main()