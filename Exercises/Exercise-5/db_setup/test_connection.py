import psycopg2

try:
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="test_db",
        user="root",
        password="root"
    )
    print("Connected successfully!")
    conn.close()
except Exception as e:
    print("Connection failed:", e)