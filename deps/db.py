import os
import psycopg2

def get_db():
    instance = os.environ["INSTANCE_CONNECTION_NAME"]  # project:region:instance
    dbname = os.environ["DB_NAME"]
    user = os.environ["DB_USER"]
    password = os.environ["DB_PASSWORD"]

    conn = psycopg2.connect(
        host=f"/cloudsql/{instance}",
        dbname=dbname,
        user=user,
        password=password,
    )
    try:
        yield conn
    finally:
        conn.close()
