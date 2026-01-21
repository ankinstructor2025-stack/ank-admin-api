from fastapi import FastAPI, Depends, Query
import os
import psycopg2

app = FastAPI()

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/v1/session")
def get_session():
    return {"state": "OK"}

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

@app.get("/v1/debug/users-select")
def users_select(conn=Depends(get_db)):
    with conn.cursor() as cur:
        cur.execute("SELECT user_id FROM users;")
        rows = cur.fetchall()

    return {
        "rows": rows,
        "row_count": len(rows),
    }

@app.get("/v1/user-check")
def user_check(
    email: str = Query(...),
    conn=Depends(get_db)
):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT user_id FROM users WHERE email = %s;",
            (email,)
        )
        row = cur.fetchone()

    return {
        "exists": row is not None,
        "user_id": row[0] if row else None,
    }
