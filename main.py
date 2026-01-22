from fastapi import FastAPI, Depends, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone
import os
import psycopg2
import uuid
from pydantic import BaseModel

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://ankinstructor2025-stack.github.io",  # GitHub Pages
        "http://localhost:5500",
        "http://127.0.0.1:5500",
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],  # Authorization を許可
)

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

@app.get("/v1/pricing")
def pricing(conn=Depends(get_db)):
    """
    pricing_items から pricing を組み立てて返す
    - item_type='seat'                : value_int=seat_limit, monthly_price=monthly_fee
    - item_type='knowledge_count'     : value_int=value,     monthly_price=monthly_price
    - item_type='search_limit_per_user_per_day' : value_int=per_user_per_day
    - item_type='search_limit_note'   : label=note
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT item_type, value_int, monthly_price, label
            FROM pricing_items
            WHERE is_active = TRUE
            ORDER BY item_type, sort_order, value_int
            """
        )
        rows = cur.fetchall()

    seats = []
    knowledge_count = []
    search_limit = {"per_user_per_day": 0, "note": ""}

    for item_type, value_int, monthly_price, label in rows:
        if item_type == "seat":
            # monthly_price を seats の monthly_fee として返す
            seats.append({
                "seat_limit": int(value_int),
                "monthly_fee": monthly_price,  # NULLなら「要相談」扱い（admin.js側が対応済み）
                "label": label or ""
            })

        elif item_type == "knowledge_count":
            knowledge_count.append({
                "value": int(value_int),
                "monthly_price": int(monthly_price or 0),
                "label": label or str(value_int)
            })

        elif item_type == "search_limit_per_user_per_day":
            # 1ユーザーあたり/日の上限
            search_limit["per_user_per_day"] = int(value_int or 0)

        elif item_type == "search_limit_note":
            search_limit["note"] = label or ""

        # それ以外は無視（将来拡張用）
        else:
            pass

    return {
        "seats": seats,
        "knowledge_count": knowledge_count,
        "search_limit": search_limit,
        "poc": None,
    }

@app.get("/v1/contract")
def get_contract(
    tenant_id: str = Query(...),
    conn=Depends(get_db)
):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
              contract_id,
              tenant_id,
              status,
              seat_limit,
              knowledge_count,
              payment_method_configured,
              current_period_end
            FROM contracts
            WHERE tenant_id = %s
            ORDER BY created_at DESC NULLS LAST, start_at DESC NULLS LAST
            LIMIT 1
        """, (tenant_id,))
        row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="contract not found")

    return {
        "contract_id": row[0],
        "tenant_id": row[1],
        "status": row[2],
        "seat_limit": row[3],
        "knowledge_count": row[4],
        "payment_method_configured": bool(row[5]),
        # admin.js は paid_until を見ているので合わせる
        "paid_until": row[6].date().isoformat() if row[6] else None,
    }

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
