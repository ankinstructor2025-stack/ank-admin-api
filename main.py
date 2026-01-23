from fastapi import FastAPI, APIRouter, Depends, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone
import os
import psycopg2
import uuid
from pydantic import BaseModel, EmailStr
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

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
    user_id: str = Query(...),   # ← Firebase UID（users.user_id と同じ想定）
    conn=Depends(get_db)
):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
              c.contract_id,
              c.status,
              c.seat_limit,
              c.knowledge_count,
              c.payment_method_configured,
              c.current_period_end,
              uc.role,
              uc.status
            FROM contracts c
            JOIN user_contracts uc
              ON uc.contract_id = c.contract_id
            WHERE uc.user_id = %s
              AND uc.status = 'active'
            ORDER BY c.created_at DESC NULLS LAST, c.start_at DESC NULLS LAST
            LIMIT 1
        """, (user_id,))
        row = cur.fetchone()

    if not row:
        return {"contract": None}

    return {
        "contract": {
            "contract_id": row[0],
            "status": row[1],
            "seat_limit": row[2],
            "knowledge_count": row[3],
            "payment_method_configured": bool(row[4]),
            "paid_until": row[5].date().isoformat() if row[5] else None,
            # ついでに返す（UIで使いたくなる）
            "my_role": row[6],         # 'admin' / 'member'
            "my_status": row[7],       # 'active' / 'disabled'
        }
    }

router = APIRouter()

def require_admin():
    return {"role": "admin"}

class ContractCreate(BaseModel):
    user_id: str
    email: str
    display_name: str | None = None
    seat_limit: int
    knowledge_count: int

@router.post("/v1/contract")
def create_contract(payload: ContractCreate, conn=Depends(get_db)):
    with conn.cursor() as cur:
        # 0) email が別 user に使われていたら止める（unique対策）
        cur.execute("""
            SELECT user_id
            FROM users
            WHERE email = %s AND user_id <> %s
            LIMIT 1;
        """, (payload.email, payload.user_id))
        row = cur.fetchone()
        if row:
            raise HTTPException(status_code=409, detail="email already used by another user")

        # 1) users を作る/更新（FKを通すため先に）
        cur.execute("""
            INSERT INTO users (user_id, email, display_name, created_at, last_login_at)
            VALUES (%s, %s, %s, NOW(), NOW())
            ON CONFLICT (user_id) DO UPDATE
              SET email = EXCLUDED.email,
                  display_name = EXCLUDED.display_name,
                  last_login_at = NOW();
        """, (payload.user_id, payload.email, payload.display_name))

        # 2) contracts 作成
        cur.execute("""
            INSERT INTO contracts (status, seat_limit, knowledge_count)
            VALUES ('active', %s, %s)
            RETURNING contract_id;
        """, (payload.seat_limit, payload.knowledge_count))
        contract_id = cur.fetchone()[0]

        # 3) user_contracts 作成
        cur.execute("""
            INSERT INTO user_contracts (user_id, contract_id, role)
            VALUES (%s, %s, 'admin');
        """, (payload.user_id, contract_id))

    conn.commit()
    return {"contract_id": str(contract_id), "status": "active"}

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

APP_BASE_URL = os.environ.get("APP_BASE_URL", "https://ankinstructor2025-stack.github.io/ank-knowledge")  # 招待リンクのベース
FROM_EMAIL = os.environ.get("INVITE_FROM_EMAIL", "ank.instructor2025@gmail.com")

class InviteCreateIn(BaseModel):
    contract_id: str
    email: str

@router.post("/v1/invites")
def create_invite(payload: InviteCreateIn, conn=Depends(get_db), current_user=Depends(require_admin)):
    # 1) token発行
    token = uuid.uuid4().hex  # まずはこれで十分（細かい強度は後で）
    invite_url = f"{APP_BASE_URL}/invite.html?token={token}"

    # 2) DB保存
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO invites (contract_id, email, token)
            VALUES (%s, %s, %s)
            RETURNING invite_id, created_at
            """,
            (payload.contract_id, payload.email, token),
        )
        row = cur.fetchone()
    conn.commit()

    invite_id, created_at = row

    # 3) SendGridでメール送信
    sg_key = os.environ.get("SENDGRID_API_KEY")
    if not sg_key:
        raise HTTPException(status_code=500, detail="SENDGRID_API_KEY not set")

    subject = "招待メール：アカウント登録"
    body = (
        "次のリンクから登録してください。\n\n"
        f"{invite_url}\n\n"
        "このメールに心当たりがない場合は破棄してください。\n"
    )

    message = Mail(
        from_email=FROM_EMAIL,
        to_emails=payload.email,
        subject=subject,
        plain_text_content=body,
    )

    try:
        sg = SendGridAPIClient(sg_key)
        res = sg.send(message)
        # SendGridは成功で 202 が返ることが多い
        if res.status_code not in (200, 201, 202):
            raise HTTPException(status_code=502, detail=f"SendGrid error: {res.status_code}")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"SendGrid send failed: {str(e)}")

    return {
        "ok": True,
        "invite_id": str(invite_id),
        "created_at": created_at.isoformat(),
        "email": payload.email,
        "contract_id": payload.contract_id,
    }

app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
