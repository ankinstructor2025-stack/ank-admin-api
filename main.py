from fastapi import FastAPI, APIRouter, Depends, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import os
import psycopg2
import uuid
from pydantic import BaseModel
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import firebase_admin
from firebase_admin import auth as firebase_auth, credentials as firebase_credentials


# =========================================================
# App / CORS
# =========================================================

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


# =========================================================
# Firebase Auth
# =========================================================

auth_scheme = HTTPBearer(auto_error=False)

def _init_firebase():
    if not firebase_admin._apps:
        firebase_admin.initialize_app(
            firebase_credentials.ApplicationDefault(),
            {"projectId": os.environ.get("FIREBASE_PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT")},
        )

def require_user(
    cred: HTTPAuthorizationCredentials = Depends(auth_scheme),
):
    if not cred or cred.scheme.lower() != "bearer":
        raise HTTPException(status_code=401, detail="missing Authorization: Bearer <idToken>")

    _init_firebase()

    token = cred.credentials
    try:
        decoded = firebase_auth.verify_id_token(token)
        return decoded  # decoded["uid"] が入る
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"invalid token: {str(e)}")


# =========================================================
# DB
# =========================================================

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


# =========================================================
# Public APIs
# =========================================================

@app.get("/v1/pricing")
def pricing(conn=Depends(get_db)):
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
            seats.append({
                "seat_limit": int(value_int),
                "monthly_fee": monthly_price,  # NULLなら「要相談」
                "label": label or "",
            })

        elif item_type == "knowledge_count":
            knowledge_count.append({
                "value": int(value_int),
                "monthly_price": int(monthly_price or 0),
                "label": label or str(value_int),
            })

        elif item_type == "search_limit_per_user_per_day":
            search_limit["per_user_per_day"] = int(value_int or 0)

        elif item_type == "search_limit_note":
            search_limit["note"] = label or ""

    return {
        "seats": seats,
        "knowledge_count": knowledge_count,
        "search_limit": search_limit,
        "poc": None,
    }


@app.get("/v1/contract")
def get_contract(user=Depends(require_user), conn=Depends(get_db)):
    user_id = user["uid"]

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
              c.contract_id,
              c.status,
              c.start_at,
              c.seat_limit,
              c.knowledge_count,
              c.payment_method_configured,
              c.monthly_amount_yen,
              c.note,
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
            "start_at": row[2].isoformat() if row[2] else None,
            "seat_limit": row[3],
            "knowledge_count": row[4],
            "payment_method_configured": bool(row[5]),
            "monthly_amount_yen": row[6],
            "note": row[7],
            # 互換用：UIが paid_until を参照しても落ちないように残す（期限管理しないので None）
            "paid_until": None,
            "my_role": row[8],
            "my_status": row[9],
        }
    }

@app.get("/v1/contracts")
def list_my_contracts(user=Depends(require_user), conn=Depends(get_db)):
    user_id = user["uid"]  # Firebase UID

    with conn.cursor() as cur:
        cur.execute("""
            SELECT
              uc.contract_id,
              uc.role,
              uc.status AS user_contract_status,
              c.status  AS contract_status,
              c.start_at,
              c.seat_limit,
              c.knowledge_count,
              c.monthly_amount_yen,
              c.note,
              c.payment_method_configured,
              c.created_at
            FROM user_contracts uc
            JOIN contracts c ON c.contract_id = uc.contract_id
            WHERE uc.user_id = %s
            ORDER BY c.created_at DESC
        """, (user_id,))
        rows = cur.fetchall()

    return [
        {
            "contract_id": r[0],
            "role": r[1],
            "user_contract_status": r[2],
            "contract_status": r[3],
            "start_at": r[4].isoformat() if r[4] else None,
            "seat_limit": r[5],
            "knowledge_count": r[6],
            "monthly_amount_yen": r[7],
            "note": r[8],
            "payment_method_configured": r[9],
            "created_at": r[10].isoformat() if r[10] else None,
            # 互換用：フロントが current_period_end を参照しても落ちないように残す（期限管理しないので None）
            "current_period_end": None,
        }
        for r in rows
    ]

@app.get("/v1/user-check")
def user_check(
    email: str = Query(...),
    conn=Depends(get_db),
):
    """
    admin 側が role を欲しがるので、可能なら role も返す。
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT user_id FROM users WHERE email = %s;",
            (email,),
        )
        row = cur.fetchone()

        if not row:
            return {"exists": False, "user_id": None, "role": None}

        user_id = row[0]

        cur.execute(
            """
            SELECT role
            FROM user_contracts
            WHERE user_id = %s AND status = 'active'
            LIMIT 1
            """,
            (user_id,),
        )
        r2 = cur.fetchone()

    return {
        "exists": True,
        "user_id": user_id,
        "role": r2[0] if r2 else None,
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


# =========================================================
# Admin APIs (router)
# =========================================================

router = APIRouter()

class ContractUpdateIn(BaseModel):
    contract_id: str
    seat_limit: int
    knowledge_count: int
    monthly_amount_yen: int
    note: str | None = None

@router.post("/v1/contracts/update")
def update_contract(
    payload: ContractUpdateIn,
    user=Depends(require_user),
    conn=Depends(get_db),
):
    user_id = user["uid"]

    with conn.cursor() as cur:
        # 1) このユーザーが、その契約の admin か確認
        cur.execute("""
            SELECT 1
            FROM user_contracts
            WHERE user_id = %s
              AND contract_id = %s
              AND role = 'admin'
              AND status = 'active'
            LIMIT 1
        """, (user_id, payload.contract_id))
        if not cur.fetchone():
            raise HTTPException(status_code=403, detail="admin only for this contract")

        # 2) contracts 更新
        cur.execute("""
            UPDATE contracts
            SET
              seat_limit = %s,
              knowledge_count = %s,
              monthly_amount_yen = %s,
              note = %s,
              updated_at = NOW()
            WHERE contract_id = %s
        """, (
            payload.seat_limit,
            payload.knowledge_count,
            payload.monthly_amount_yen,
            (payload.note or None),
            payload.contract_id,
        ))

    conn.commit()
    return {"ok": True}

from fastapi import Query, HTTPException

@router.get("/v1/contracts/members")
def list_members(
    contract_id: str = Query(...),
    user=Depends(require_user),
    conn=Depends(get_db),
):
    uid = user["uid"]

    with conn.cursor() as cur:
        # 1) admin か確認
        cur.execute("""
            SELECT 1
            FROM user_contracts
            WHERE user_id=%s AND contract_id=%s AND role='admin' AND status='active'
            LIMIT 1
        """, (uid, contract_id))
        if not cur.fetchone():
            raise HTTPException(status_code=403, detail="admin only for this contract")

        # 2) 支払い設定チェック（未設定なら弾く）
        cur.execute("""
            SELECT payment_method_configured
            FROM contracts
            WHERE contract_id=%s
            LIMIT 1
        """, (contract_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="contract not found")
        if not row[0]:
            raise HTTPException(status_code=409, detail="payment is not configured")

        # 3) メンバー一覧
        cur.execute("""
            SELECT u.email, uc.role, uc.status, u.last_login_at
            FROM user_contracts uc
            LEFT JOIN users u ON u.user_id = uc.user_id
            WHERE uc.contract_id = %s
            ORDER BY uc.role DESC, u.email NULLS LAST
        """, (contract_id,))
        rows = cur.fetchall()

    return {
        "contract_id": contract_id,
        "members": [
            {
                "email": r[0],
                "role": r[1],
                "status": r[2],
                "last_login_at": r[3].isoformat() if r[3] else None,
            }
            for r in rows
        ]
    }

class ContractMarkPaidIn(BaseModel):
    contract_id: str

@router.post("/v1/contracts/mark-paid")
def mark_paid(
    payload: ContractMarkPaidIn,
    user=Depends(require_user),
    conn=Depends(get_db),
):
    """
    「支払い設定へ」を押したら完了扱いにする（仮）
    - payment_method_configured = TRUE
    - start_at が未設定なら NOW() で埋める（利用開始日）
    """
    user_id = user["uid"]

    with conn.cursor() as cur:
        # このユーザーが、その契約の admin か確認（updateと同じ条件）:contentReference[oaicite:2]{index=2}
        cur.execute("""
            SELECT 1
            FROM user_contracts
            WHERE user_id = %s
              AND contract_id = %s
              AND role = 'admin'
              AND status = 'active'
            LIMIT 1
        """, (user_id, payload.contract_id))
        if not cur.fetchone():
            raise HTTPException(status_code=403, detail="admin only for this contract")

        # 支払い設定「完了」扱い
        cur.execute("""
            UPDATE contracts
            SET
              payment_method_configured = TRUE,
              start_at = COALESCE(start_at, NOW()),
              updated_at = NOW()
            WHERE contract_id = %s
        """, (payload.contract_id,))

    conn.commit()
    return {"ok": True}

def require_admin(user=Depends(require_user), conn=Depends(get_db)):
    uid = user["uid"]
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM user_contracts
            WHERE user_id = %s AND role = 'admin' AND status = 'active'
            LIMIT 1
            """,
            (uid,),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=403, detail="admin only")
    return user


class ContractCreate(BaseModel):
    user_id: str
    email: str
    display_name: str | None = None
    seat_limit: int
    knowledge_count: int
    monthly_amount_yen: int
    note: str | None = None

@router.post("/v1/contract")
def create_contract(
    payload: ContractCreate,
    user=Depends(require_user),
    conn=Depends(get_db),
):
    uid = user["uid"]

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT user_id
            FROM users
            WHERE email = %s AND user_id <> %s
            LIMIT 1;
            """,
            (payload.email, uid),
        )
        row = cur.fetchone()
        if row:
            raise HTTPException(status_code=409, detail="email already used by another user")

        cur.execute(
            """
            INSERT INTO users (user_id, email, display_name, created_at, last_login_at)
            VALUES (%s, %s, %s, NOW(), NOW())
            ON CONFLICT (user_id) DO UPDATE
              SET email = EXCLUDED.email,
                  display_name = EXCLUDED.display_name,
                  last_login_at = NOW();
            """,
            (uid, payload.email, payload.display_name),
        )

        cur.execute("""
            INSERT INTO contracts (
              status,
              start_at,
              seat_limit,
              knowledge_count,
              payment_method_configured,
              monthly_amount_yen,
              note,
              created_at,
              updated_at
            )
            VALUES (
              'active',
              NOW(),
              %s,
              %s,
              FALSE,
              %s,
              %s,
              NOW(),
              NOW()
            )
            RETURNING contract_id;
        """, (
            payload.seat_limit,
            payload.knowledge_count,
            payload.monthly_amount_yen,
            (payload.note or None),
        ))
        contract_id = cur.fetchone()[0]

        cur.execute(
            """
            INSERT INTO user_contracts (user_id, contract_id, role, status)
            VALUES (%s, %s, 'admin', 'active');
            """,
            (uid, contract_id),
        )

    conn.commit()
    return {"contract_id": str(contract_id), "status": "active"}


APP_BASE_URL = os.environ.get(
    "APP_BASE_URL",
    "https://ankinstructor2025-stack.github.io/ank-knowledge"
)
FROM_EMAIL = os.environ.get("INVITE_FROM_EMAIL", "ank.instructor2025@gmail.com")


class InviteCreateIn(BaseModel):
    contract_id: str
    email: str


@router.post("/v1/invites")
def create_invite(
    payload: InviteCreateIn,
    conn=Depends(get_db),
    current_user=Depends(require_admin),
):
    token = uuid.uuid4().hex
    invite_url = f"{APP_BASE_URL}/invite.html?token={token}"

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


class InviteConsumeIn(BaseModel):
    token: str


@router.post("/v1/invites/consume")
def consume_invite(
    payload: InviteConsumeIn,
    user=Depends(require_user),
    conn=Depends(get_db),
):
    """
    招待トークンを消費して、ログイン中ユーザーを contract に紐づける
    """
    uid = user["uid"]
    token_email = user.get("email")

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT invite_id, contract_id, email
            FROM invites
            WHERE token = %s
            LIMIT 1
            """,
            (payload.token,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="invalid invite token")

        invite_id, contract_id, invited_email = row

        if token_email and invited_email and token_email.lower() != invited_email.lower():
            raise HTTPException(status_code=403, detail="email mismatch")

        # users を upsert（user-check が users 参照なのでここで担保）
        cur.execute(
            """
            INSERT INTO users (user_id, email, display_name, created_at, last_login_at)
            VALUES (%s, %s, %s, NOW(), NOW())
            ON CONFLICT (user_id) DO UPDATE
              SET email = EXCLUDED.email,
                  last_login_at = NOW();
            """,
            (uid, token_email, user.get("name") or ""),
        )

        # user_contracts に紐づけ（既にあれば有効化）
        cur.execute(
            """
            INSERT INTO user_contracts (user_id, contract_id, role, status)
            VALUES (%s, %s, 'member', 'active')
            ON CONFLICT (user_id, contract_id) DO UPDATE
              SET status = 'active';
            """,
            (uid, contract_id),
        )

        # token を無効化（再利用防止）
        cur.execute(
            "UPDATE invites SET token = NULL WHERE invite_id = %s",
            (invite_id,),
        )

    conn.commit()
    return {"ok": True, "contract_id": str(contract_id)}


app.include_router(router)
