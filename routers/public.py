from fastapi import APIRouter, Depends, Query, HTTPException
from app.deps.auth import require_user
from app.deps.db import get_db

router = APIRouter()

@router.get("/v1/session")
def get_session(
    user=Depends(require_user),
    conn=Depends(get_db),
):
    """
    フロントの分岐用セッション情報。
    - Firebaseログイン済み（Bearer token 必須）
    - users に登録済みか
    - user_contracts の状態（active があるか）
    """
    email = (user.get("email") or "").strip()
    uid = (user.get("uid") or "").strip()

    if not uid:
        raise HTTPException(status_code=400, detail="no uid in session")
    if not email:
        raise HTTPException(status_code=400, detail="no email in session")

    user_exists = False
    user_id: Optional[str] = None
    contracts = []
    has_active_contract = False
    active_roles = set()

    with conn.cursor() as cur:
        # users は email で判定（email は unique の想定）
        cur.execute(
            """
            SELECT user_id
            FROM users
            WHERE email = %s
            LIMIT 1;
            """,
            (email,),
        )
        row = cur.fetchone()
        if row:
            user_exists = True
            user_id = str(row[0])

            # user_contracts を返す（status が active かどうかを見る）
            cur.execute(
                """
                SELECT contract_id, role, status
                FROM user_contracts
                WHERE user_id = %s
                ORDER BY contract_id;
                """,
                (user_id,),
            )
            rows = cur.fetchall() or []
            for (contract_id, role, status) in rows:
                c = {
                    "contract_id": str(contract_id),
                    "role": role,
                    "status": status,
                }
                contracts.append(c)
                if status == "active":
                    has_active_contract = True
                    if role:
                        active_roles.add(role)

    # “いま有効なロール” を1つ返す（必要ならフロントで使える）
    # admin が1つでも active なら admin 優先、なければ member
    role = None
    if "admin" in active_roles:
        role = "admin"
    elif "member" in active_roles:
        role = "member"

    return {
        "authed": True,
        "uid": uid,
        "email": email,
        "user_exists": user_exists,
        "user_id": user_id,                # users にいれば入る
        "has_active_contract": has_active_contract,
        "role": role,                      # active 契約から推定（無ければ null）
        "contracts": contracts,            # [{contract_id, role, status}, ...]
    }

@router.get("/v1/pricing")
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


@router.get("/v1/contract")
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

@router.get("/v1/contracts")
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

@router.get("/v1/user-check")
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


@router.get("/v1/debug/users-select")
def users_select(conn=Depends(get_db)):
    with conn.cursor() as cur:
        cur.execute("SELECT user_id FROM users;")
        rows = cur.fetchall()

    return {
        "rows": rows,
        "row_count": len(rows),
    }



