from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from typing import Optional

from app.deps.auth import require_user
from app.deps.db import get_db

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
        # このユーザーが、その契約の admin か確認（updateと同じ条件）
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


