import os
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from typing import Optional

from app.deps.db import get_db
from app.services.contracts_acl import require_admin
from app.deps.auth import require_user

router = APIRouter()

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


