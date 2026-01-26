from fastapi import Depends, HTTPException
from app.deps.auth import require_user
from app.deps.db import get_db

def require_contract_admin(uid: str, contract_id: str, conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM user_contracts
            WHERE user_id=%s
              AND contract_id=%s
              AND role='admin'
              AND status='active'
            LIMIT 1
            """,
            (uid, contract_id),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=403, detail="admin only for this contract")

def require_admin(user=Depends(require_user), conn=Depends(get_db)):
    uid = user["uid"]
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM user_contracts
            WHERE user_id=%s
              AND role='admin'
              AND status='active'
            LIMIT 1
            """,
            (uid,),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=403, detail="admin required")
    return user
