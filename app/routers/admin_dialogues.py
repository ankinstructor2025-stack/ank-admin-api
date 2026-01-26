import os
import uuid
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query

from app.deps.auth import require_user
from app.deps.db import get_db
from app.services.contracts_acl import require_contract_admin
from app.core.settings import BUCKET_NAME

router = APIRouter()

MAX_DIALOGUE_PER_MONTH = 5

def month_key_jst() -> str:
    return datetime.now().strftime("%Y-%m")

@router.get("/v1/admin/dialogues")
def list_dialogues(
    contract_id: str = Query(...),
    user=Depends(require_user),
    conn=Depends(get_db),
):
    uid = user["uid"]
    require_contract_admin(uid, contract_id, conn)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT upload_id, object_key, created_at
            FROM upload_logs
            WHERE contract_id=%s
            ORDER BY created_at DESC
            LIMIT 100
            """,
            (contract_id,),
        )
        rows = cur.fetchall()

    return {
        "items": [
            {
                "upload_id": r[0],
                "object_key": r[1],
                "created_at": r[2].isoformat() if r[2] else None,
            }
            for r in rows
        ]
    }
