from fastapi import APIRouter, Depends, HTTPException, Query

from app.deps.auth import require_user
from app.deps.db import get_db
from app.services.contracts_acl import require_contract_admin

router = APIRouter()

@router.get("/v1/admin/dialogues")
def list_dialogues(
    contract_id: str = Query(...),
    user=Depends(require_user),
    conn=Depends(get_db),
):
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid in token")

    contract_id = (contract_id or "").strip()
    if not contract_id:
        raise HTTPException(status_code=400, detail="contract_id is required")

    require_contract_admin(uid, contract_id, conn)

    active_object_key = None
    items = []

    with conn.cursor() as cur:
        # 現在有効な対話データ
        cur.execute(
            """
            SELECT active_dialogue_object_key
            FROM contracts
            WHERE contract_id = %s
            LIMIT 1
            """,
            (contract_id,),
        )
        row = cur.fetchone()
        if row:
            active_object_key = row[0]

        # 対話データの一覧
        cur.execute(
            """
            SELECT upload_id, object_key, month_key, created_at, kind
            FROM upload_logs
            WHERE contract_id = %s
              AND kind = 'dialogue'
            ORDER BY created_at DESC
            LIMIT 200
            """,
            (contract_id,),
        )
        rows = cur.fetchall() or []

        for (upload_id, object_key, month_key, created_at, kind) in rows:
            items.append(
                {
                    "upload_id": str(upload_id),
                    "object_key": object_key,
                    "month_key": month_key,
                    "created_at": created_at.isoformat() if created_at else None,
                    "kind": kind,
                }
            )

    return {
        "contract_id": contract_id,
        "active_object_key": active_object_key,
        "items": items,
    }


@router.post("/v1/admin/dialogues/activate")
def activate_dialogue(
    payload: dict,
    user=Depends(require_user),
    conn=Depends(get_db),
):
    contract_id = (payload.get("contract_id") or "").strip()
    object_key = (payload.get("object_key") or "").strip()

    if not contract_id or not object_key:
        raise HTTPException(status_code=400, detail="contract_id and object_key are required")

    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid in token")

    require_contract_admin(uid, contract_id, conn)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM upload_logs
            WHERE contract_id = %s
              AND kind = 'dialogue'
              AND object_key = %s
            LIMIT 1
            """,
            (contract_id, object_key),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="dialogue object_key not found in upload_logs")

        cur.execute(
            """
            UPDATE contracts
            SET active_dialogue_object_key = %s,
                updated_at = NOW()
            WHERE contract_id = %s
            """,
            (object_key, contract_id),
        )

    conn.commit()
    return {"ok": True, "contract_id": contract_id, "active_object_key": object_key}


@router.post("/v1/admin/dialogues/build-qa")
def build_qa(
    payload: dict,
    user=Depends(require_user),
    conn=Depends(get_db),
):
    contract_id = (payload.get("contract_id") or "").strip()
    if not contract_id:
        raise HTTPException(status_code=400, detail="contract_id is required")

    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid in token")

    require_contract_admin(uid, contract_id, conn)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT active_dialogue_object_key
            FROM contracts
            WHERE contract_id = %s
            LIMIT 1
            """,
            (contract_id,),
        )
        row = cur.fetchone()
        active_key = row[0] if row else None

    if not active_key:
        raise HTTPException(status_code=409, detail="active dialogue data is not selected")

    return {
        "ok": True,
        "contract_id": contract_id,
        "active_object_key": active_key,
        "status": "requested",
    }
