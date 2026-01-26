import os
import uuid
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel, Field
from google.cloud import storage
from google.oauth2 import service_account

from app.deps.auth import require_user
from app.deps.db import get_db
from app.core.settings import BUCKET_NAME, MAX_DIALOGUE_PER_MONTH, month_key_jst
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
        # 現在有効な対話データ（contracts側）
        cur.execute("""
            SELECT active_dialogue_object_key
            FROM contracts
            WHERE contract_id = %s
            LIMIT 1
        """, (contract_id,))
        row = cur.fetchone()
        if row:
            active_object_key = row[0]

        # upload_logs（対話データのみ）
        cur.execute("""
            SELECT upload_id, object_key, month_key, created_at, kind
            FROM upload_logs
            WHERE contract_id = %s
              AND kind = 'dialogue'
            ORDER BY created_at DESC
            LIMIT 200
        """, (contract_id,))
        rows = cur.fetchall() or []

        for (upload_id, object_key, month_key, created_at, kind) in rows:
            items.append({
                "upload_id": str(upload_id),
                "object_key": object_key,
                "month_key": month_key,
                "created_at": created_at.isoformat() if created_at else None,
                "kind": kind,
            })

    return {
        "contract_id": contract_id,
        "active_object_key": active_object_key,
        "items": items,
    }


# ---------------------------------------------------------
# 対話データ（dialogue）有効化（1つ選ぶ）
#   POST /v1/admin/dialogues/activate
#   body: { contract_id, object_key }
# ---------------------------------------------------------
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

    # 指定 object_key がこの契約の dialogue として存在するか（保険）
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1
            FROM upload_logs
            WHERE contract_id = %s
              AND kind = 'dialogue'
              AND object_key = %s
            LIMIT 1
        """, (contract_id, object_key))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="dialogue object_key not found in upload_logs")

        # 有効化（contracts に 1本だけ持つ）
        cur.execute("""
            UPDATE contracts
            SET active_dialogue_object_key = %s,
                updated_at = NOW()
            WHERE contract_id = %s
        """, (object_key, contract_id))

    conn.commit()
    return {"ok": True, "contract_id": contract_id, "active_object_key": object_key}


# ---------------------------------------------------------
# QA作成（開始）
#   POST /v1/admin/dialogues/build-qa
#   body: { contract_id }
#   ※ 実体のQA生成は後で実装する前提。ここは「開始できる」だけ。
# ---------------------------------------------------------
@router.post("/v1/admin/dialogues/build-qa")
def build_qa_from_active_dialogue(
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
        cur.execute("""
            SELECT active_dialogue_object_key
            FROM contracts
            WHERE contract_id = %s
            LIMIT 1
        """, (contract_id,))
        row = cur.fetchone()
        active_key = row[0] if row else None

    if not active_key:
        raise HTTPException(status_code=409, detail="active dialogue data is not selected")

    # ここではジョブキューなどはまだ持たない（余計な改造をしない）
    # 「有効な対話データが決まっていて、開始要求が通る」ことだけ保証する
    return {
        "ok": True,
        "contract_id": contract_id,
        "active_object_key": active_key,
        "status": "requested",
    }


# =========================================================
# Upload URLs (Signed URL)
# =========================================================

@router.post("/v1/admin/upload-finalize")
def upload_finalize(
    payload: dict,
    user=Depends(require_user),
    conn=Depends(get_db),
):
    contract_id = (payload.get("contract_id") or "").strip()
    object_key = (payload.get("object_key") or "").strip()
    upload_id = (payload.get("upload_id") or "").strip()

    if not contract_id or not object_key or not upload_id:
        raise HTTPException(status_code=400, detail="contract_id, object_key, upload_id are required")

    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid in token")

    require_contract_admin(uid, contract_id, conn)

    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1
            FROM upload_logs
            WHERE upload_id = %s
              AND contract_id = %s
              AND object_key = %s
            LIMIT 1
        """, (upload_id, contract_id, object_key))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="upload log not found")

    return {"ok": True}

class UploadUrlRequest(BaseModel):
    contract_id: str
    kind: str = "dialogue"
    filename: str
    content_type: str
    note: str | None = None

@router.post("/v1/admin/upload-url")
def create_upload_url(
    req: UploadUrlRequest,
    user=Depends(require_user),
    conn=Depends(get_db),
):
    # 1) 認証
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid in token")

    # 2) 入力
    contract_id = (req.contract_id or "").strip()
    if not contract_id:
        raise HTTPException(status_code=400, detail="contract_id is required")

    # 3) 契約 admin チェック
    require_contract_admin(uid, contract_id, conn)

    # 4) 月5件制限（dialogueのみ）
    mk = month_key_jst()
    if req.kind == "dialogue":
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM upload_logs
                WHERE contract_id = %s
                  AND kind = 'dialogue'
                  AND month_key = %s
            """, (contract_id, mk))
            cnt = int(cur.fetchone()[0] or 0)

        if cnt >= MAX_DIALOGUE_PER_MONTH:
            raise HTTPException(
                status_code=409,
                detail=f"dialogue uploads limit reached: {cnt}/{MAX_DIALOGUE_PER_MONTH} for {mk}"
            )

    # 5) object_key（contracts/ で統一）
    upload_id = uuid.uuid4()
    safe_name = (req.filename or "file").replace("/", "_").replace("\\", "_")
    object_key = f"contracts/{contract_id}/{mk}/{upload_id}_{safe_name}"

    # 6) 台帳INSERT（乱発抑止）
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO upload_logs (upload_id, contract_id, kind, object_key, month_key, created_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
        """, (str(upload_id), contract_id, req.kind, object_key, mk))
    conn.commit()

    # 7) 署名URL（Secret Managerを /secrets にマウントした鍵で署名する）
    signer_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or "/secrets/ank-gcs-signer"

    # Secret が正しくマウントされていない場合はここで落とす（原因が分かりやすい）
    if not os.path.exists(signer_path):
        raise HTTPException(status_code=500, detail=f"signer file not found: {signer_path}")

    credentials = service_account.Credentials.from_service_account_file(signer_path)

    client = storage.Client(credentials=credentials)
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(object_key)

    url = blob.generate_signed_url(
        version="v4",
        expiration=timedelta(minutes=15),
        method="PUT",
        content_type=req.content_type,
    )

    return {
        "upload_id": str(upload_id),
        "object_key": object_key,
        "upload_url": url,
    }

# =========================================================
# Existing Admin APIs
# =========================================================


