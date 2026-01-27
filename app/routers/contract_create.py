import os
import json
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.deps.auth import require_user

# GCS
from google.cloud import storage

from app.core.settings import BUCKET_NAME

router = APIRouter()

# ==========
# Settings
# ==========
APP_BASE_URL = os.environ.get(
    "APP_BASE_URL",
    "https://ankinstructor2025-stack.github.io/ank-knowledge"
)
FROM_EMAIL = os.environ.get("INVITE_FROM_EMAIL", "ank.instructor2025@gmail.com")

_storage_client = storage.Client()


class ContractCreate(BaseModel):
    user_id: str
    email: str
    display_name: str | None = None
    seat_limit: int
    knowledge_count: int
    monthly_amount_yen: int
    note: str | None = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _bucket():
    if not BUCKET_NAME:
        raise HTTPException(status_code=500, detail="BUCKET_NAME is not set")
    return _storage_client.bucket(BUCKET_NAME)


def _upload_json(bucket, path: str, data: dict, *, if_generation_match=None):
    blob = bucket.blob(path)
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    blob.upload_from_string(
        payload,
        content_type="application/json; charset=utf-8",
        if_generation_match=if_generation_match,
    )
    return blob


@router.post("/v1/contract")
def create_contract(
    payload: ContractCreate,
    user=Depends(require_user),
):
    """
    旧: Cloud SQL の users / contracts / user_contracts にINSERT
    新: GCS に契約フォルダと初期ファイルを作成

    作成物:
      tenants/{contract_id}/.keep
      tenants/{contract_id}/contract.json
      tenants/{contract_id}/members/{uid}.json
      tenants/{contract_id}/meta.json
      tenants/{contract_id}/db/read.db   (空のプレースホルダ)
      tenants/{contract_id}/db/write.db  (空のプレースホルダ)
    """
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=401, detail="unauthorized")

    # フロントから user_id が来る前提なので整合性だけ見る
    if (payload.user_id or "").strip() != uid:
        raise HTTPException(status_code=403, detail="uid mismatch")

    # 最低限の入力チェック
    email = (payload.email or "").strip()
    if not email:
        raise HTTPException(status_code=400, detail="email is required")

    # contract_id はファイル階層に使うので、ハイフン無しhexにしておく
    contract_id = uuid.uuid4().hex
    now = _now_iso()

    bucket = _bucket()

    base = f"tenants/{contract_id}/"

    # 1) “フォルダ”作成（.keep）
    bucket.blob(base + ".keep").upload_from_string(
        b"", content_type="application/octet-stream"
    )

    # 2) contract.json
    contract = {
        "contract_id": contract_id,
        "status": "active",
        "start_at": now,  # DBのNOW()相当
        "seat_limit": int(payload.seat_limit),
        "knowledge_count": int(payload.knowledge_count),
        "payment_method_configured": False,  # 旧contractsと合わせる
        "monthly_amount_yen": int(payload.monthly_amount_yen),
        "note": (payload.note or "").strip() or None,
        "created_at": now,
        "updated_at": now,
        "schema_version": 1,
        "owner": {
            "uid": uid,
            "email": email,
            "display_name": payload.display_name or "",
        },
    }
    _upload_json(bucket, base + "contract.json", contract)

    # 3) members/{uid}.json（owner/adminの起点）
    # 旧実装では user_contracts に role='admin' で入れてたので、
    # ファイルでは owner として保存（ownerは別枠、admin枠5は後で追加）
    member = {
        "uid": uid,
        "email": email,
        "display_name": payload.display_name or "",
        "role": "owner",
        "status": "active",
        "created_at": now,
        "updated_at": now,
    }
    _upload_json(bucket, base + f"members/{uid}.json", member)

    # 4) meta.json（ロックやDB世代管理の置き場）
    meta = {
        "contract_id": contract_id,
        "locked": False,
        "lock_reason": "",
        "lock_owner_uid": "",
        "lock_created_at": None,
        "db_generation": 0,
        "schema_version": 1,
        "created_at": now,
        "updated_at": now,
    }
    _upload_json(bucket, base + "meta.json", meta)

    # 5) DBプレースホルダ（実体は後で ank-knowledge-api が作る/差し替える）
    bucket.blob(base + "db/read.db").upload_from_string(
        b"", content_type="application/octet-stream"
    )
    bucket.blob(base + "db/write.db").upload_from_string(
        b"", content_type="application/octet-stream"
    )

    return {"contract_id": contract_id, "status": "active"}
