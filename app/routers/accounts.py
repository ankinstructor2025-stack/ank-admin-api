# app/routers/accounts.py
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from app.deps.auth import require_user
from google.cloud import storage
from app.core.settings import BUCKET_NAME

router = APIRouter()
_storage = storage.Client()


def _bucket():
    if not BUCKET_NAME:
        raise HTTPException(status_code=500, detail="UPLOAD_BUCKET is not set")
    return _storage.bucket(BUCKET_NAME)


@router.post("/v1/account")
def create_account(
    payload: dict,
    user=Depends(require_user),
):
    uid = (user.get("uid") or "").strip()
    email = (user.get("email") or "").strip()

    if not uid:
        raise HTTPException(status_code=400, detail="no uid")
    if not email:
        raise HTTPException(status_code=400, detail="no email")

    name = (payload.get("name") or "").strip()

    auth_key = uid
    bucket = _bucket()

    # 1) user.json（無ければ作る）
    user_path = f"users/{auth_key}/user.json"
    if not bucket.blob(user_path).exists():
        bucket.blob(user_path).upload_from_string(
            json.dumps({
                "uid": uid,
                "email": email,
                "created_at": datetime.now(timezone.utc).isoformat()
            }, ensure_ascii=False),
            content_type="application/json"
        )

    # 2) account_id 発行
    account_id = f"acc_{uuid.uuid4().hex[:12]}"

    now = datetime.now(timezone.utc).isoformat()

    # 3) user 側 index
    bucket.blob(
        f"users/{auth_key}/accounts/{account_id}.json"
    ).upload_from_string(
        json.dumps({
            "account_id": account_id,
            "role": "owner",
            "status": "active",
            "created_at": now
        }, ensure_ascii=False),
        content_type="application/json"
    )

    # 4) account 実体
    bucket.blob(
        f"accounts/{account_id}/account.json"
    ).upload_from_string(
        json.dumps({
            "account_id": account_id,
            "name": name,
            "owner_uid": uid,
            "created_at": now
        }, ensure_ascii=False),
        content_type="application/json"
    )

    return {
        "account_id": account_id
    }
