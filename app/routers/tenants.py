from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from google.cloud import storage

from app.deps.auth import require_user
from app.core.settings import BUCKET_NAME

router = APIRouter()
_storage = storage.Client()


def _bucket():
    if not BUCKET_NAME:
        raise HTTPException(status_code=500, detail="UPLOAD_BUCKET is not set")
    return _storage.bucket(BUCKET_NAME)


@router.get("/v1/tenants")
def list_tenants(
    account_id: str = Query(...),
    user=Depends(require_user),
):
    uid = user.get("uid")
    if not uid:
        raise HTTPException(status_code=400, detail="no uid")

    bucket = _bucket()
    prefix = f"accounts/{account_id}/tenants/"

    tenants = []
    for b in _storage.list_blobs(bucket, prefix=prefix):
        if not b.name.endswith("/tenant.json"):
            continue
        data = json.loads(b.download_as_text())
        tenants.append({
            "tenant_id": data["tenant_id"],
            "name": data.get("name")
        })

    tenants.sort(key=lambda x: x["tenant_id"])
    return {"tenants": tenants}


@router.post("/v1/tenant")
def create_tenant(
    payload: dict,
    user=Depends(require_user),
):
    uid = user.get("uid")
    if not uid:
        raise HTTPException(status_code=400, detail="no uid")

    account_id = (payload.get("account_id") or "").strip()
    if not account_id:
        raise HTTPException(status_code=400, detail="account_id required")

    name = (payload.get("name") or "").strip()

    bucket = _bucket()

    tenant_id = f"ten_{uuid.uuid4().hex[:12]}"
    now = datetime.now(timezone.utc).isoformat()

    # tenant 実体
    bucket.blob(
        f"accounts/{account_id}/tenants/{tenant_id}/tenant.json"
    ).upload_from_string(
        json.dumps({
            "tenant_id": tenant_id,
            "account_id": account_id,
            "name": name,
            "created_at": now
        }, ensure_ascii=False),
        content_type="application/json"
    )

    # user 側索引（任意だが後で効く）
    bucket.blob(
        f"users/{uid}/tenants/{tenant_id}.json"
    ).upload_from_string(
        json.dumps({
            "tenant_id": tenant_id,
            "account_id": account_id,
            "role": "admin",
            "created_at": now
        }, ensure_ascii=False),
        content_type="application/json"
    )

    return {
        "tenant_id": tenant_id
    }
