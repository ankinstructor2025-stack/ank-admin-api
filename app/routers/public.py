from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from google.cloud import storage

from app.core.settings import BUCKET_NAME
from app.deps.auth import require_user

router = APIRouter()
_storage = storage.Client()


# =========================
# Common helpers
# =========================
def _bucket():
    if not BUCKET_NAME:
        raise HTTPException(status_code=500, detail="BUCKET_NAME is not set")
    return _storage.bucket(BUCKET_NAME)


def _read_json(bucket, path: str) -> dict:
    blob = bucket.blob(path)
    if not blob.exists():
        raise HTTPException(status_code=404, detail=f"not found: {path}")
    text = blob.download_as_text(encoding="utf-8")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail=f"invalid json: {path}")


def _blob_exists(bucket, path: str) -> bool:
    return bucket.blob(path).exists()


def _account_id_for_uid(uid: str) -> str:
    # 1ユーザー=1アカウント（複数アカウントは持たない）
    return f"acc_{uid}"


def _list_tenants(bucket, account_id: str) -> list[dict[str, Any]]:
    prefix = f"accounts/{account_id}/tenants/"
    tenants: list[dict[str, Any]] = []

    print(f"[list_tenants] prefix={prefix}")

    count = 0
    for b in bucket.list_blobs(prefix=prefix):
        count += 1
        print(f"[list_tenants] blob={b.name}")

        if not b.name.endswith("/tenant.json"):
            continue

        parts = b.name.split("/")
        if len(parts) < 5:
            print(f"[list_tenants] skip short parts={parts}")
            continue

        tenant_id = parts[-2]
        print(f"[list_tenants] tenant_id={tenant_id}")

        name = ""
        status = ""
        try:
            data = json.loads(b.download_as_text(encoding="utf-8"))
            name = (data.get("name") or "").strip()
            status = (data.get("status") or "").strip()
        except Exception as e:
            print(f"[list_tenants] json error tenant_id={tenant_id} err={e}")

        contract_path = f"accounts/{account_id}/tenants/{tenant_id}/contract.json"
        has_contract = _blob_exists(bucket, contract_path)

        tenants.append({
            "tenant_id": tenant_id,
            "name": name,
            "status": status,
            "has_contract": has_contract,
        })

    print(f"[list_tenants] total_blobs={count} tenants_len={len(tenants)}")
    return tenants



# =========================
# Public APIs (入口系のみ)
# =========================
@router.get("/v1/session")
def get_session(user=Depends(require_user)):
    email = (user.get("email") or "").strip()
    uid = (user.get("uid") or "").strip()

    if not uid:
        raise HTTPException(status_code=400, detail="no uid in session")
    if not email:
        raise HTTPException(status_code=400, detail="no email in session")

    bucket = _bucket()
    account_id = _account_id_for_uid(uid)

    user_exists = _blob_exists(bucket, f"users/{uid}/user.json")
    account_exists = _blob_exists(bucket, f"accounts/{account_id}/account.json")

    tenants: list[dict[str, Any]] = []
    if account_exists:
        tenants = _list_tenants(bucket, account_id)

    # ----------------------------
    # ★ QA専用判定（正仕様）
    # ----------------------------
    tenant_id = None
    qa_only = False

    if len(tenants) == 1:
        tenant_id = tenants[0].get("tenant_id")

        if tenant_id:
            tenant_path = (
                f"accounts/{account_id}/tenants/{tenant_id}/tenant.json"
            )
            if _blob_exists(bucket, tenant_path):
                tenant = _read_json(bucket, tenant_path)
                if isinstance(tenant, dict):
                    qa_only = (tenant.get("plan_id") == "basic")

    # ----------------------------

    return {
        "authed": True,
        "uid": uid,
        "email": email,
        "user_exists": user_exists,
        "account_id": account_id,
        "account_exists": account_exists,
        "tenants": tenants,

        # ★ UI判定用
        "tenant_id": tenant_id,
        "qa_only": qa_only,
    }

@router.get("/v1/system")
def system():
    """
    システム設定（認証なしで参照してもよい想定）
    """
    bucket = _bucket()
    return _read_json(bucket, "settings/system.json")
