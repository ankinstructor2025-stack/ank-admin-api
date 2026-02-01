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
    """
    accounts/<account_id>/tenants/<tenant_id>/tenant.json を列挙して tenants を返す。
    契約はテナント1:1なので、
      accounts/<account_id>/tenants/<tenant_id>/contract.json の有無で has_contract を判定。
    """
    prefix = f"accounts/{account_id}/tenants/"
    tenants: list[dict[str, Any]] = []

    # tenant.json を手掛かりに tenant_id を拾う
    for b in _storage.list_blobs(bucket, prefix=prefix):
        if not b.name.endswith("/tenant.json"):
            continue

        # accounts/<aid>/tenants/<tid>/tenant.json
        parts = b.name.split("/")
        # ["accounts", aid, "tenants", tid, "tenant.json"]
        if len(parts) < 5:
            continue
        tenant_id = parts[3]

        # tenant.json を読む（壊れてたら最低限で返す）
        name = ""
        status = ""
        try:
            data = json.loads(b.download_as_text(encoding="utf-8"))
            name = (data.get("name") or "").strip()
            status = (data.get("status") or "").strip()
        except Exception:
            pass

        contract_path = f"accounts/{account_id}/tenants/{tenant_id}/contract.json"
        has_contract = _blob_exists(bucket, contract_path)

        tenants.append(
            {
                "tenant_id": tenant_id,
                "name": name,
                "status": status,
                "has_contract": has_contract,
            }
        )

    return tenants


# =========================
# Public APIs (入口系のみ)
# =========================
@router.get("/v1/session")
def get_session(user=Depends(require_user)):
    uid = user["uid"]

    # users/{uid}/user.json
    user_json = read_json(f"users/{uid}/user.json")
    if not user_json:
        raise HTTPException(status_code=403, detail="user.json not found")

    account_id = user_json.get("account_id")
    if not account_id:
        raise HTTPException(status_code=500, detail="account_id missing")

    # users/{uid}/tenants/ は 1:1 前提
    tenant_dirs = list_dirs(f"users/{uid}/tenants/")
    if not tenant_dirs:
        raise HTTPException(status_code=403, detail="tenant not found")

    tenant_id = tenant_dirs[0]

    # accounts/{account_id}/tenants/{tenant_id}/contract.json
    contract = read_json(
        f"accounts/{account_id}/tenants/{tenant_id}/contract.json"
    )
    if not contract:
        raise HTTPException(status_code=403, detail="contract not found")

    plan_id = contract.get("plan_id")
    if not plan_id:
        raise HTTPException(status_code=500, detail="plan_id missing")

    return {
        "uid": uid,
        "account_id": account_id,
        "tenant_id": tenant_id,
        "plan_id": plan_id,
    }

@router.get("/v1/system")
def system():
    """
    システム設定（認証なしで参照してもよい想定）
    """
    bucket = _bucket()
    return _read_json(bucket, "settings/system.json")
