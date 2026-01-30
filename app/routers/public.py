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
    """
    入口判定（DBなし）
    - ログイン＝認証済み（uid/emailが取れている）
    - アプリ内ユーザーは users/<uid>/user.json があるときだけ存在
    - アカウントは 1ユーザー=1アカウント（acc_<uid>）で固定
    - テナント一覧と、契約（テナント1:1）の有無を返す
    """
    email = (user.get("email") or "").strip()
    uid = (user.get("uid") or "").strip()

    if not uid:
        raise HTTPException(status_code=400, detail="no uid in session")
    if not email:
        raise HTTPException(status_code=400, detail="no email in session")

    bucket = _bucket()

    account_id = _account_id_for_uid(uid)

    # アプリ内ユーザー（＝アカウント作成後に作る想定）
    user_exists = _blob_exists(bucket, f"users/{uid}/user.json")

    # アカウント実体（accounts/<account_id>/account.json）
    account_exists = _blob_exists(bucket, f"accounts/{account_id}/account.json")

    tenants: list[dict[str, Any]] = []
    if account_exists:
        tenants = _list_tenants(bucket, account_id)

    return {
        "authed": True,
        "uid": uid,
        "email": email,
        "user_exists": user_exists,
        "account_id": account_id,           # ★ これがないと tenants 画面に渡せない
        "account_exists": account_exists,   # ★ 入口分岐に使える
        "tenants": tenants,                 # ★ tenantが無ければ tenants作成へ
    }


@router.get("/v1/system")
def system():
    """
    システム設定（認証なしで参照してもよい想定）
    """
    bucket = _bucket()
    return _read_json(bucket, "settings/system.json")
