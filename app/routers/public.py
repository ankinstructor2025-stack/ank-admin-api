from __future__ import annotations

import json
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from app.deps.auth import require_user

from google.cloud import storage
from app.core.settings import BUCKET_NAME

router = APIRouter()
_storage = storage.Client()


# =========================
# GCS helpers
# =========================
def _bucket():
    if not BUCKET_NAME:
        raise HTTPException(status_code=500, detail="UPLOAD_BUCKET is not set")
    return _storage.bucket(BUCKET_NAME)


def _blob_exists(bucket, path: str) -> bool:
    return bucket.blob(path).exists()


def _read_json(bucket, path: str) -> dict:
    blob = bucket.blob(path)
    if not blob.exists():
        raise HTTPException(status_code=404, detail="not found")
    text = blob.download_as_text(encoding="utf-8")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="invalid json")


def _list_account_index(bucket, auth_key: str) -> list[dict[str, Any]]:
    """
    users/<auth_key>/accounts/<account_id>.json を列挙して、
    [{account_id, role, status}, ...] を返す。
    """
    prefix = f"users/{auth_key}/accounts/"
    results: list[dict[str, Any]] = []

    # GCSのprefix配下だけを見る（全走査しない）
    for b in _storage.list_blobs(bucket, prefix=prefix):
        name = b.name
        if not name.endswith(".json"):
            continue

        # users/<auth_key>/accounts/<account_id>.json
        account_id = name[len(prefix):].replace(".json", "").strip()
        if not account_id:
            continue

        try:
            data = _read_json(bucket, name)
        except HTTPException:
            # 壊れたindexは無視（運用優先）
            continue

        results.append(
            {
                "account_id": data.get("account_id") or account_id,
                "role": (data.get("role") or "").strip() or None,
                "status": (data.get("status") or "active").strip(),
            }
        )

    # 表示安定のためソート
    results.sort(key=lambda x: (x.get("account_id") or ""))
    return results


# =========================
# /v1/session (rebuild)
# =========================
@router.get("/v1/session")
def get_session(user=Depends(require_user)):
    """
    フロントの分岐用セッション情報（作り直し版 / DBなし）。

    役割：
    - 認証済みか
    - このユーザーが「サービス登録済み」か（user.jsonの有無）
    - 所属Account一覧（users/<uid>/accounts/* のみ。全走査しない）

    注意：
    - 旧フィールド（has_active_contract, contracts, role 等）は返さない（ゴミを残さない）
    """
    email = (user.get("email") or "").strip()
    uid = (user.get("uid") or "").strip()

    if not uid:
        raise HTTPException(status_code=400, detail="no uid in session")
    if not email:
        raise HTTPException(status_code=400, detail="no email in session")

    # 当面は auth_key = uid
    # 将来 MS を入れるなら auth_key = "ms__<oid>" / "google__<uid>" に寄せる
    auth_key = uid

    bucket = _bucket()

    user_path = f"users/{auth_key}/user.json"
    user_exists = _blob_exists(bucket, user_path)

    accounts: list[dict[str, Any]] = []
    if user_exists:
        accounts = _list_account_index(bucket, auth_key)

    return {
        "authed": True,
        "uid": uid,
        "email": email,
        "user_exists": user_exists,
        "accounts": accounts,  # [{account_id, role, status}, ...]
    }


# =========================
# /v1/pricing (DBなし)
# =========================
@router.get("/v1/pricing")
def pricing():
    """
    DBなし運用のため、pricingは当面「固定」または「空」で返す。

    ここをGCSの settings/pricing.json に寄せたいなら、次の形で拡張する：
      settings/pricing.json を読み込んで返す
    ただ、まずは Cloud SQL 脱出を優先するので固定レスポンスにしている。
    """
    return {
        "seats": [],
        "knowledge_count": [],
        "search_limit": {"per_user_per_day": 0, "note": ""},
        "poc": None,
    }


# =========================
# Legacy endpoints removed
# =========================
# 以下は Cloud SQL 前提のため削除：
# - /v1/contract
# - /v1/contracts
# - /v1/user-check
# - /v1/debug/users-select
#
# 必要な一覧は「tenants」「accounts」側のAPIとして別ファイルで実装する。
