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


def _list_account_index(bucket, auth_key: str) -> list[dict[str, Any]]:
    """
    users/<auth_key>/accounts/<account_id>.json を列挙して
    [{account_id, role, status}, ...] を返す（全走査しない）
    """
    prefix = f"users/{auth_key}/accounts/"
    out: list[dict[str, Any]] = []
    for b in _storage.list_blobs(bucket, prefix=prefix):
        if not b.name.endswith(".json"):
            continue
        try:
            data = json.loads(b.download_as_text(encoding="utf-8"))
        except Exception:
            continue
        out.append(
            {
                "account_id": data.get("account_id") or "",
                "role": data.get("role") or "",
                "status": data.get("status") or "",
            }
        )
    return out


# =========================
# Public APIs (入口系のみ)
# =========================
@router.get("/v1/session")
def get_session(user=Depends(require_user)):
    """
    入口判定（DBなし）
    - users/<uid>/user.json があるか
    - users/<uid>/accounts/*.json を返す
    """
    email = (user.get("email") or "").strip()
    uid = (user.get("uid") or "").strip()

    if not uid:
        raise HTTPException(status_code=400, detail="no uid in session")
    if not email:
        raise HTTPException(status_code=400, detail="no email in session")

    bucket = _bucket()
    auth_key = uid  # 将来 MS 対応するならここを provider付きにする

    user_exists = _blob_exists(bucket, f"users/{auth_key}/user.json")
    accounts = _list_account_index(bucket, auth_key)

    return {
        "authed": True,
        "uid": uid,
        "email": email,
        "user_exists": user_exists,
        "accounts": accounts,
    }


@router.get("/v1/system")
def system():
    """
    システム設定（認証なしで参照してもよい想定）
    """
    bucket = _bucket()
    return _read_json(bucket, "settings/system.json")
