# app/routers/accounts.py
from __future__ import annotations

import json
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from google.cloud import storage

from app.core.settings import BUCKET_NAME
from app.deps.auth import require_user

router = APIRouter()
_storage = storage.Client()


def _bucket():
    if not BUCKET_NAME:
        # いまの文言が "UPLOAD_BUCKET" になっていて混乱しやすいので修正
        raise HTTPException(status_code=500, detail="BUCKET_NAME is not set")
    return _storage.bucket(BUCKET_NAME)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _account_id_for_uid(uid: str) -> str:
    # 1ユーザー=1アカウントを確定させる（複数accountを作れない）
    return f"acc_{uid}"


@router.get("/v1/account")
def get_account(user=Depends(require_user)):
    """
    1ユーザー=1アカウント前提:
    - account_id は acc_{uid} に固定
    - 無ければ 404
    """
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid")

    bucket = _bucket()
    account_id = _account_id_for_uid(uid)

    account_path = f"accounts/{account_id}/account.json"
    blob = bucket.blob(account_path)
    if not blob.exists():
        raise HTTPException(status_code=404, detail="account not found")

    data = json.loads(blob.download_as_text(encoding="utf-8"))
    return {
        "account": data
    }


@router.post("/v1/account")
def create_account(payload: dict, user=Depends(require_user)):
    """
    アカウント作成時に初めて「アプリ内ユーザー(user.json)」も作る。
    ただしアカウントは 1ユーザー=1件固定なので、既に存在する場合は既存を返す。
    """
    uid = (user.get("uid") or "").strip()
    email = (user.get("email") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid")
    if not email:
        raise HTTPException(status_code=400, detail="no email")

    name = (payload.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="no name")

    bucket = _bucket()
    now = _now_iso()

    account_id = _account_id_for_uid(uid)
    account_path = f"accounts/{account_id}/account.json"
    user_path = f"users/{uid}/user.json"

    # 既存チェック（= 二重作成防止）
    account_blob = bucket.blob(account_path)
    if account_blob.exists():
        # 既存を返す（作成画面のリトライや二重クリックでも壊れない）
        existing = json.loads(account_blob.download_as_text(encoding="utf-8"))
        return {
            "account_id": account_id,
            "created": False,
            "account": existing,
        }

    # 1) アプリ内ユーザーをここで初めて作る（ログインだけでは作らない）
    user_blob = bucket.blob(user_path)
    if not user_blob.exists():
        user_blob.upload_from_string(
            json.dumps(
                {
                    "uid": uid,
                    "email": email,
                    "created_at": now,
                },
                ensure_ascii=False,
            ),
            content_type="application/json",
        )

    # 2) account 実体（1ユーザー=1件）
    account_blob.upload_from_string(
        json.dumps(
            {
                "account_id": account_id,
                "name": name,
                "owner_uid": uid,
                "owner_email": email,
                "created_at": now,
            },
            ensure_ascii=False,
        ),
        content_type="application/json",
    )

    return {
        "account_id": account_id,
        "created": True,
        "account": {
            "account_id": account_id,
            "name": name,
            "owner_uid": uid,
            "owner_email": email,
            "created_at": now,
        },
    }
