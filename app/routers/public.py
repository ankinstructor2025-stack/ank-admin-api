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
# GCS helpers
# =========================
def _bucket():
    if not BUCKET_NAME:
        raise HTTPException(status_code=500, detail="UPLOAD_BUCKET is not set")
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
    results: list[dict[str, Any]] = []

    for b in _storage.list_blobs(bucket, prefix=prefix):
        if not b.name.endswith(".json"):
            continue

        # users/<auth_key>/accounts/<account_id>.json
        account_id = b.name[len(prefix):].replace(".json", "").strip()
        if not account_id:
            continue

        try:
            data = _read_json(bucket, b.name)
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

    results.sort(key=lambda x: (x.get("account_id") or ""))
    return results


# =========================
# /v1/session (DBなし)
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

    auth_key = uid  # 将来 MS 対応するならここを provider付きにする

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
        "accounts": accounts,
    }


# =========================
# /v1/pricing (GCS settings)
# =========================
@router.get("/v1/pricing")
def pricing():
    """
    settings/pricing.json を読み込む。

    フロント互換のため、返却は以下の形に寄せる：
      {
        "seats": [{seat_limit, monthly_fee, label}, ...],
        "knowledge_count": [{value, monthly_price, label}, ...],
        "search_limit": {...},
        "poc": null
      }

    ※ pricing.json の schema が plans 配列でも、
       互換に変換して返す（まず動かす優先）。
    """
    bucket = _bucket()
    raw = _read_json(bucket, "settings/pricing.json")

    plans = raw.get("plans") or []
    if not isinstance(plans, list):
        plans = []

    seats = []
    knowledge_count = []

    # 互換変換：
    # - seats: plan.seat_limit を選択肢に
    # - monthly_fee: plan.monthly_price をそのまま入れる（暫定）
    # - knowledge_count: plan.knowledge_count を選択肢に
    #   monthly_price は 0（暫定）…UIは動く。金額ロジックは後で plan 方式に寄せるのが本筋。
    for p in plans:
        try:
            seat_limit = int(p.get("seat_limit"))
        except Exception:
            continue

        label = (p.get("label") or "").strip() or f"{seat_limit}人"
        monthly_price = p.get("monthly_price", None)
        # monthly_price は数値 or None を許容
        monthly_fee = None
        if monthly_price is not None:
            try:
                monthly_fee = int(monthly_price)
            except Exception:
                monthly_fee = None

        seats.append(
            {
                "seat_limit": seat_limit,
                "monthly_fee": monthly_fee,
                "label": label,
            }
        )

        # knowledge_count
        kc = p.get("knowledge_count", None)
        if kc is not None:
            try:
                v = int(kc)
                knowledge_count.append(
                    {
                        "value": v,
                        "monthly_price": 0,  # 暫定（プラン一本化へ寄せるなら後で消える）
                        "label": str(v),
                    }
                )
            except Exception:
                pass

    # 重複削除＆ソート
    seats_map = {}
    for s in seats:
        seats_map[s["seat_limit"]] = s
    seats = [seats_map[k] for k in sorted(seats_map.keys())]

    kc_map = {}
    for k in knowledge_count:
        kc_map[k["value"]] = k
    knowledge_count = [kc_map[k] for k in sorted(kc_map.keys())]

    return {
        "seats": seats,
        "knowledge_count": knowledge_count,
        "search_limit": {"per_user_per_day": 0, "note": ""},
        "poc": None,
    }


# =========================
# /v1/system (GCS settings)
# =========================
@router.get("/v1/system")
def system():
    bucket = _bucket()
    raw = _read_json(bucket, "settings/system.json")
    return raw
