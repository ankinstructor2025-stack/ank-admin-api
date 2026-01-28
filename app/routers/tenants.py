# app/routers/tenants.py
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from google.cloud import storage

from app.core.settings import BUCKET_NAME
from app.deps.auth import require_user

router = APIRouter()
_storage = storage.Client()


# =========================
# Helpers
# =========================
def _bucket():
    if not BUCKET_NAME:
        raise HTTPException(status_code=500, detail="UPLOAD_BUCKET is not set")
    return _storage.bucket(BUCKET_NAME)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(bucket, path: str) -> dict:
    blob = bucket.blob(path)
    if not blob.exists():
        raise HTTPException(status_code=404, detail=f"not found: {path}")
    text = blob.download_as_text(encoding="utf-8")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail=f"invalid json: {path}")


def _write_json(bucket, path: str, data: dict):
    blob = bucket.blob(path)
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    blob.upload_from_string(payload, content_type="application/json; charset=utf-8")


def _read_system_limits(bucket) -> dict[str, int]:
    """
    settings/system.json の limits を読む
    例:
      {
        "limits": {
          "max_seat_limit": 50,
          "max_knowledge_count": 20000,
          "max_tenants_per_account": 10
        }
      }
    """
    blob = bucket.blob("settings/system.json")
    if not blob.exists():
        return {}
    try:
        data = json.loads(blob.download_as_text(encoding="utf-8"))
        limits = data.get("limits") or {}
        out: dict[str, int] = {}
        for k, v in limits.items():
            try:
                out[str(k)] = int(v)
            except Exception:
                pass
        return out
    except Exception:
        return {}


def _assert_account_member(bucket, account_id: str, uid: str):
    """
    account の member 判定を入れたい場合に使う想定。
    いまは最小運用のため、未実装（常にOK）にしても動くが、
    将来 accounts/<account_id>/members/<uid>.json を見て弾く形にできる。
    """
    # まずは最小：未チェック
    return True


# =========================
# API
# =========================
@router.get("/v1/tenants")
def list_tenants(
    account_id: str = Query(...),
    user=Depends(require_user),
):
    """
    accounts/<account_id>/tenants/<tenant_id>/tenant.json を列挙して一覧返す
    """
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid")

    bucket = _bucket()
    _assert_account_member(bucket, account_id, uid)

    prefix = f"accounts/{account_id}/tenants/"
    tenants: list[dict[str, Any]] = []

    # tenant.json だけ拾う
    for b in _storage.list_blobs(bucket, prefix=prefix):
        if not b.name.endswith("/tenant.json"):
            continue
        try:
            data = json.loads(b.download_as_text(encoding="utf-8"))
        except Exception:
            continue

        tenants.append(
            {
                "tenant_id": data.get("tenant_id"),
                "name": data.get("name") or "",
                "status": data.get("status") or "active",
                "contract_status": data.get("contract_status") or "draft",
                "payment_method_configured": bool(data.get("payment_method_configured")),
                "seat_limit": data.get("seat_limit"),
                "knowledge_count": data.get("knowledge_count"),
                "monthly_amount_yen": data.get("monthly_amount_yen"),
                "note": data.get("note"),
            }
        )

    tenants.sort(key=lambda x: (x.get("tenant_id") or ""))
    return {"tenants": tenants}


@router.post("/v1/tenant")
def create_tenant(
    payload: dict,
    user=Depends(require_user),
):
    """
    テナント（＝契約の器）作成
    - accounts/<account_id>/tenants/<tenant_id>/tenant.json を作る
    - users/<uid>/tenants/<tenant_id>.json を索引として作る（後で役に立つ）
    """
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid")

    account_id = (payload.get("account_id") or "").strip()
    if not account_id:
        raise HTTPException(status_code=400, detail="account_id required")

    name = (payload.get("name") or "").strip()

    bucket = _bucket()
    _assert_account_member(bucket, account_id, uid)

    # system.json の max_tenants_per_account を軽く効かせる（任意）
    limits = _read_system_limits(bucket)
    max_tenants = int(limits.get("max_tenants_per_account") or 0)
    if max_tenants:
        # ざっくり数える（prefix list）
        prefix = f"accounts/{account_id}/tenants/"
        count = 0
        for b in _storage.list_blobs(bucket, prefix=prefix):
            if b.name.endswith("/tenant.json"):
                count += 1
                if count >= max_tenants:
                    raise HTTPException(status_code=400, detail="tenant count limit reached")

    tenant_id = f"ten_{uuid.uuid4().hex[:12]}"
    now = _now_iso()

    tenant = {
        "tenant_id": tenant_id,
        "account_id": account_id,
        "name": name,
        "status": "active",
        # 契約は最初 draft（ここでプラン確定してから active にする）
        "contract_status": "draft",
        "payment_method_configured": False,
        "seat_limit": None,
        "knowledge_count": None,
        "monthly_amount_yen": None,
        "note": None,
        "created_at": now,
        "updated_at": now,
    }

    # tenant 実体
    _write_json(bucket, f"accounts/{account_id}/tenants/{tenant_id}/tenant.json", tenant)

    # user 側索引（任意だが、GET /v1/tenant で account_id 不明の時に助かる）
    user_index = {
        "tenant_id": tenant_id,
        "account_id": account_id,
        "role": "admin",
        "status": "active",
        "created_at": now,
    }
    _write_json(bucket, f"users/{uid}/tenants/{tenant_id}.json", user_index)

    return {"tenant_id": tenant_id}


@router.get("/v1/tenant")
def get_tenant(
    tenant_id: str = Query(...),
    account_id: str = Query(""),
    user=Depends(require_user),
):
    """
    テナント詳細を返す
    - account_id があれば accounts 側から直接読む
    - なければ users/<uid>/tenants/<tenant_id>.json から account_id を引く
    """
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid")

    bucket = _bucket()

    # 1) account_id 指定があればそれを優先
    if account_id:
        _assert_account_member(bucket, account_id, uid)
        path = f"accounts/{account_id}/tenants/{tenant_id}/tenant.json"
        data = _read_json(bucket, path)
        return data

    # 2) user索引から account_id を引く
    idx_path = f"users/{uid}/tenants/{tenant_id}.json"
    idx = _read_json(bucket, idx_path)
    aid = (idx.get("account_id") or "").strip()
    if not aid:
        raise HTTPException(status_code=500, detail="tenant index missing account_id")

    _assert_account_member(bucket, aid, uid)
    data = _read_json(bucket, f"accounts/{aid}/tenants/{tenant_id}/tenant.json")
    return data


@router.post("/v1/tenant/contract")
def upsert_tenant_contract(
    payload: dict,
    user=Depends(require_user),
):
    """
    契約内容の保存（テナント＝契約）
    - seat_limit / knowledge_count / monthly_amount_yen / note を tenant.json に保存
    - contract_status を draft → active にできる（確定）
    - system.json の limits で上限チェック
    """
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid")

    tenant_id = (payload.get("tenant_id") or "").strip()
    account_id = (payload.get("account_id") or "").strip()
    if not tenant_id:
        raise HTTPException(status_code=400, detail="tenant_id required")
    if not account_id:
        raise HTTPException(status_code=400, detail="account_id required")

    try:
        seat_limit = int(payload.get("seat_limit"))
        knowledge_count = int(payload.get("knowledge_count"))
    except Exception:
        raise HTTPException(status_code=400, detail="seat_limit/knowledge_count must be int")

    monthly_amount_yen = payload.get("monthly_amount_yen", None)
    if monthly_amount_yen is None:
        raise HTTPException(status_code=400, detail="monthly_amount_yen required")
    try:
        monthly_amount_yen = int(monthly_amount_yen)
    except Exception:
        raise HTTPException(status_code=400, detail="monthly_amount_yen must be int")

    note = (payload.get("note") or "").strip()
    contract_status = (payload.get("contract_status") or "draft").strip()
    if contract_status not in ("draft", "active"):
        raise HTTPException(status_code=400, detail="invalid contract_status")

    bucket = _bucket()
    _assert_account_member(bucket, account_id, uid)

    # ガードレール（system.json）
    limits = _read_system_limits(bucket)
    max_seat = int(limits.get("max_seat_limit") or 0)
    max_kc = int(limits.get("max_knowledge_count") or 0)

    if max_seat and seat_limit > max_seat:
        raise HTTPException(status_code=400, detail="seat_limit exceeds system limit")
    if max_kc and knowledge_count > max_kc:
        raise HTTPException(status_code=400, detail="knowledge_count exceeds system limit")

    path = f"accounts/{account_id}/tenants/{tenant_id}/tenant.json"
    data = _read_json(bucket, path)
    now = _now_iso()

    data["seat_limit"] = seat_limit
    data["knowledge_count"] = knowledge_count
    data["monthly_amount_yen"] = monthly_amount_yen
    data["note"] = note or None
    data["contract_status"] = contract_status
    data["updated_at"] = now

    if contract_status == "active" and not data.get("activated_at"):
        data["activated_at"] = now

    _write_json(bucket, path, data)
    return {"ok": True}


@router.post("/v1/tenant/mark-paid")
def mark_paid(
    payload: dict,
    user=Depends(require_user),
):
    """
    支払い設定完了（仮）
    - tenant.json の payment_method_configured を true にするだけ
    """
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid")

    tenant_id = (payload.get("tenant_id") or "").strip()
    account_id = (payload.get("account_id") or "").strip()
    if not tenant_id:
        raise HTTPException(status_code=400, detail="tenant_id required")
    if not account_id:
        raise HTTPException(status_code=400, detail="account_id required")

    bucket = _bucket()
    _assert_account_member(bucket, account_id, uid)

    path = f"accounts/{account_id}/tenants/{tenant_id}/tenant.json"
    data = _read_json(bucket, path)

    now = _now_iso()
    data["payment_method_configured"] = True
    data["updated_at"] = now

    _write_json(bucket, path, data)
    return {"ok": True}
