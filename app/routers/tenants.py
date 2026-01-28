# app/routers/tenants.py
from __future__ import annotations

import json
import os
import sqlite3
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
# Common helpers
# =========================
def _bucket():
    if not BUCKET_NAME:
        raise HTTPException(status_code=500, detail="BUCKET_NAME is not set")
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
    settings/system.json の limits を読む（無ければ空）
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
    account の member 判定を入れるならここ。
    いまは最小運用で未チェック（常にOK）。
    将来:
      accounts/<account_id>/members/<uid>.json を見て弾く等。
    """
    return True


# =========================
# Pricing API (source of truth = GCS settings/pricing.json)
# =========================
@router.get("/v1/pricing")
def get_pricing(user=Depends(require_user)):
    """
    settings/pricing.json をそのまま返す（加工しない）

    重要:
    - seats / knowledge_count が空のまま返ると UI が詰むので、
      空なら 500 で止めて原因を明確にする。
    """
    bucket = _bucket()
    gcs_path = "settings/pricing.json"
    blob = bucket.blob(gcs_path)
    if not blob.exists():
        raise HTTPException(status_code=404, detail=f"{gcs_path} not found in bucket={bucket.name}")

    try:
        data = json.loads(blob.download_as_text(encoding="utf-8"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"pricing.json read error: {e}")

    if not isinstance(data, dict):
        raise HTTPException(status_code=500, detail="pricing.json must be an object")

    seats = data.get("seats") or []
    kc = data.get("knowledge_count") or []
    if not isinstance(seats, list) or not isinstance(kc, list) or (len(seats) == 0) or (len(kc) == 0):
        raise HTTPException(
            status_code=500,
            detail=f"pricing.json is empty (seats/knowledge_count). Please update gs://{bucket.name}/{gcs_path}",
        )

    return data


# =========================
# SQLite init (契約保存時に作る)
# =========================
_SQLITE_SCHEMA_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS qa (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  question TEXT NOT NULL,
  answer TEXT NOT NULL,
  source_key TEXT,
  created_at TEXT NOT NULL
);

INSERT OR IGNORE INTO meta(key, value) VALUES ('schema_version', '1');
"""


def _create_sqlite_file(local_path: str, *, tenant_id: str, account_id: str, role: str):
    """
    role: "write" or "read"
    """
    conn = sqlite3.connect(local_path)
    try:
        cur = conn.cursor()
        cur.executescript(_SQLITE_SCHEMA_SQL)
        cur.execute("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)", ("tenant_id", tenant_id))
        cur.execute("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)", ("account_id", account_id))
        cur.execute("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)", ("db_role", role))
        cur.execute("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)", ("created_at", _now_iso()))
        conn.commit()
    finally:
        conn.close()


def _ensure_tenant_sqlite_dbs(bucket, *, account_id: str, tenant_id: str):
    """
    契約保存時に、DBを「実体生成」してGCSに置く。
    - 既にサイズ>0 のDBがある場合は上書きしない（事故防止）
    - 無い / 0B の場合のみ生成してアップロード
    """
    base = f"accounts/{account_id}/tenants/{tenant_id}/db/"
    targets = [
        ("write.db", "write"),
        ("read.db", "read"),
    ]

    for filename, role in targets:
        gcs_path = base + filename
        blob = bucket.blob(gcs_path)

        exists = blob.exists()
        size = blob.size if exists else None

        # 既にちゃんとあるなら何もしない
        if exists and (size is not None) and size > 0:
            continue

        local_path = f"/tmp/{account_id}_{tenant_id}_{role}.db"
        _create_sqlite_file(local_path, tenant_id=tenant_id, account_id=account_id, role=role)
        blob.upload_from_filename(local_path, content_type="application/octet-stream")

        try:
            os.remove(local_path)
        except Exception:
            pass


# =========================
# Tenants APIs
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
                "payment_method_configured": bool(data.get("payment_method_configured")),
                "seat_limit": data.get("seat_limit"),
                "knowledge_count": data.get("knowledge_count"),
                "monthly_amount_yen": data.get("monthly_amount_yen"),
                "plan_id": data.get("plan_id"),
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
    - users/<uid>/tenants/<tenant_id>.json を索引として作る（任意）
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

    limits = _read_system_limits(bucket)
    max_tenants = int(limits.get("max_tenants_per_account") or 0)
    if max_tenants:
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
        "payment_method_configured": False,
        "seat_limit": None,
        "knowledge_count": None,
        "monthly_amount_yen": None,
        "plan_id": None,
        "note": None,
        "created_at": now,
        "updated_at": now,
    }

    _write_json(bucket, f"accounts/{account_id}/tenants/{tenant_id}/tenant.json", tenant)

    # 任意：ユーザー索引（account_id を引く用途）
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

    if account_id:
        _assert_account_member(bucket, account_id, uid)
        path = f"accounts/{account_id}/tenants/{tenant_id}/tenant.json"
        return _read_json(bucket, path)

    idx_path = f"users/{uid}/tenants/{tenant_id}.json"
    idx = _read_json(bucket, idx_path)
    aid = (idx.get("account_id") or "").strip()
    if not aid:
        raise HTTPException(status_code=500, detail="tenant index missing account_id")

    _assert_account_member(bucket, aid, uid)
    return _read_json(bucket, f"accounts/{aid}/tenants/{tenant_id}/tenant.json")


@router.post("/v1/tenant/contract")
def upsert_tenant_contract(
    payload: dict,
    user=Depends(require_user),
):
    """
    契約内容の保存
    - 支払い前(payment_method_configured=false)は何度でも更新可
    - 支払い後(payment_method_configured=true)は更新不可（400）
    - 保存時に SQLite(write/read) を生成（無い/0Bのみ。上書きしない）

    payload:
      account_id, tenant_id, seat_limit, knowledge_count, monthly_amount_yen, note?, plan_id?
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
    plan_id = (payload.get("plan_id") or "").strip() or None

    bucket = _bucket()
    _assert_account_member(bucket, account_id, uid)

    limits = _read_system_limits(bucket)
    max_seat = int(limits.get("max_seat_limit") or 0)
    max_kc = int(limits.get("max_knowledge_count") or 0)

    if max_seat and seat_limit > max_seat:
        raise HTTPException(status_code=400, detail="seat_limit exceeds system limit")
    if max_kc and knowledge_count > max_kc:
        raise HTTPException(status_code=400, detail="knowledge_count exceeds system limit")

    path = f"accounts/{account_id}/tenants/{tenant_id}/tenant.json"
    data = _read_json(bucket, path)

    # 支払い後は変更不可
    if bool(data.get("payment_method_configured")):
        raise HTTPException(status_code=400, detail="contract is locked (payment configured)")

    now = _now_iso()
    data["seat_limit"] = seat_limit
    data["knowledge_count"] = knowledge_count
    data["monthly_amount_yen"] = monthly_amount_yen
    data["note"] = note or None
    data["plan_id"] = plan_id
    data["updated_at"] = now

    if not data.get("contract_saved_at"):
        data["contract_saved_at"] = now

    _write_json(bucket, path, data)

    # 契約保存時にDBを作る（無い/0Bのみ）
    _ensure_tenant_sqlite_dbs(bucket, account_id=account_id, tenant_id=tenant_id)

    return {"ok": True}


@router.post("/v1/tenant/mark-paid")
def mark_paid(
    payload: dict,
    user=Depends(require_user),
):
    """
    支払い設定完了（仮）
    - tenant.json の payment_method_configured を true にするだけ
    - true になった瞬間、/v1/tenant/contract は変更不可になる
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

    if not data.get("paid_at"):
        data["paid_at"] = now

    _write_json(bucket, path, data)
    return {"ok": True}
