# app/routers/tenants.py
from __future__ import annotations

import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

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
        obj = json.loads(text)
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail=f"invalid json: {path}")
    if not isinstance(obj, dict):
        raise HTTPException(status_code=500, detail=f"json must be an object: {path}")
    return obj


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
    """
    return True


# =========================
# Plans / Pricing (GCS)
# =========================
def _read_plans(bucket) -> dict:
    # settings/plans.json（プラン定義）
    return _read_json(bucket, "settings/plans.json")


def _find_plan(plans_obj: dict, plan_id: str) -> Optional[dict]:
    plans = plans_obj.get("plans") or []
    if not isinstance(plans, list):
        return None
    for p in plans:
        if isinstance(p, dict) and (p.get("plan_id") or "") == plan_id:
            return p
    return None


def _plan_requires_db(plan: dict) -> bool:
    """
    plans.json の features.requires_db を見る。
    - 無い場合は True 扱い（安全側）
    """
    f = plan.get("features") or {}
    if not isinstance(f, dict):
        return True
    if "requires_db" not in f:
        return True
    return bool(f.get("requires_db"))


def _plan_monthly_price(plan: dict) -> int:
    v = plan.get("monthly_price", 0)
    try:
        return int(v)
    except Exception:
        raise HTTPException(status_code=500, detail="plans.json monthly_price must be int")


@router.get("/v1/plans")
def get_plans(user=Depends(require_user)):
    """
    settings/plans.json をそのまま返す（加工しない）
    """
    bucket = _bucket()
    gcs_path = "settings/plans.json"
    blob = bucket.blob(gcs_path)
    if not blob.exists():
        raise HTTPException(status_code=404, detail=f"{gcs_path} not found in bucket={bucket.name}")

    try:
        data = json.loads(blob.download_as_text(encoding="utf-8"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"plans.json read error: {e}")

    if not isinstance(data, dict):
        raise HTTPException(status_code=500, detail="plans.json must be an object")

    plans = data.get("plans") or []
    if not isinstance(plans, list) or len(plans) == 0:
        raise HTTPException(
            status_code=500,
            detail=f"plans.json is empty (plans). Please update gs://{bucket.name}/{gcs_path}",
        )

    return data


# 旧 pricing（seat/knowledge 前提）も残す
@router.get("/v1/pricing")
def get_pricing(user=Depends(require_user)):
    """
    settings/pricing.json をそのまま返す（加工しない）
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
# Contract helpers (GCS)
# =========================
def _contract_path(account_id: str, tenant_id: str) -> str:
    return f"accounts/{account_id}/tenants/{tenant_id}/contract.json"


def _read_contract(bucket, account_id: str, tenant_id: str) -> Optional[dict]:
    path = _contract_path(account_id, tenant_id)
    blob = bucket.blob(path)
    if not blob.exists():
        return None
    return _read_json(bucket, path)


def _write_contract(bucket, account_id: str, tenant_id: str, data: dict):
    path = _contract_path(account_id, tenant_id)
    _write_json(bucket, path, data)


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
                "contract_id": data.get("contract_id"),  # 追加（無ければNone）
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
        "contract_id": None,
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


# =========================
# NEW: Contract API (tenant + contract を作る/更新する)
# =========================
@router.post("/v1/contract")
def create_or_update_contract(
    payload: dict,
    user=Depends(require_user),
):
    """
    目的：
      - 「このプランで契約」押下で tenant と contract を 1:1 で作る/更新する

    入力：
      account_id, plan_id, note?
      tenant_id は任意（無ければ新規作成）
      monthly_amount_yen は plans.json の monthly_price を使う（payloadは信用しない）

    出力：
      { tenant_id, contract_id }
    """
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid")

    account_id = (payload.get("account_id") or "").strip()
    if not account_id:
        raise HTTPException(status_code=400, detail="account_id required")

    plan_id = (payload.get("plan_id") or "").strip()
    if not plan_id:
        raise HTTPException(status_code=400, detail="plan_id required")

    note = (payload.get("note") or "").strip() or None
    tenant_id = (payload.get("tenant_id") or "").strip() or None

    bucket = _bucket()
    _assert_account_member(bucket, account_id, uid)

    # plan 検証 & 月額はplans.json由来
    plans_obj = _read_plans(bucket)
    plan = _find_plan(plans_obj, plan_id)
    if not plan:
        raise HTTPException(status_code=400, detail=f"unknown plan_id: {plan_id}")
    monthly_amount_yen = _plan_monthly_price(plan)

    limits = _read_system_limits(bucket)
    max_tenants = int(limits.get("max_tenants_per_account") or 0)

    now = _now_iso()

    # tenant_id が無ければ tenant を新規作成
    if not tenant_id:
        if max_tenants:
            prefix = f"accounts/{account_id}/tenants/"
            count = 0
            for b in _storage.list_blobs(bucket, prefix=prefix):
                if b.name.endswith("/tenant.json"):
                    count += 1
                    if count >= max_tenants:
                        raise HTTPException(status_code=400, detail="tenant count limit reached")

        tenant_id = f"ten_{uuid.uuid4().hex[:12]}"
        tenant = {
            "tenant_id": tenant_id,
            "account_id": account_id,
            "name": "",
            "status": "active",
            "payment_method_configured": False,
            "seat_limit": None,
            "knowledge_count": None,
            "monthly_amount_yen": monthly_amount_yen,
            "plan_id": plan_id,
            "note": note,
            "contract_id": None,  # 後で入れる
            "created_at": now,
            "updated_at": now,
            "contract_saved_at": now,
        }
        _write_json(bucket, f"accounts/{account_id}/tenants/{tenant_id}/tenant.json", tenant)

        user_index = {
            "tenant_id": tenant_id,
            "account_id": account_id,
            "role": "admin",
            "status": "active",
            "created_at": now,
        }
        _write_json(bucket, f"users/{uid}/tenants/{tenant_id}.json", user_index)

    # 既存tenantを読み込み
    tenant_path = f"accounts/{account_id}/tenants/{tenant_id}/tenant.json"
    tenant = _read_json(bucket, tenant_path)

    # 支払い後は契約変更不可（「作成」だけは初回なのでここに来る前にtenantが無い）
    if bool(tenant.get("payment_method_configured")):
        raise HTTPException(status_code=400, detail="contract is locked (payment configured)")

    # contract.json（1:1）を作成/更新
    contract = _read_contract(bucket, account_id, tenant_id)
    if not contract:
        contract_id = f"con_{uuid.uuid4().hex[:12]}"
        contract = {
            "contract_id": contract_id,
            "tenant_id": tenant_id,
            "account_id": account_id,
            "status": "active",
            "plan_id": plan_id,
            "monthly_amount_yen": monthly_amount_yen,
            "note": note,
            "created_at": now,
            "updated_at": now,
        }
    else:
        contract_id = (contract.get("contract_id") or "").strip() or f"con_{uuid.uuid4().hex[:12]}"
        contract["contract_id"] = contract_id
        contract["status"] = "active"
        contract["plan_id"] = plan_id
        contract["monthly_amount_yen"] = monthly_amount_yen
        contract["note"] = note
        contract["updated_at"] = now

    _write_contract(bucket, account_id, tenant_id, contract)

    # tenant.json にも反映（UI表示用/検索用）
    tenant["plan_id"] = plan_id
    tenant["monthly_amount_yen"] = monthly_amount_yen
    tenant["note"] = note
    tenant["contract_id"] = contract_id
    tenant["updated_at"] = now
    if not tenant.get("contract_saved_at"):
        tenant["contract_saved_at"] = now
    _write_json(bucket, tenant_path, tenant)

    # requires_db=true のときだけDB生成
    if _plan_requires_db(plan):
        _ensure_tenant_sqlite_dbs(bucket, account_id=account_id, tenant_id=tenant_id)

    return {"tenant_id": tenant_id, "contract_id": contract_id}


# =========================
# Backward compatible: /v1/tenant/contract (tenant更新＋必要ならDB生成)
# =========================
@router.post("/v1/tenant/contract")
def upsert_tenant_contract(
    payload: dict,
    user=Depends(require_user),
):
    """
    互換用：tenant_id が既にある前提で、契約情報を更新する。

    新方式：
      account_id, tenant_id, plan_id, note?
      monthly_amount_yen は plans.json の monthly_price を使う（payloadは信用しない）

    旧方式（plan_id無し）：
      account_id, tenant_id, seat_limit, knowledge_count, monthly_amount_yen, note?
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

    note = (payload.get("note") or "").strip() or None
    plan_id = (payload.get("plan_id") or "").strip() or None

    bucket = _bucket()
    _assert_account_member(bucket, account_id, uid)

    tenant_path = f"accounts/{account_id}/tenants/{tenant_id}/tenant.json"
    tenant = _read_json(bucket, tenant_path)

    if bool(tenant.get("payment_method_configured")):
        raise HTTPException(status_code=400, detail="contract is locked (payment configured)")

    limits = _read_system_limits(bucket)
    max_seat = int(limits.get("max_seat_limit") or 0)
    max_kc = int(limits.get("max_knowledge_count") or 0)

    now = _now_iso()

    # -------------------------
    # 新方式：plan_id 優先
    # -------------------------
    if plan_id:
        plans_obj = _read_plans(bucket)
        plan = _find_plan(plans_obj, plan_id)
        if not plan:
            raise HTTPException(status_code=400, detail=f"unknown plan_id: {plan_id}")

        monthly_amount_yen = _plan_monthly_price(plan)

        seat_limit: Optional[int] = None
        knowledge_count: Optional[int] = None

        if payload.get("seat_limit") is not None:
            try:
                seat_limit = int(payload.get("seat_limit"))
            except Exception:
                raise HTTPException(status_code=400, detail="seat_limit must be int")
        if payload.get("knowledge_count") is not None:
            try:
                knowledge_count = int(payload.get("knowledge_count"))
            except Exception:
                raise HTTPException(status_code=400, detail="knowledge_count must be int")

        if seat_limit is not None and max_seat and seat_limit > max_seat:
            raise HTTPException(status_code=400, detail="seat_limit exceeds system limit")
        if knowledge_count is not None and max_kc and knowledge_count > max_kc:
            raise HTTPException(status_code=400, detail="knowledge_count exceeds system limit")

        # contract.json も更新（1:1）
        contract = _read_contract(bucket, account_id, tenant_id)
        if not contract:
            contract_id = f"con_{uuid.uuid4().hex[:12]}"
            contract = {
                "contract_id": contract_id,
                "tenant_id": tenant_id,
                "account_id": account_id,
                "status": "active",
                "plan_id": plan_id,
                "monthly_amount_yen": monthly_amount_yen,
                "note": note,
                "created_at": now,
                "updated_at": now,
            }
        else:
            contract_id = (contract.get("contract_id") or "").strip() or f"con_{uuid.uuid4().hex[:12]}"
            contract["contract_id"] = contract_id
            contract["status"] = "active"
            contract["plan_id"] = plan_id
            contract["monthly_amount_yen"] = monthly_amount_yen
            contract["note"] = note
            contract["updated_at"] = now

        _write_contract(bucket, account_id, tenant_id, contract)

        tenant["plan_id"] = plan_id
        tenant["monthly_amount_yen"] = monthly_amount_yen
        tenant["note"] = note
        tenant["contract_id"] = contract_id
        tenant["updated_at"] = now
        if seat_limit is not None:
            tenant["seat_limit"] = seat_limit
        if knowledge_count is not None:
            tenant["knowledge_count"] = knowledge_count
        if not tenant.get("contract_saved_at"):
            tenant["contract_saved_at"] = now

        _write_json(bucket, tenant_path, tenant)

        if _plan_requires_db(plan):
            _ensure_tenant_sqlite_dbs(bucket, account_id=account_id, tenant_id=tenant_id)

        return {"ok": True}

    # -------------------------
    # 旧互換：plan_id が無い場合
    # -------------------------
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

    if max_seat and seat_limit > max_seat:
        raise HTTPException(status_code=400, detail="seat_limit exceeds system limit")
    if max_kc and knowledge_count > max_kc:
        raise HTTPException(status_code=400, detail="knowledge_count exceeds system limit")

    # contract.json は旧方式だとplan_id無しなので、tenantの反映だけ（必要なら後で統一）
    tenant["seat_limit"] = seat_limit
    tenant["knowledge_count"] = knowledge_count
    tenant["monthly_amount_yen"] = monthly_amount_yen
    tenant["note"] = note
    tenant["plan_id"] = None
    tenant["updated_at"] = now
    if not tenant.get("contract_saved_at"):
        tenant["contract_saved_at"] = now

    _write_json(bucket, tenant_path, tenant)

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


@router.get("/v1/my/tenant")
def get_my_single_tenant(
    account_id: str = Query(...),
    user=Depends(require_user),
):
    """
    単一テナント運用向け：
    users/<uid>/tenants/*.json（逆引き）から、指定account_idのtenantを1件返す。
    複数あっても「最初の1件だけ」返す（複数対応は後回し）。

    return:
      { exists: bool, account_id, tenant_id?, plan_id? }
    """
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid")

    account_id = (account_id or "").strip()
    if not account_id:
        raise HTTPException(status_code=400, detail="account_id required")

    bucket = _bucket()

    prefix = f"users/{uid}/tenants/"
    found_tenant_id = None

    for b in _storage.list_blobs(bucket, prefix=prefix):
        if not b.name.endswith(".json"):
            continue

        try:
            idx = json.loads(b.download_as_text(encoding="utf-8"))
        except Exception:
            continue

        if not isinstance(idx, dict):
            continue

        aid = (idx.get("account_id") or "").strip()
        tid = (idx.get("tenant_id") or "").strip()

        if aid == account_id and tid:
            found_tenant_id = tid
            break

    if not found_tenant_id:
        return {"exists": False, "account_id": account_id}

    tenant_path = f"accounts/{account_id}/tenants/{found_tenant_id}/tenant.json"
    t = _read_json(bucket, tenant_path)
    plan_id = (t.get("plan_id") or "").strip() or None

    return {
        "exists": True,
        "account_id": account_id,
        "tenant_id": found_tenant_id,
        "plan_id": plan_id,
    }
