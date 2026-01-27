import os
import json
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.deps.auth import require_user
from google.cloud import storage

from app.core.settings import BUCKET_NAME

router = APIRouter()

_storage = storage.Client()


class ContractUpdateIn(BaseModel):
    contract_id: str
    seat_limit: int
    knowledge_count: int
    monthly_amount_yen: int
    note: str | None = None


class ContractIdIn(BaseModel):
    contract_id: str


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _bucket():
    if not BUCKET_NAME:
        raise HTTPException(status_code=500, detail="BUCKET_NAME is not set")
    return _storage.bucket(BUCKET_NAME)


def _read_json_with_generation(bucket, path: str):
    blob = bucket.blob(path)
    if not blob.exists():
        raise HTTPException(status_code=404, detail="not found")
    text = blob.download_as_text(encoding="utf-8")
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="invalid json")
    # generation は GCS のオブジェクト世代（楽観ロックに使う）
    blob.reload()
    return data, blob.generation


def _write_json_if_generation_matches(bucket, path: str, data: dict, generation: int):
    blob = bucket.blob(path)
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    blob.upload_from_string(
        payload,
        content_type="application/json; charset=utf-8",
        if_generation_match=generation,
    )
    return blob


def _require_contract_admin(bucket, contract_id: str, uid: str):
    member_path = f"tenants/{contract_id}/members/{uid}.json"
    member_blob = bucket.blob(member_path)
    if not member_blob.exists():
        raise HTTPException(status_code=403, detail="not a member")
    member = json.loads(member_blob.download_as_text(encoding="utf-8"))
    if (member.get("status") or "") != "active":
        raise HTTPException(status_code=403, detail="inactive member")
    role = (member.get("role") or "").strip()
    if role not in ("owner", "admin"):
        raise HTTPException(status_code=403, detail="not an admin")
    return member


@router.post("/v1/contracts/update")
def update_contract(payload: ContractUpdateIn, user=Depends(require_user)):
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=401, detail="unauthorized")

    contract_id = (payload.contract_id or "").strip()
    if not contract_id:
        raise HTTPException(status_code=400, detail="contract_id is required")

    bucket = _bucket()
    _require_contract_admin(bucket, contract_id, uid)

    contract_path = f"tenants/{contract_id}/contract.json"
    contract, gen = _read_json_with_generation(bucket, contract_path)

    now = _now_iso()
    contract["seat_limit"] = int(payload.seat_limit)
    contract["knowledge_count"] = int(payload.knowledge_count)
    contract["monthly_amount_yen"] = int(payload.monthly_amount_yen)
    contract["note"] = (payload.note or "").strip() or None
    contract["updated_at"] = now

    # 楽観ロック（別タブ更新などの衝突を検知）
    try:
        _write_json_if_generation_matches(bucket, contract_path, contract, gen)
    except Exception:
        # generation不一致など
        raise HTTPException(status_code=409, detail="conflict")

    return {"ok": True}


@router.post("/v1/contracts/mark-paid")
def mark_paid(payload: ContractIdIn, user=Depends(require_user)):
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=401, detail="unauthorized")

    contract_id = (payload.contract_id or "").strip()
    if not contract_id:
        raise HTTPException(status_code=400, detail="contract_id is required")

    bucket = _bucket()
    _require_contract_admin(bucket, contract_id, uid)

    contract_path = f"tenants/{contract_id}/contract.json"
    contract, gen = _read_json_with_generation(bucket, contract_path)

    now = _now_iso()
    contract["payment_method_configured"] = True
    contract["start_at"] = contract.get("start_at") or now
    contract["updated_at"] = now

    try:
        _write_json_if_generation_matches(bucket, contract_path, contract, gen)
    except Exception:
        raise HTTPException(status_code=409, detail="conflict")

    return {"ok": True}
