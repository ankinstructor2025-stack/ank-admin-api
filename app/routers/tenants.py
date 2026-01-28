import json
from datetime import datetime, timezone
from fastapi import HTTPException, Depends
from app.deps.auth import require_user

def _now_iso():
    return datetime.now(timezone.utc).isoformat()

@router.post("/v1/tenant/contract")
def upsert_tenant_contract(payload: dict, user=Depends(require_user)):
    uid = user.get("uid")
    if not uid:
        raise HTTPException(status_code=400, detail="no uid")

    tenant_id = (payload.get("tenant_id") or "").strip()
    account_id = (payload.get("account_id") or "").strip()
    if not tenant_id:
        raise HTTPException(status_code=400, detail="tenant_id required")
    if not account_id:
        raise HTTPException(status_code=400, detail="account_id required")

    seat_limit = int(payload.get("seat_limit") or 0)
    knowledge_count = int(payload.get("knowledge_count") or 0)
    monthly_amount_yen = payload.get("monthly_amount_yen")
    note = (payload.get("note") or "").strip()
    contract_status = (payload.get("contract_status") or "draft").strip()

    if monthly_amount_yen is None:
        raise HTTPException(status_code=400, detail="monthly_amount_yen required")

    bucket = _bucket()
    path = f"accounts/{account_id}/tenants/{tenant_id}/tenant.json"
    blob = bucket.blob(path)
    if not blob.exists():
        raise HTTPException(status_code=404, detail="tenant not found")

    data = json.loads(blob.download_as_text(encoding="utf-8"))
    now = _now_iso()

    data["seat_limit"] = seat_limit
    data["knowledge_count"] = knowledge_count
    data["monthly_amount_yen"] = int(monthly_amount_yen)
    data["note"] = note or None
    data["contract_status"] = contract_status
    data["updated_at"] = now
    if contract_status == "active" and not data.get("activated_at"):
        data["activated_at"] = now

    blob.upload_from_string(
        json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        content_type="application/json; charset=utf-8"
    )

    return {"ok": True}

@router.post("/v1/tenant/mark-paid")
def mark_paid(payload: dict, user=Depends(require_user)):
    uid = user.get("uid")
    if not uid:
        raise HTTPException(status_code=400, detail="no uid")

    tenant_id = (payload.get("tenant_id") or "").strip()
    account_id = (payload.get("account_id") or "").strip()
    if not tenant_id:
        raise HTTPException(status_code=400, detail="tenant_id required")
    if not account_id:
        raise HTTPException(status_code=400, detail="account_id required")

    bucket = _bucket()
    path = f"accounts/{account_id}/tenants/{tenant_id}/tenant.json"
    blob = bucket.blob(path)
    if not blob.exists():
        raise HTTPException(status_code=404, detail="tenant not found")

    data = json.loads(blob.download_as_text(encoding="utf-8"))
    now = _now_iso()

    data["payment_method_configured"] = True
    data["updated_at"] = now

    blob.upload_from_string(
        json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        content_type="application/json; charset=utf-8"
    )

    return {"ok": True}
