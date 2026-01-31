import os
import re
import json
import csv
from io import StringIO
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from google.cloud import storage

from app.deps.auth import require_user
from app.core.settings import BUCKET_NAME

router = APIRouter()

MAX_SAMPLE_BYTES_DEFAULT = 2_000_000

# =====================
# Input / Output
# =====================
class JudgeMethodIn(BaseModel):
    # 正式には tenant_id を使う
    tenant_id: Optional[str] = None
    # 互換用（フロントがまだ contract_id を投げてくる場合）
    contract_id: Optional[str] = None

    object_key: Optional[str] = None
    sample_bytes: int = Field(
        default=MAX_SAMPLE_BYTES_DEFAULT,
        ge=10_000,
        le=2_000_000
    )

class JudgeMethodOut(BaseModel):
    can_extract_qa: bool
    method: Optional[str] = None
    confidence: float = 0.0
    reasons: List[str] = []
    stats: Dict[str, Any] = {}

# =====================
# Utils
# =====================
def _resolve_tenant_id(req: JudgeMethodIn) -> str:
    t = (req.tenant_id or "").strip()
    if t:
        return t
    c = (req.contract_id or "").strip()
    if c:
        # 互換：contract_id を tenant_id 扱い
        return c
    raise HTTPException(status_code=400, detail="tenant_id (or contract_id) required")

def _gcs_read_head_text(object_key: str, limit_bytes: int) -> str:
    if not BUCKET_NAME:
        raise HTTPException(status_code=500, detail="BUCKET_NAME is not set")

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(object_key)

    if not blob.exists():
        raise HTTPException(status_code=404, detail="object not found in GCS")

    data = blob.download_as_bytes(start=0, end=limit_bytes - 1)
    return data.decode("utf-8", errors="replace")

# =====================
# 判定ロジック（最小）
# ※ uploads.py の A-F 判定に将来統合予定
# =====================
def _judge_simple(text: str) -> JudgeMethodOut:
    lines = [ln for ln in text.splitlines() if ln.strip()]

    if len(lines) < 10:
        return JudgeMethodOut(
            can_extract_qa=False,
            method=None,
            confidence=0.0,
            reasons=["内容が少なすぎる"],
            stats={"lines": len(lines)},
        )

    return JudgeMethodOut(
        can_extract_qa=True,
        method="D",
        confidence=0.6,
        reasons=["文章量あり"],
        stats={"lines": len(lines)},
    )

# =====================
# API
# =====================
@router.post(
    "/v1/admin/dialogues/judge-method",
    response_model=JudgeMethodOut
)
def judge_method(
    req: JudgeMethodIn,
    user=Depends(require_user),
):
    """
    DBアクセス停止版:
    - tenant_id を解決（互換で contract_id も可）
    - ACL 判定は一旦行わない
    - GCS の object_key を読んで方式判定のみ行う
    """
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid")

    tenant_id = _resolve_tenant_id(req)

    if not req.object_key:
        raise HTTPException(status_code=400, detail="object_key required")

    # ※ tenant_id は将来 ACL 判定に使うため、ここでは未使用
    text = _gcs_read_head_text(req.object_key, req.sample_bytes)
    return _judge_simple(text)
