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
from app.deps.db import get_db
from app.core.settings import BUCKET_NAME
from app.services.contracts_acl import require_contract_admin

router = APIRouter()

MAX_SAMPLE_BYTES_DEFAULT = 2_000_000

class JudgeMethodIn(BaseModel):
    contract_id: str = Field(..., min_length=1)
    object_key: Optional[str] = None
    sample_bytes: int = Field(default=MAX_SAMPLE_BYTES_DEFAULT, ge=10_000, le=2_000_000)

class JudgeMethodOut(BaseModel):
    can_extract_qa: bool
    method: Optional[str] = None
    confidence: float = 0.0
    reasons: List[str] = []
    stats: Dict[str, Any] = {}

def _gcs_read_head_text(object_key: str, limit_bytes: int) -> str:
    if not BUCKET_NAME:
        raise HTTPException(status_code=500, detail="BUCKET_NAME is not set")

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(object_key)
    data = blob.download_as_bytes(start=0, end=limit_bytes - 1)
    return data.decode("utf-8", errors="replace")

def _judge_simple(text: str) -> JudgeMethodOut:
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if len(lines) < 10:
        return JudgeMethodOut(False, None, 0.0, ["内容が少なすぎる"], {})
    return JudgeMethodOut(True, "D", 0.6, ["文章量あり"], {"lines": len(lines)})

@router.post("/v1/admin/dialogues/judge-method", response_model=JudgeMethodOut)
def judge_method(
    req: JudgeMethodIn,
    user=Depends(require_user),
    conn=Depends(get_db),
):
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid")

    require_contract_admin(uid, req.contract_id, conn)

    if not req.object_key:
        raise HTTPException(status_code=400, detail="object_key required")

    text = _gcs_read_head_text(req.object_key, req.sample_bytes)
    return _judge_simple(text)
