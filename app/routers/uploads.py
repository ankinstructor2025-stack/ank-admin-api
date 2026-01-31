# app/routers/uploads.py
# 方針（DBアクセス停止版）：
# 1) upload-url：署名URLを返すだけ（DB参照/書き込みなし）
# 2) upload-finalize：GCSの内容をサンプル読取→QA化可否/方式判定
#    - NG：GCS削除 + エラーメッセージ
#    - OK：upload_logs 相当を GCS に JSON 保存（DBへINSERTしない）
#
# 補足：
# - contract_id は当面互換のため受け取るが、内部は tenant_id として扱えるようにする
# - テナント別管理：object_key は tenants/{tenant_id}/uploads/... に寄せる

import os
import re
import uuid
import json
import csv
from io import StringIO
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any, Tuple

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from google.cloud import storage
from google.oauth2 import service_account

from app.core import settings as app_settings  # BUCKET_NAME/UPLOAD_BUCKET 等

router = APIRouter()

# -------------------------
# 設定
# -------------------------
ALLOWED_EXTS = {".txt", ".json", ".csv"}
MAX_BYTES = 100 * 1024 * 1024  # 100MB
MIN_BYTES = 1

# 署名URL期限
SIGNED_URL_EXPIRES_MIN = 15

# GCS上の保存先
# tenants/{tenant_id}/uploads/{YYYY-MM}/{upload_id}_{safe_filename}
# tenants/{tenant_id}/upload_logs/{YYYY-MM}/{upload_id}.json
def _object_key_upload(tenant_id: str, month_key: str, upload_id: str, safe_filename: str) -> str:
    return f"tenants/{tenant_id}/uploads/{month_key}/{upload_id}_{safe_filename}"

def _object_key_upload_log(tenant_id: str, month_key: str, upload_id: str) -> str:
    return f"tenants/{tenant_id}/upload_logs/{month_key}/{upload_id}.json"

# -------------------------
# Util
# -------------------------
def _get_bucket_name() -> str:
    name = (getattr(app_settings, "BUCKET_NAME", "") or os.environ.get("BUCKET_NAME") or "").strip()
    if name:
        return name
    name = (getattr(app_settings, "UPLOAD_BUCKET", "") or os.environ.get("UPLOAD_BUCKET") or "").strip()
    if name:
        return name
    raise HTTPException(status_code=500, detail="BUCKET_NAME (or UPLOAD_BUCKET) is not set")

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _month_key_jst() -> str:
    # 厳密JSTが必要なら後で差し替え。ひとまず UTC を月キーにする。
    now = datetime.utcnow()
    return f"{now.year:04d}-{now.month:02d}"

def _ext_lower(filename: str) -> str:
    name = (filename or "").strip().lower()
    i = name.rfind(".")
    return name[i:] if i >= 0 else ""

def _validate_file_meta(filename: str, size_bytes: int):
    ext = _ext_lower(filename)
    if ext not in ALLOWED_EXTS:
        raise HTTPException(status_code=400, detail="許可されていないファイル形式です（.txt / .json / .csv）")
    if size_bytes > MAX_BYTES:
        raise HTTPException(status_code=400, detail="ファイルサイズが大きすぎます（上限100MB）")
    if size_bytes < MIN_BYTES:
        raise HTTPException(status_code=400, detail="ファイルサイズが小さすぎます")

def _safe_name(filename: str) -> str:
    s = (filename or "file").strip()
    s = s.replace("/", "_").replace("\\", "_").replace("..", "_")
    s = re.sub(r"[^\w\.\-\(\)\[\]ぁ-んァ-ン一-龥]+", "_", s)
    return s[:120] if len(s) > 120 else s

def _resolve_signer_file(path: str) -> Optional[str]:
    """
    Secret のマウントが
      - /secrets/ank-gcs-signer (ファイル)
      - /secrets/ank-gcs-signer (ディレクトリ配下にファイル)
    のどちらでも拾えるようにする。
    """
    if not path:
        return None

    if os.path.isfile(path):
        return path

    if os.path.isdir(path):
        try:
            entries = sorted(os.listdir(path))
        except Exception:
            entries = []
        cand = []
        for e in entries:
            p = os.path.join(path, e)
            if os.path.isfile(p):
                cand.append(p)
        for p in cand:
            if p.lower().endswith(".json"):
                return p
        if len(cand) == 1:
            return cand[0]
        for p in cand:
            base = os.path.basename(p).lower()
            if base in ("latest", "key", "credentials", "service_account.json", "ank-gcs-signer"):
                return p
        if cand:
            return cand[0]
    return None

def _signer_credentials_from_env_or_secret() -> service_account.Credentials:
    """
    署名URL(v4 PUT)生成のためのサービスアカウント鍵を読む。
    GOOGLE_APPLICATION_CREDENTIALS が dir/file どちらでもOK。
    無ければ /secrets/ank-gcs-signer を dir/file どちらでもOK。
    """
    raw = (os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or "").strip() or "/secrets/ank-gcs-signer"
    signer_file = _resolve_signer_file(raw)

    if not signer_file:
        detail = f"signer file not found: {raw}"
        if os.path.isdir(raw):
            try:
                detail += f" (dir entries={os.listdir(raw)})"
            except Exception:
                pass
        raise HTTPException(status_code=500, detail=detail)

    try:
        return service_account.Credentials.from_service_account_file(signer_file)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"failed to load signer credentials: {signer_file}: {e}")

def _gcs_client_with_signer() -> storage.Client:
    cred = _signer_credentials_from_env_or_secret()
    return storage.Client(credentials=cred)

def _gcs_read_head_text(object_key: str, max_bytes: int = 200_000) -> str:
    bucket_name = _get_bucket_name()
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_key)
    if not blob.exists():
        raise HTTPException(status_code=400, detail="uploaded object not found in GCS")
    data = blob.download_as_bytes(end=max_bytes - 1)
    try:
        return data.decode("utf-8", errors="replace")
    except Exception:
        return data.decode(errors="replace")

def _gcs_delete(object_key: str):
    bucket_name = _get_bucket_name()
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_key)
    try:
        blob.delete()
    except Exception:
        # 削除失敗は握る（ユーザにはNGメッセージを返したい）
        pass

def _gcs_write_json(object_key: str, data: dict):
    bucket_name = _get_bucket_name()
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_key)
    blob.upload_from_string(
        json.dumps(data, ensure_ascii=False),
        content_type="application/json; charset=utf-8",
    )

# -------------------------
# 判定ロジック（A-F）
# -------------------------
class JudgeResult(BaseModel):
    ok: bool
    qa_mode: Optional[str] = None   # "A"..."F"
    confidence: float = 0.0
    reasons: List[str] = []
    stats: Dict[str, Any] = {}

def _looks_like_json(text: str) -> bool:
    s = text.lstrip()
    return s.startswith("{") or s.startswith("[")

def _try_parse_json(text: str) -> Optional[dict]:
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            if isinstance(obj.get("messages"), list):
                return {"kind": "messages"}
        if isinstance(obj, list):
            if obj and isinstance(obj[0], dict) and ("role" in obj[0] or "content" in obj[0]):
                return {"kind": "role_list"}
        return {"kind": "json"}
    except Exception:
        return None

def _try_parse_csv(text: str) -> Optional[dict]:
    try:
        reader = csv.reader(StringIO(text))
        rows = list(reader)
        if not rows:
            return None
        header = rows[0]
        header_l = [str(h or "").strip().lower() for h in header]
        return {"rows": len(rows), "cols": len(header), "header": header_l}
    except Exception:
        return None

def _looks_like_speaker_dialogue(lines: List[str]) -> int:
    rg = re.compile(r"^[^:：]{1,20}[:：]\s*\S+")
    return sum(1 for ln in lines[:200] if rg.search(ln))

def _looks_like_qa_style(lines: List[str]) -> int:
    rg = re.compile(r"^(Q[:：]|A[:：]|質問[:：]|回答[:：])\s*\S+", re.IGNORECASE)
    return sum(1 for ln in lines[:200] if rg.search(ln))

def _looks_like_ticket_mail(text: str, lines: List[str]) -> bool:
    head = "\n".join(lines[:80]).lower()
    if "subject:" in head or "from:" in head or "to:" in head or "cc:" in head or "date:" in head:
        return True
    if "-----original message-----" in head or "返信:" in head or "転送:" in head:
        return True
    if any(ln.startswith(">") for ln in lines[:200]):
        return True
    if re.search(r"\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b", "\n".join(lines[:200])):
        return True
    return False

def _detect_mode_A_to_F(filename: str, content_type: str, text: str) -> Tuple[bool, Optional[str], float, List[str], Dict[str, Any]]:
    ext = _ext_lower(filename)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    stats: Dict[str, Any] = {
        "ext": ext,
        "lines": len(lines),
        "chars": len(text),
        "content_type": content_type,
    }

    if len(text.strip()) < 50:
        return False, None, 0.0, ["内容が短すぎます"], stats

    # B: JSON
    if ext == ".json" or _looks_like_json(text):
        j = _try_parse_json(text)
        if j:
            stats["json_kind"] = j.get("kind")
            return True, "B", 0.90, ["JSON（messages/role）として判定"], stats

    # C: CSV
    if ext == ".csv":
        c = _try_parse_csv(text)
        if c:
            stats.update({"csv_rows": c.get("rows"), "csv_cols": c.get("cols"), "csv_header": c.get("header")})
            return True, "C", 0.80, ["CSVとして判定"], stats

    # E: QA形式
    qa_hits = _looks_like_qa_style(lines)
    if qa_hits >= 2:
        stats["qa_hits"] = qa_hits
        return True, "E", 0.80, ["QA形式として判定"], stats

    # F: ログ/チケット/メール
    if _looks_like_ticket_mail(text, lines):
        return True, "F", 0.75, ["ログ/チケット/メールスレとして判定"], stats

    # A: 話者ラベル対話
    sp_hits = _looks_like_speaker_dialogue(lines)
    if sp_hits >= 2:
        stats["speaker_hits"] = sp_hits
        return True, "A", 0.75, ["話者ラベル付き対話として判定"], stats

    # D: 単一文書
    if len(lines) >= 5:
        return True, "D", 0.60, ["単一文書として判定"], stats

    return False, None, 0.0, ["形式が判定できません（特徴が弱い）"], stats

def judge_qa_mode(filename: str, content_type: str, text: str) -> JudgeResult:
    ok, mode, conf, reasons, stats = _detect_mode_A_to_F(filename, content_type, text)
    return JudgeResult(ok=ok, qa_mode=mode, confidence=conf, reasons=reasons, stats=stats)

# -------------------------
# API
# -------------------------
class UploadUrlRequest(BaseModel):
    # 互換のため contract_id を残す（内部では tenant_id として扱える）
    tenant_id: Optional[str] = Field(default=None)
    contract_id: Optional[str] = Field(default=None)

    kind: str = Field(default="dialogue")
    filename: str = Field(..., min_length=1)
    content_type: str = Field(default="application/octet-stream")
    size_bytes: int = Field(default=0, ge=0)
    note: str = Field(default="")

def _resolve_tenant_id(req: UploadUrlRequest) -> str:
    t = (req.tenant_id or "").strip()
    if t:
        return t
    # 互換：contract_id を tenant_id 扱い
    c = (req.contract_id or "").strip()
    if c:
        return c
    raise HTTPException(status_code=400, detail="tenant_id (or contract_id) is required")

@router.post("/v1/admin/upload-url")
def create_upload_url(req: UploadUrlRequest):
    """
    DBアクセス停止版：
      - 認可は一旦ここでは行わない（将来: user.json で tenant role を確認）
      - ファイルメタ検証 → object_key 作成 → 署名URL返却
    """
    tenant_id = _resolve_tenant_id(req)
    _validate_file_meta(req.filename, int(req.size_bytes or 0))

    mk = _month_key_jst()
    upload_id = str(uuid.uuid4())
    safe = _safe_name(req.filename)

    object_key = _object_key_upload(tenant_id, mk, upload_id, safe)

    bucket_name = _get_bucket_name()
    client = _gcs_client_with_signer()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_key)

    url = blob.generate_signed_url(
        version="v4",
        expiration=timedelta(minutes=SIGNED_URL_EXPIRES_MIN),
        method="PUT",
        content_type=req.content_type or "application/octet-stream",
    )

    return {
        "upload_id": upload_id,
        "object_key": object_key,
        "upload_url": url,
        "month_key": mk,
        "tenant_id": tenant_id,
        "kind": (req.kind or "dialogue").strip() or "dialogue",
    }

@router.post("/v1/admin/upload-finalize")
def upload_finalize(payload: dict):
    """
    DBアクセス停止版：
      - object_key を先頭サンプル読み込みして方式判定
      - NG：GCS削除
      - OK：upload_logs 相当を GCS JSON として保存
    """
    # 入力
    tenant_id = (payload.get("tenant_id") or "").strip()
    contract_id = (payload.get("contract_id") or "").strip()  # 互換
    if not tenant_id:
        tenant_id = contract_id

    object_key = (payload.get("object_key") or "").strip()
    upload_id = (payload.get("upload_id") or "").strip()
    filename = (payload.get("filename") or "").strip()
    content_type = (payload.get("content_type") or "").strip()
    kind = (payload.get("kind") or "dialogue").strip() or "dialogue"

    if not tenant_id or not object_key or not upload_id:
        raise HTTPException(status_code=400, detail="tenant_id (or contract_id), object_key, upload_id are required")

    # 判定用に先頭を読む
    text = _gcs_read_head_text(object_key)
    judge = judge_qa_mode(filename or object_key, content_type, text)

    if not judge.ok:
        _gcs_delete(object_key)
        return {
            "ok": False,
            "message": "QA化できない形式です",
            "reasons": judge.reasons,
            "stats": judge.stats,
        }

    # OK：upload_logs を GCS に JSON で保存
    mk = _month_key_jst()
    log_key = _object_key_upload_log(tenant_id, mk, upload_id)

    log_doc = {
        "upload_id": upload_id,
        "tenant_id": tenant_id,
        "kind": kind,
        "object_key": object_key,
        "month_key": mk,
        "created_at": _now_iso(),
        "filename": filename or "",
        "content_type": content_type or "",
        "judge": {
            "ok": True,
            "qa_mode": judge.qa_mode,            # "A".."F"
            "confidence": float(judge.confidence or 0.0),
            "reasons": judge.reasons or [],
            "stats": judge.stats or {},
        },
        "note": (payload.get("note") or "").strip(),
    }

    try:
        _gcs_write_json(log_key, log_doc)
    except Exception as e:
        # ログ保存に失敗しても、アップロードデータは残す（原因調査用）
        raise HTTPException(status_code=500, detail=f"failed to write upload log to GCS: {e}")

    # UI互換のレスポンス（従来と同じキーをなるべく維持）
    return {
        "ok": True,
        "upload_id": upload_id,
        "tenant_id": tenant_id,
        "contract_id": tenant_id,  # 互換：フロントが contract_id を見ていても破綻しにくい
        "object_key": object_key,
        "qa_mode": judge.qa_mode,
        "confidence": judge.confidence,
        "reasons": judge.reasons,
        "stats": judge.stats,
        "upload_log_key": log_key,
    }
