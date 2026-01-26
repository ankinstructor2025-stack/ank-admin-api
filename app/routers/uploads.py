# app/routers/uploads.py
# 方針：
# 1) upload-url：署名URLを返すだけ（DBは書かない）
# 2) upload-finalize：GCSの内容をサンプル読取→QA化可否/方式判定
#    - NG：GCS削除 + エラーメッセージ
#    - OK：upload_logs に INSERT（OKの時だけ）

import os
import re
import uuid
import json
import csv
from io import StringIO
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from google.cloud import storage
from google.oauth2 import service_account

from app.deps.auth import require_user
from app.deps.db import get_db
from app.services.contracts_acl import require_contract_admin
from app.core import settings as app_settings  # BUCKET_NAME/UPLOAD_BUCKET 等

router = APIRouter()

# 画面と合わせる（.txt / .json / .csv、1KB〜100MB）
ALLOWED_EXTS = {".txt", ".json", ".csv"}
MIN_BYTES = 1024
MAX_BYTES = 100 * 1024 * 1024

# dialogue 月上限（OKになったものだけ count する想定）
MAX_DIALOGUE_PER_MONTH = int(os.getenv("MAX_DIALOGUE_PER_MONTH", "5"))

# 判定サンプル（先頭だけ読む）
MAX_SAMPLE_BYTES_DEFAULT = 2_000_000


# -------------------------
# util
# -------------------------
def _month_key_jst() -> str:
    # 月キーが必要なだけなので簡易でOK（厳密JSTが必要なら後で差し替え）
    return datetime.now().strftime("%Y-%m")


def _get_bucket_name() -> str:
    bn = getattr(app_settings, "BUCKET_NAME", "") or ""
    if not bn:
        bn = getattr(app_settings, "UPLOAD_BUCKET", "") or ""
    bn = (bn or "").strip()
    if not bn:
        raise HTTPException(status_code=500, detail="BUCKET_NAME (or UPLOAD_BUCKET) is not set")
    return bn


def _ext_lower(filename: str) -> str:
    name = (filename or "").strip().lower()
    i = name.rfind(".")
    return name[i:] if i >= 0 else ""


def _validate_file_meta(filename: str, size_bytes: int):
    ext = _ext_lower(filename)
    if ext not in ALLOWED_EXTS:
        raise HTTPException(
            status_code=400,
            detail="許可されていないファイル形式です（.txt / .json / .csv）",
        )
    if size_bytes < MIN_BYTES:
        raise HTTPException(status_code=400, detail="ファイルサイズが小さすぎます（1KB以上）")
    if size_bytes > MAX_BYTES:
        raise HTTPException(status_code=400, detail="ファイルサイズが大きすぎます（上限100MB）")


def _storage_client_with_signer():
    # 署名に使う鍵（Secret Manager を /secrets にマウントしている想定）
    signer_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or "/secrets/ank-gcs-signer"
    if os.path.exists(signer_path):
        cred = service_account.Credentials.from_service_account_file(signer_path)
        return storage.Client(credentials=cred)
    # 無ければデフォルト（Cloud Run SA）
    return storage.Client()


def _gcs_read_head_text(object_key: str, limit_bytes: int) -> str:
    bucket_name = _get_bucket_name()
    client = _storage_client_with_signer()
    blob = client.bucket(bucket_name).blob(object_key)
    data = blob.download_as_bytes(start=0, end=max(0, limit_bytes - 1))
    return data.decode("utf-8", errors="replace")


def _gcs_delete(object_key: str) -> None:
    bucket_name = _get_bucket_name()
    client = _storage_client_with_signer()
    blob = client.bucket(bucket_name).blob(object_key)
    blob.delete()


# -------------------------
# 判定（方式A〜F / 不可）
# -------------------------
class JudgeResult(BaseModel):
    can_extract_qa: bool
    method: Optional[str] = None  # "A".."F"
    confidence: float = 0.0
    reasons: List[str] = []
    stats: Dict[str, Any] = {}


_SPEAKER_PATTERNS = [
    r"^\s*(user|assistant|system)\s*[:：]",
    r"^\s*(u|a|s)\s*[:：]",
    r"^\s*[^\s]{1,20}\s*[:：]\s+",
]
_QA_PATTERNS = [
    r"^\s*Q\s*[:：]",
    r"^\s*A\s*[:：]",
    r"^\s*質問\s*[:：]",
    r"^\s*回答\s*[:：]",
]
_QUOTE_PATTERNS = [
    r"^\s*>",
    r"^\s*From:\s",
    r"^\s*Sent:\s",
    r"^\s*Subject:\s",
]


def _count_matches(lines: List[str], patterns: List[str]) -> int:
    regs = [re.compile(p, re.IGNORECASE) for p in patterns]
    n = 0
    for ln in lines:
        for rg in regs:
            if rg.search(ln):
                n += 1
                break
    return n


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
    head = "\n".join(text.splitlines()[:200])
    try:
        f = StringIO(head)
        reader = csv.reader(f)
        rows = [r for r in reader if r]
        if len(rows) < 2:
            return None
        header = [c.strip().lower() for c in rows[0]]
        has_speaker_text = ("speaker" in header and ("text" in header or "message" in header or "content" in header))
        has_role_content = ("role" in header and ("content" in header or "text" in header or "message" in header))
        if has_speaker_text or has_role_content:
            return {"header": header}
        return None
    except Exception:
        return None


def _judge_text(object_key: str, text: str) -> JudgeResult:
    ext = _ext_lower(object_key.split("/")[-1])
    lines = text.splitlines()
    nonempty = [ln for ln in lines if ln.strip()]

    stats = {
        "ext": ext,
        "lines": len(lines),
        "nonempty_lines": len(nonempty),
        "sample_bytes": len(text.encode("utf-8", errors="ignore")),
    }

    if len(nonempty) < 10:
        return JudgeResult(
            can_extract_qa=False,
            method=None,
            confidence=0.0,
            reasons=["内容が少なすぎます（行数が不足）"],
            stats=stats,
        )

    # JSON → 方式B
    if ext == ".json" or _looks_like_json(text):
        meta = _try_parse_json(text)
        if meta:
            return JudgeResult(
                can_extract_qa=True,
                method="B",
                confidence=0.92,
                reasons=[f"JSONとして解析できる（{meta.get('kind')}）"],
                stats={**stats, "json_kind": meta.get("kind")},
            )

    # CSV → 方式C
    if ext == ".csv":
        meta = _try_parse_csv(text)
        if meta:
            return JudgeResult(
                can_extract_qa=True,
                method="C",
                confidence=0.9,
                reasons=["CSVヘッダに speaker/text または role/content がある"],
                stats={**stats, "csv_header": meta.get("header")},
            )

    # カウント系
    speaker_markers = _count_matches(nonempty[:2000], _SPEAKER_PATTERNS)
    qa_markers = _count_matches(nonempty[:2000], _QA_PATTERNS)
    quote_markers = _count_matches(nonempty[:2000], _QUOTE_PATTERNS)

    stats.update({
        "speaker_markers": speaker_markers,
        "qa_markers": qa_markers,
        "quote_markers": quote_markers,
    })

    # 方式E：Q/A形式が強い
    if qa_markers >= 6:
        return JudgeResult(True, "E", 0.85, ["Q/A 記法が一定数ある（Q:, A: など）"], stats)

    # 方式F：メール/チケット/引用っぽい
    if quote_markers >= 6:
        return JudgeResult(True, "F", 0.75, ["引用/ヘッダ行が多い（メール/スレ形式の可能性）"], stats)

    # 方式A：話者ラベルの対話
    if speaker_markers >= 10:
        return JudgeResult(True, "A", 0.78, ["話者ラベルが複数回出現（User:, Assistant: など）"], stats)

    # 方式D：単一文章（QA生成寄り）
    if len(nonempty) >= 30:
        return JudgeResult(True, "D", 0.6, ["文章量があるため単一文書としてQA化（生成寄り）が可能"], stats)

    return JudgeResult(False, None, 0.0, ["形式が判定できません（特徴が弱い）"], stats)


# -------------------------
# API
# -------------------------
class UploadUrlRequest(BaseModel):
    contract_id: str = Field(..., min_length=1)
    kind: str = Field(default="dialogue")
    filename: str = Field(..., min_length=1)
    content_type: str = Field(default="application/octet-stream")
    size_bytes: int = Field(default=0, ge=0)
    note: str = Field(default="")


@router.post("/v1/admin/upload-url")
def create_upload_url(
    req: UploadUrlRequest,
    user=Depends(require_user),
    conn=Depends(get_db),
):
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid in token")

    contract_id = (req.contract_id or "").strip()
    if not contract_id:
        raise HTTPException(status_code=400, detail="contract_id is required")

    require_contract_admin(uid, contract_id, conn)
    _validate_file_meta(req.filename, int(req.size_bytes or 0))

    mk = _month_key_jst()

    # 上限判定は「OKになったものだけ」数える（= upload_logs のみ参照）
    if (req.kind or "dialogue") == "dialogue":
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM upload_logs
                WHERE contract_id = %s
                  AND kind = 'dialogue'
                  AND month_key = %s
                """,
                (contract_id, mk),
            )
            cnt = int(cur.fetchone()[0] or 0)
        if cnt >= MAX_DIALOGUE_PER_MONTH:
            raise HTTPException(
                status_code=409,
                detail=f"dialogue uploads limit reached: {cnt}/{MAX_DIALOGUE_PER_MONTH} for {mk}",
            )

    upload_id = uuid.uuid4().hex
    safe_name = (req.filename or "file").replace("/", "_").replace("\\", "_")
    object_key = f"contracts/{contract_id}/{mk}/{upload_id}_{safe_name}"

    # Signed URL（PUT）
    signer_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or "/secrets/ank-gcs-signer"
    if not os.path.exists(signer_path):
        raise HTTPException(status_code=500, detail=f"signer file not found: {signer_path}")

    credentials = service_account.Credentials.from_service_account_file(signer_path)
    bucket_name = _get_bucket_name()
    client = storage.Client(credentials=credentials)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_key)

    url = blob.generate_signed_url(
        version="v4",
        expiration=timedelta(minutes=15),
        method="PUT",
        content_type=req.content_type or "application/octet-stream",
    )

    # DBは書かない（OK時だけ）
    return {
        "upload_id": upload_id,
        "object_key": object_key,
        "upload_url": url,
        "month_key": mk,
    }


@router.post("/v1/admin/upload-finalize")
def upload_finalize(
    payload: dict,
    user=Depends(require_user),
    conn=Depends(get_db),
):
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid in token")

    contract_id = (payload.get("contract_id") or "").strip()
    object_key = (payload.get("object_key") or "").strip()
    upload_id = (payload.get("upload_id") or "").strip()
    kind = (payload.get("kind") or "dialogue").strip()  # フロントが送らなくてもOK
    sample_bytes = int(payload.get("sample_bytes") or MAX_SAMPLE_BYTES_DEFAULT)

    if not contract_id or not object_key or not upload_id:
        raise HTTPException(status_code=400, detail="contract_id, object_key, upload_id are required")

    require_contract_admin(uid, contract_id, conn)

    # 1) GCS先頭サンプルを読む
    try:
        text = _gcs_read_head_text(object_key, max(10_000, min(sample_bytes, MAX_SAMPLE_BYTES_DEFAULT)))
    except Exception as e:
        # 読めない＝NG。削除は試みる
        try:
            _gcs_delete(object_key)
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"failed to read uploaded file: {e}")

    # 2) 判定
    judge = _judge_text(object_key, text)

    # 3) NGなら削除してエラー
    if not judge.can_extract_qa:
        try:
            _gcs_delete(object_key)
        except Exception as e:
            # 削除に失敗しても「NG」はNG。理由だけ返す
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "QA化できません（削除に失敗）",
                    "reasons": judge.reasons,
                    "delete_error": str(e),
                },
            )

        raise HTTPException(
            status_code=409,
            detail={
                "message": "QA化できません（アップロードしたファイルは削除しました）",
                "reasons": judge.reasons,
                "stats": judge.stats,
            },
        )

    # 4) OKなら upload_logs に INSERT（OK時だけ）
    mk = _month_key_jst()
    with conn.cursor() as cur:
        # 最小カラムだけ（あなたの既存SELECTに合わせる）
        cur.execute(
            """
            INSERT INTO upload_logs (upload_id, contract_id, kind, object_key, month_key, created_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            """,
            (upload_id, contract_id, kind, object_key, mk),
        )
    conn.commit()

    # 方式を返す（フロント表示用）
    return {
        "ok": True,
        "upload_id": upload_id,
        "contract_id": contract_id,
        "object_key": object_key,
        "month_key": mk,
        "can_extract_qa": True,
        "method": judge.method,
        "confidence": judge.confidence,
        "reasons": judge.reasons,
        "stats": judge.stats,
    }
