# app/routers/uploads.py
# 方針：
# 1) upload-url：署名URLを返すだけ（DBは書かない）
# 2) upload-finalize：GCSの内容をサンプル読取→QA化可否/方式判定
#    - NG：GCS削除 + エラーメッセージ
#    - OK：upload_logs に INSERT（OKの時だけ）
#
# 重要：
# - upload_logs のスキーマが古い（qa_mode/confidenceが無い）場合でも 500 で落ちないようにフォールバックする

import os
import re
import uuid
import json
import csv
from io import StringIO
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from google.cloud import storage
from google.oauth2 import service_account

from app.deps.auth import require_user
from app.deps.db import get_db
from app.services.contracts_acl import require_contract_admin
from app.core import settings as app_settings  # BUCKET_NAME/UPLOAD_BUCKET 等

router = APIRouter()

# -------------------------
# 設定
# -------------------------
ALLOWED_EXTS = {".txt", ".json", ".csv"}
MAX_BYTES = 100 * 1024 * 1024  # 100MB
MIN_BYTES = 1
MAX_DIALOGUE_PER_MONTH = 2000  # dialogueのみ：OKになったものだけカウント

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


def _month_key_jst() -> str:
    # 月単位キー（JST厳密が必要なら後で差し替え）
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
    # 読み取りは実行SAでOK（必要な権限が付いている前提）
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


def _db_has_columns(conn, table: str, cols: List[str]) -> Dict[str, bool]:
    """
    upload_logs の列が存在するか確認（存在しない列を INSERT して 500 になるのを避ける）
    """
    colset = set()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            """,
            (table,),
        )
        for row in cur.fetchall():
            colset.add(row[0])
    return {c: (c in colset) for c in cols}


# -------------------------
# 判定ロジック（最小）
# -------------------------
class JudgeResult(BaseModel):
    ok: bool
    qa_mode: Optional[str] = None
    confidence: float = 0.0
    reasons: List[str] = []
    stats: Dict[str, Any] = {}


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
    try:
        reader = csv.reader(StringIO(text))
        rows = list(reader)
        if not rows:
            return None
        header = rows[0]
        return {"rows": len(rows), "cols": len(header)}
    except Exception:
        return None


def judge_qa_mode(filename: str, content_type: str, text: str) -> JudgeResult:
    ext = _ext_lower(filename)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    stats = {
        "ext": ext,
        "lines": len(lines),
        "chars": len(text),
        "content_type": content_type,
    }

    if len(text.strip()) < 50:
        return JudgeResult(ok=False, qa_mode=None, confidence=0.0, reasons=["内容が短すぎます"], stats=stats)

    # JSON
    if ext == ".json" or _looks_like_json(text):
        j = _try_parse_json(text)
        if j:
            return JudgeResult(ok=True, qa_mode=f"json:{j['kind']}", confidence=0.85, reasons=["JSONとして解析できました"], stats=stats)

    # CSV
    if ext == ".csv":
        c = _try_parse_csv(text)
        if c:
            return JudgeResult(ok=True, qa_mode="csv", confidence=0.75, reasons=["CSVとして解析できました"], stats=stats)

    # text dialogueっぽい（簡易）
    pat_dialogue = [
        r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}",
        r"発言",
        r"質問",
        r"答弁",
        r"委員",
        r"大臣",
    ]
    hit = _count_matches(lines[:200], pat_dialogue)
    if hit >= 3:
        return JudgeResult(ok=True, qa_mode="text:dialogue", confidence=0.7, reasons=["対話/ぎじろくっぽい特徴がありました"], stats={**stats, "hit": hit})

    # 一般テキスト
    if len(lines) >= 5:
        return JudgeResult(ok=True, qa_mode="text:generic", confidence=0.55, reasons=["一般テキストとして取り扱います"], stats=stats)

    return JudgeResult(ok=False, qa_mode=None, confidence=0.0, reasons=["形式が判定できません（特徴が弱い）"], stats=stats)


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
    # 1) 認証
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid in token")

    # 2) 入力
    contract_id = (req.contract_id or "").strip()
    if not contract_id:
        raise HTTPException(status_code=400, detail="contract_id is required")

    require_contract_admin(uid, contract_id, conn)
    _validate_file_meta(req.filename, int(req.size_bytes or 0))

    # 3) 月キー＆上限（OKのときだけカウント = upload_logs参照）
    mk = _month_key_jst()
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

    # 4) object_key
    upload_id = str(uuid.uuid4())
    safe = _safe_name(req.filename)
    object_key = f"contracts/{contract_id}/{mk}/{upload_id}_{safe}"

    # 5) Signed URL
    bucket_name = _get_bucket_name()
    client = _gcs_client_with_signer()
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
    # 1) 認証
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid in token")

    contract_id = (payload.get("contract_id") or "").strip()
    object_key = (payload.get("object_key") or "").strip()
    upload_id = (payload.get("upload_id") or "").strip()
    filename = (payload.get("filename") or "").strip()  # 任意（UIが送るなら使う）
    content_type = (payload.get("content_type") or "").strip()  # 任意

    if not contract_id or not object_key or not upload_id:
        raise HTTPException(status_code=400, detail="contract_id, object_key, upload_id are required")

    require_contract_admin(uid, contract_id, conn)

    # 2) GCSから先頭を読む（判定用）
    text = _gcs_read_head_text(object_key)
    judge = judge_qa_mode(filename or object_key, content_type, text)

    if not judge.ok:
        # NG：削除して返す
        _gcs_delete(object_key)
        return {
            "ok": False,
            "message": "QA化できない形式です",
            "reasons": judge.reasons,
            "stats": judge.stats,
        }

    # 3) OK：upload_logsへINSERT（OKのときだけ）
    mk = _month_key_jst()
    kind = "dialogue"

    # DBスキーマ差を吸収
    col = _db_has_columns(conn, "upload_logs", ["qa_mode", "confidence"])

    try:
        with conn.cursor() as cur:
            if col["qa_mode"] and col["confidence"]:
                cur.execute(
                    """
                    INSERT INTO upload_logs
                      (upload_id, contract_id, kind, object_key, month_key, qa_mode, confidence, created_at)
                    VALUES
                      (%s, %s, %s, %s, %s, %s, %s, NOW())
                    """,
                    (upload_id, contract_id, kind, object_key, mk, judge.qa_mode, float(judge.confidence)),
                )
            else:
                # 古い定義でも落とさない（qa_mode/confidenceは保存しない）
                cur.execute(
                    """
                    INSERT INTO upload_logs
                      (upload_id, contract_id, kind, object_key, month_key, created_at)
                    VALUES
                      (%s, %s, %s, %s, %s, NOW())
                    """,
                    (upload_id, contract_id, kind, object_key, mk),
                )
        conn.commit()
    except Exception as e:
        # DB失敗時：GCSは消さない（原因調査用に残す）
        raise HTTPException(status_code=500, detail=f"failed to insert upload_logs: {e}")

    return {
        "ok": True,
        "upload_id": upload_id,
        "contract_id": contract_id,
        "object_key": object_key,
        "qa_mode": judge.qa_mode,
        "confidence": judge.confidence,
        "reasons": judge.reasons,
        "stats": judge.stats,
    }
