# admin_dialogues.py
#
# 現在の前提（重要）：
# - Cloud SQL を使わない
# - SQLite にも書き込まない
# - QA生成の結果は「ファイルとしてGCSに保存」する（knowledge側の責務）
#
# このファイルの責務：
# - UIから tenant_id/object_key を受け取って knowledge API に中継する
# - （任意）生成された qa_file_object_key のダウンロード用署名URL(GET)を発行する

import os
import json
import urllib.request
import urllib.error
from datetime import timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from google.cloud import storage
from google.oauth2 import service_account

from app.deps.auth import require_user

router = APIRouter()

# ==========
# Settings
# ==========
SIGNED_URL_EXPIRES_MIN = 15

def _get_bucket_name() -> str:
    # app.core.settings を使っているならそこに寄せてもOK
    # ここは最小で環境変数優先にする
    name = (os.environ.get("BUCKET_NAME") or os.environ.get("UPLOAD_BUCKET") or "").strip()
    if not name:
        raise HTTPException(status_code=500, detail="BUCKET_NAME (or UPLOAD_BUCKET) is not set")
    return name

def _get_knowledge_base_url() -> str:
    """
    admin -> knowledge の中継先。
    Cloud Run の環境変数 KNOWLEDGE_API_BASE_URL に設定する。
    例: https://ank-knowledge-api-xxxx.asia-northeast1.run.app
    """
    base = (os.environ.get("KNOWLEDGE_API_BASE_URL") or "").strip()
    if not base:
        raise HTTPException(status_code=500, detail="KNOWLEDGE_API_BASE_URL is not set")
    return base.rstrip("/")

def _http_post_json(url: str, payload: dict, timeout_sec: int = 120) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url=url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            try:
                return json.loads(body)
            except Exception:
                return {"raw": body}

    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")

        # JSONなら整形して「文字列」で返す（UIログで読めるように）
        try:
            j = json.loads(raw)
            detail_text = json.dumps(
                {"knowledge_status": e.code, "body": j},
                ensure_ascii=False
            )
        except Exception:
            detail_text = f"knowledge_status={e.code} body={raw}"

        raise HTTPException(status_code=502, detail=detail_text)

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"failed to call knowledge api: {e}")

# ==========
# GCS signer (download url)
# ==========
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
        if cand:
            return cand[0]
    return None

def _signer_credentials_from_env_or_secret() -> service_account.Credentials:
    """
    署名URL生成のためのサービスアカウント鍵を読む。
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

def _generate_signed_download_url(object_key: str, expires_min: int = SIGNED_URL_EXPIRES_MIN) -> str:
    bucket_name = _get_bucket_name()
    client = _gcs_client_with_signer()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_key)

    # 存在チェック（無くてもURLは生成できるが、UI的には404の方が親切）
    try:
        if not blob.exists():
            raise HTTPException(status_code=404, detail="object not found in GCS")
    except HTTPException:
        raise
    except Exception:
        # exists() が失敗しても署名URL自体は作れる可能性があるので続行しても良いが、
        # ここは「壊れている」検知としてエラーに倒す
        raise HTTPException(status_code=500, detail="failed to access GCS")

    url = blob.generate_signed_url(
        version="v4",
        expiration=timedelta(minutes=expires_min),
        method="GET",
    )
    return url

# ==========
# Models
# ==========
class BuildQaIn(BaseModel):
    # 正：tenant_id。互換で contract_id も受ける
    tenant_id: Optional[str] = Field(default=None)
    contract_id: Optional[str] = Field(default=None)

    # uploads で保存した元データのGCSキー
    object_key: str = Field(..., min_length=1)

    # 出力形式（knowledge側で対応）
    output_format: str = Field(default="jsonl")

class BuildQaOut(BaseModel):
    ok: bool
    tenant_id: str
    object_key: str
    knowledge: dict

class DownloadUrlIn(BaseModel):
    # knowledgeが返す qa_file_object_key をそのまま受ける
    object_key: str = Field(..., min_length=1)
    expires_min: int = Field(default=SIGNED_URL_EXPIRES_MIN, ge=1, le=60)

class DownloadUrlOut(BaseModel):
    ok: bool
    object_key: str
    download_url: str
    expires_min: int

# ==========
# Endpoints
# ==========
@router.post("/v1/qa/build", response_model=BuildQaOut)
def build_qa(req: BuildQaIn, user=Depends(require_user)):
    """
    DB未使用：
    UI → admin → knowledge へ中継するだけ。
    生成結果の保存（GCS）は knowledge 側が行う。
    """
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid in token")

    tenant_id = (req.tenant_id or "").strip()
    if not tenant_id:
        tenant_id = (req.contract_id or "").strip()  # 互換
    if not tenant_id:
        raise HTTPException(status_code=400, detail="tenant_id (or contract_id) is required")

    object_key = (req.object_key or "").strip()
    if not object_key:
        raise HTTPException(status_code=400, detail="object_key is required")

    base = _get_knowledge_base_url()
    url = f"{base}/v1/qa/build"

    # knowledge側の想定I/F：
    # 返り値に qa_file_object_key / manifest_object_key などが入る前提
    knowledge_body = _http_post_json(
        url,
        {
            "tenant_id": tenant_id,
            "object_key": object_key,
            "output_format": (req.output_format or "jsonl").strip() or "jsonl",
        },
        timeout_sec=120,
    )

    return {
        "ok": True,
        "tenant_id": tenant_id,
        "object_key": object_key,
        "knowledge": knowledge_body,
    }

@router.post("/v1/qa/download-url", response_model=DownloadUrlOut)
def qa_download_url(req: DownloadUrlIn, user=Depends(require_user)):
    """
    DB未使用：
    GCSの object_key からダウンロード用署名URL(GET)を返す。
    """
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid in token")

    object_key = (req.object_key or "").strip()
    if not object_key:
        raise HTTPException(status_code=400, detail="object_key is required")

    url = _generate_signed_download_url(object_key, expires_min=req.expires_min)

    return {
        "ok": True,
        "object_key": object_key,
        "download_url": url,
        "expires_min": req.expires_min,
    }
