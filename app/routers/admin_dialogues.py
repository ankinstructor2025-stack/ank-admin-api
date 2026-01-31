# admin_dialogues.py

from fastapi import APIRouter, Depends, HTTPException, Query
import os
import json
import urllib.request
import urllib.error
from datetime import timedelta
from typing import Optional

from google.cloud import storage
from google.oauth2 import service_account

from app.deps.auth import require_user
from app.deps.db import get_db
from app.services.contracts_acl import require_contract_admin

router = APIRouter()


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


# ========= 追加：GCS署名URL（download用） =========
SIGNED_URL_EXPIRES_MIN_DEFAULT = 15

def _get_bucket_name() -> str:
    name = (os.environ.get("BUCKET_NAME") or os.environ.get("UPLOAD_BUCKET") or "").strip()
    if not name:
        raise HTTPException(status_code=500, detail="BUCKET_NAME (or UPLOAD_BUCKET) is not set")
    return name

def _resolve_signer_file(path: str) -> Optional[str]:
    if not path:
        return None
    if os.path.isfile(path):
        return path
    if os.path.isdir(path):
        try:
            entries = sorted(os.listdir(path))
        except Exception:
            entries = []
        files = []
        for e in entries:
            p = os.path.join(path, e)
            if os.path.isfile(p):
                files.append(p)
        for p in files:
            if p.lower().endswith(".json"):
                return p
        if len(files) == 1:
            return files[0]
        if files:
            return files[0]
    return None

def _signer_credentials_from_env_or_secret() -> service_account.Credentials:
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

def _generate_signed_download_url(object_key: str, expires_min: int) -> str:
    bucket_name = _get_bucket_name()
    client = _gcs_client_with_signer()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_key)

    # 存在チェック（ユーザー体験のため）
    try:
        if not blob.exists():
            raise HTTPException(status_code=404, detail="qa file not found in GCS")
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=500, detail="failed to access GCS")

    return blob.generate_signed_url(
        version="v4",
        expiration=timedelta(minutes=expires_min),
        method="GET",
    )

def _extract_qa_file_key(knowledge_body: dict) -> Optional[str]:
    """
    knowledge の戻り値揺れに耐える（例）:
      - {"qa_file_object_key": "..."}
      - {"result": {"qa_file_object_key": "..."}}
      - {"manifest": {"qa_file_object_key": "..."}} など
    """
    if not isinstance(knowledge_body, dict):
        return None
    for k in ("qa_file_object_key", "qa_object_key", "qa_file_key"):
        v = knowledge_body.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    # 1段だけ深掘り
    for k in ("result", "manifest", "data", "knowledge"):
        d = knowledge_body.get(k)
        if isinstance(d, dict):
            for kk in ("qa_file_object_key", "qa_object_key", "qa_file_key"):
                v = d.get(kk)
                if isinstance(v, str) and v.strip():
                    return v.strip()
    return None


@router.get("/v1/admin/dialogues")
def list_dialogues(
    contract_id: str = Query(...),
    user=Depends(require_user),
    conn=Depends(get_db),
):
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid in token")

    contract_id = (contract_id or "").strip()
    if not contract_id:
        raise HTTPException(status_code=400, detail="contract_id is required")

    require_contract_admin(uid, contract_id, conn)

    active_object_key = None
    items = []

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT active_dialogue_object_key
            FROM contracts
            WHERE contract_id = %s
            LIMIT 1
            """,
            (contract_id,),
        )
        row = cur.fetchone()
        if row:
            active_object_key = row[0]

        cur.execute(
            """
            SELECT upload_id, object_key, month_key, created_at, kind
            FROM upload_logs
            WHERE contract_id = %s
              AND kind = 'dialogue'
            ORDER BY created_at DESC
            LIMIT 200
            """,
            (contract_id,),
        )
        rows = cur.fetchall() or []

        for (upload_id, object_key, month_key, created_at, kind) in rows:
            items.append(
                {
                    "upload_id": str(upload_id),
                    "object_key": object_key,
                    "month_key": month_key,
                    "created_at": created_at.isoformat() if created_at else None,
                    "kind": kind,
                }
            )

    return {
        "contract_id": contract_id,
        "active_object_key": active_object_key,
        "items": items,
    }


@router.post("/v1/admin/dialogues/activate")
def activate_dialogue(
    payload: dict,
    user=Depends(require_user),
    conn=Depends(get_db),
):
    contract_id = (payload.get("contract_id") or "").strip()
    object_key = (payload.get("object_key") or "").strip()

    if not contract_id or not object_key:
        raise HTTPException(status_code=400, detail="contract_id and object_key are required")

    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid in token")

    require_contract_admin(uid, contract_id, conn)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM upload_logs
            WHERE contract_id = %s
              AND kind = 'dialogue'
              AND object_key = %s
            LIMIT 1
            """,
            (contract_id, object_key),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="dialogue object_key not found in upload_logs")

        cur.execute(
            """
            UPDATE contracts
            SET active_dialogue_object_key = %s,
                updated_at = NOW()
            WHERE contract_id = %s
            """,
            (object_key, contract_id),
        )

    conn.commit()
    return {"ok": True, "contract_id": contract_id, "active_object_key": object_key}


@router.post("/v1/qa/build")
def build_qa(
    payload: dict,
    user=Depends(require_user),
):
    """
    ★修正対象（ここだけ）★

    UIから {contract_id, object_key, output_format} を受け取って knowledge に中継する。
    - DBは一切使わない（Cloud SQL停止前提）
    - object_key は必須（active_dialogue_object_key の参照はしない）
    - knowledge が GCS に QAファイルを保存し、qa_file_object_key を返す前提
    - 返ってきた qa_file_object_key から download_url(署名URL) を生成して返す
    """
    contract_id = (payload.get("contract_id") or "").strip()
    if not contract_id:
        raise HTTPException(status_code=400, detail="contract_id is required")

    object_key = (payload.get("object_key") or "").strip()
    if not object_key:
        raise HTTPException(status_code=400, detail="object_key is required")

    output_format = (payload.get("output_format") or "csv").strip() or "csv"
    expires_min = int(payload.get("expires_min") or SIGNED_URL_EXPIRES_MIN_DEFAULT)

    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid in token")

    # 3) knowledgeへ中継（tenant_id は contract_id をそのまま流用して互換維持）
    base = _get_knowledge_base_url()
    url = f"{base}/v1/qa/build"

    knowledge_body = _http_post_json(
        url,
        {
            "tenant_id": contract_id,    # 今の画面表示の前提に合わせる
            "object_key": object_key,
            "output_format": output_format,
        },
        timeout_sec=120,
    )

    qa_file_key = _extract_qa_file_key(knowledge_body)
    download_url = None

    if qa_file_key:
        # 署名URLでダウンロードできるようにする
        # （このURLをUIの「結果をダウンロード」にそのまま使える）
        download_url = _generate_signed_download_url(qa_file_key, expires_min=max(1, min(expires_min, 60)))

    return {
        "ok": True,
        "contract_id": contract_id,
        "object_key": object_key,
        "status": "requested",
        "knowledge": knowledge_body,
        # 追加（既存のレスポンスを壊さずに情報を足す）
        "qa_file_object_key": qa_file_key,
        "download_url": download_url,
        "expires_min": max(1, min(expires_min, 60)),
    }
