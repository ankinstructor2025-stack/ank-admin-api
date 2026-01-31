import os
import uuid
import json
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, EmailStr

from app.deps.auth import require_user
from app.core.settings import APP_BASE_URL, FROM_EMAIL

# SendGrid は任意（キーが無ければ送らずにURLだけ返す）
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# GCS
from google.cloud import storage

router = APIRouter()

# ==========
# 設定
# ==========
# 例: ank-bucket
ANK_BUCKET = os.environ.get("ANK_BUCKET", "").strip()

# 招待データの保存場所（GCS内）
# tenants/{tenant_id}/invites/pending/{token}.json
# tenants/{tenant_id}/invites/used/{token}.json
def _invite_pending_path(tenant_id: str, token: str) -> str:
    return f"tenants/{tenant_id}/invites/pending/{token}.json"

def _invite_used_path(tenant_id: str, token: str) -> str:
    return f"tenants/{tenant_id}/invites/used/{token}.json"

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _require_bucket_name():
    if not ANK_BUCKET:
        raise HTTPException(status_code=500, detail="ANK_BUCKET not set")

def _gcs_client() -> storage.Client:
    # Cloud Run では通常 ADC で動く
    return storage.Client()

def _bucket() -> storage.Bucket:
    _require_bucket_name()
    return _gcs_client().bucket(ANK_BUCKET)

def _blob_exists(path: str) -> bool:
    b = _bucket().blob(path)
    return b.exists()

def _write_json(path: str, data: dict):
    b = _bucket().blob(path)
    b.upload_from_string(
        json.dumps(data, ensure_ascii=False),
        content_type="application/json; charset=utf-8",
    )

def _read_json(path: str) -> dict:
    b = _bucket().blob(path)
    if not b.exists():
        raise HTTPException(status_code=404, detail="not found")
    s = b.download_as_text(encoding="utf-8")
    try:
        return json.loads(s)
    except Exception:
        raise HTTPException(status_code=500, detail="invalid json in storage")

def _move_blob(src: str, dst: str):
    bucket = _bucket()
    src_blob = bucket.blob(src)
    if not src_blob.exists():
        raise HTTPException(status_code=404, detail="invalid token")
    # copy -> delete
    bucket.copy_blob(src_blob, bucket, dst)
    src_blob.delete()

# ==========
# 入出力
# ==========
class InviteCreateIn(BaseModel):
    # DBの contract_id をやめて tenant_id に寄せる（テナント別管理の前提）
    tenant_id: str
    email: EmailStr

class InviteConsumeIn(BaseModel):
    tenant_id: str
    token: str

# ==========
# 権限（暫定）
# ==========
def require_tenant_admin(uid: str, tenant_id: str):
    """
    ここは本来、tenant の admin 判定が必要。
    DB停止中の暫定として「とりあえず通す」。
    将来:
      - users/{uid}/user.json の tenants で role=admin を確認
      - tenants/{tenant_id}/admins.json を確認
    """
    return

# ==========
# API
# ==========
@router.post("/v1/invites")
def create_invite(
    payload: InviteCreateIn,
    user=Depends(require_user),
):
    """
    DBアクセス停止版:
      - GCSに pending invite JSON を保存
      - （任意で）SendGrid送信
    """
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid in token")

    tenant_id = (payload.tenant_id or "").strip()
    if not tenant_id:
        raise HTTPException(status_code=400, detail="tenant_id required")

    require_tenant_admin(uid, tenant_id)

    token = uuid.uuid4().hex
    invite_url = f"{APP_BASE_URL}/invite.html?token={token}"

    # 既存衝突はほぼ無いが、念のためリトライ
    pending_path = _invite_pending_path(tenant_id, token)
    if _blob_exists(pending_path):
        token = uuid.uuid4().hex
        invite_url = f"{APP_BASE_URL}/invite.html?token={token}"
        pending_path = _invite_pending_path(tenant_id, token)

    invite_doc = {
        "tenant_id": tenant_id,
        "email": payload.email,
        "token": token,
        "status": "pending",
        "created_at": _now_iso(),
        "created_by": uid,
    }

    _write_json(pending_path, invite_doc)

    # SendGrid は任意（無ければ送らずに返す）
    sg_key = os.environ.get("SENDGRID_API_KEY", "").strip()
    if sg_key:
        msg = Mail(
            from_email=FROM_EMAIL,
            to_emails=str(payload.email),
            subject="招待メール",
            plain_text_content=f"以下のURLから登録してください。\n{invite_url}",
        )
        try:
            SendGridAPIClient(sg_key).send(msg)
        except Exception as e:
            # 招待データは保存済み。送信だけ失敗は 502 扱いにするか悩むが、
            # いまは原因調査しやすいようにエラーで返す。
            raise HTTPException(status_code=502, detail=f"sendgrid error: {e}")

    return {
        "ok": True,
        "tenant_id": tenant_id,
        "email": str(payload.email),
        "token": token,         # UIで表示したくないなら消してOK
        "invite_url": invite_url,  # SENDGRID無し環境ではこれを表示して手動送付できる
        "sent": bool(sg_key),
    }

@router.post("/v1/invites/consume")
def consume_invite(
    payload: InviteConsumeIn,
    user=Depends(require_user),
):
    """
    DBアクセス停止版:
      - pending/{token}.json を読み、used/{token}.json へ移動（= 使い切り化）
      - ここでは users/{uid}/user.json を更新しない（次の段階）
    """
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid in token")

    tenant_id = (payload.tenant_id or "").strip()
    token = (payload.token or "").strip()
    if not tenant_id or not token:
        raise HTTPException(status_code=400, detail="tenant_id and token required")

    pending_path = _invite_pending_path(tenant_id, token)
    doc = _read_json(pending_path)

    # 招待メールの宛先とログインユーザーの email を照合したい場合はここでやる
    invited_email = (doc.get("email") or "").strip().lower()
    user_email = (user.get("email") or "").strip().lower()
    if invited_email and user_email and invited_email != user_email:
        # いまの方針が「ここで厳密照合しない」なら、このチェックは外してOK
        raise HTTPException(status_code=403, detail="email mismatch")

    # used に移動して使い切り化
    used_path = _invite_used_path(tenant_id, token)
    # move 前に used が既にあれば「二重consume」なのでok返す方がUIは安定
    if _blob_exists(used_path):
        return {"ok": True, "already_consumed": True}

    # used 側に追記したい情報があるなら、move後にwriteする
    _move_blob(pending_path, used_path)

    # used のJSONを更新（consume情報を追記）
    used_doc = _read_json(used_path)
    used_doc["status"] = "used"
    used_doc["consumed_at"] = _now_iso()
    used_doc["consumed_by"] = uid
    _write_json(used_path, used_doc)

    return {"ok": True, "already_consumed": False}
