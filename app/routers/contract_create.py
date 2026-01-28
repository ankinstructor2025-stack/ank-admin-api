import os
import json
import uuid
import sqlite3
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.deps.auth import require_user

# GCS
from google.cloud import storage

from app.core.settings import BUCKET_NAME

router = APIRouter()

# ==========
# Settings
# ==========
APP_BASE_URL = os.environ.get(
    "APP_BASE_URL",
    "https://ankinstructor2025-stack.github.io/ank-knowledge"
)
FROM_EMAIL = os.environ.get("INVITE_FROM_EMAIL", "ank.instructor2025@gmail.com")

_storage_client = storage.Client()


class ContractCreate(BaseModel):
    user_id: str
    email: str
    display_name: str | None = None
    seat_limit: int
    knowledge_count: int
    monthly_amount_yen: int
    note: str | None = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _bucket():
    if not BUCKET_NAME:
        raise HTTPException(status_code=500, detail="BUCKET_NAME is not set")
    return _storage_client.bucket(BUCKET_NAME)


def _upload_json(bucket, path: str, data: dict, *, if_generation_match=None):
    blob = bucket.blob(path)
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    blob.upload_from_string(
        payload,
        content_type="application/json; charset=utf-8",
        if_generation_match=if_generation_match,
    )
    return blob


# ==========
# SQLite init
# ==========

_SCHEMA_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS knowledge_files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  object_key TEXT NOT NULL,
  filename TEXT NOT NULL,
  status TEXT NOT NULL,      -- uploaded / processed / error
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS qa (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  question TEXT NOT NULL,
  answer TEXT NOT NULL,
  source_key TEXT,
  created_at TEXT NOT NULL
);

INSERT OR IGNORE INTO meta(key, value) VALUES ('schema_version', '1');
"""

def _create_sqlite_file(local_path: str, *, contract_id: str, role: str):
    # role: "write" or "read"
    conn = sqlite3.connect(local_path)
    try:
        cur = conn.cursor()
        cur.executescript(_SCHEMA_SQL)
        cur.execute("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)", ("contract_id", contract_id))
        cur.execute("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)", ("db_role", role))
        cur.execute("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)", ("created_at", _now_iso()))
        conn.commit()
    finally:
        conn.close()


def _upload_file(bucket, gcs_path: str, local_path: str):
    blob = bucket.blob(gcs_path)
    # SQLiteはapplication/x-sqlite3でもよいが、octet-streamで十分
    blob.upload_from_filename(local_path, content_type="application/octet-stream")
    return blob


@router.post("/v1/contract")
def create_contract(
    payload: ContractCreate,
    user=Depends(require_user),
):
    """
    新: GCS に契約フォルダと初期ファイルを作成（DBはSQLiteを生成してアップロード）

    作成物:
      tenants/{contract_id}/.keep
      tenants/{contract_id}/contract.json
      tenants/{contract_id}/members/{uid}.json
      tenants/{contract_id}/meta.json
      tenants/{contract_id}/db/read.db    (SQLite実体)
      tenants/{contract_id}/db/write.db   (SQLite実体)
    """
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=401, detail="unauthorized")

    # フロントから user_id が来る前提なので整合性だけ見る
    if (payload.user_id or "").strip() != uid:
        raise HTTPException(status_code=403, detail="uid mismatch")

    # 最低限の入力チェック
    email = (payload.email or "").strip()
    if not email:
        raise HTTPException(status_code=400, detail="email is required")

    contract_id = uuid.uuid4().hex
    now = _now_iso()

    bucket = _bucket()
    base = f"tenants/{contract_id}/"

    # 1) “フォルダ”作成（.keep）
    bucket.blob(base + ".keep").upload_from_string(
        b"", content_type="application/octet-stream"
    )

    # 2) contract.json
    contract = {
        "contract_id": contract_id,
        "status": "active",
        "start_at": now,
        "seat_limit": int(payload.seat_limit),
        "knowledge_count": int(payload.knowledge_count),
        "payment_method_configured": False,
        "monthly_amount_yen": int(payload.monthly_amount_yen),
        "note": (payload.note or "").strip() or None,
        "created_at": now,
        "updated_at": now,
        "schema_version": 1,
        "owner": {
            "uid": uid,
            "email": email,
            "display_name": payload.display_name or "",
        },
    }
    _upload_json(bucket, base + "contract.json", contract)

    # 3) members/{uid}.json（owner/adminの起点）
    member = {
        "uid": uid,
        "email": email,
        "display_name": payload.display_name or "",
        "role": "owner",
        "status": "active",
        "created_at": now,
        "updated_at": now,
    }
    _upload_json(bucket, base + f"members/{uid}.json", member)

    # 4) meta.json（ロックやDB世代管理の置き場）
    meta = {
        "contract_id": contract_id,
        "locked": False,
        "lock_reason": "",
        "lock_owner_uid": "",
        "lock_created_at": None,
        "db_generation": 0,
        "schema_version": 1,
        "created_at": now,
        "updated_at": now,
    }
    _upload_json(bucket, base + "meta.json", meta)

    # 5) SQLite を生成してアップロード（0Bプレースホルダは作らない）
    tmp_write = f"/tmp/{contract_id}_write.db"
    tmp_read = f"/tmp/{contract_id}_read.db"

    _create_sqlite_file(tmp_write, contract_id=contract_id, role="write")
    _create_sqlite_file(tmp_read, contract_id=contract_id, role="read")

    _upload_file(bucket, base + "db/write.db", tmp_write)
    _upload_file(bucket, base + "db/read.db", tmp_read)

    # /tmp を掃除（任意：なくても良いが、明示するなら）
    try:
        os.remove(tmp_write)
    except Exception:
        pass
    try:
        os.remove(tmp_read)
    except Exception:
        pass

    return {"contract_id": contract_id, "status": "active"}
