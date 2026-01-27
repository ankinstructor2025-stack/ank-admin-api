# admin_dialogues.py

from fastapi import APIRouter, Depends, HTTPException, Query
import os
import json
import urllib.request
import urllib.error

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
    conn=Depends(get_db),
):
    """
    UIから {contract_id, object_key} を受け取って knowledge に中継する。
    - object_key が来ていればそれを優先
    - 無ければ contracts.active_dialogue_object_key を使う（互換）
    - upload_logs(kind='dialogue') に存在するか検証
    - knowledge の /v1/qa/build に POST して結果を返す（今はechoでOK）
    """
    contract_id = (payload.get("contract_id") or "").strip()
    if not contract_id:
        raise HTTPException(status_code=400, detail="contract_id is required")

    object_key = (payload.get("object_key") or "").strip()

    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid in token")

    require_contract_admin(uid, contract_id, conn)

    # 1) object_key決定（UI指定優先 → 無ければactive）
    if not object_key:
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
            object_key = row[0] if row else None

    if not object_key:
        raise HTTPException(status_code=409, detail="dialogue data is not selected")

    # 2) upload_logsに存在するか（dialogue限定）
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

    # 3) knowledgeへ中継
    base = _get_knowledge_base_url()
    url = f"{base}/v1/qa/build"

    knowledge_body = _http_post_json(
        url,
        {"contract_id": contract_id, "object_key": object_key},
        timeout_sec=120,
    )

    return {
        "ok": True,
        "contract_id": contract_id,
        "object_key": object_key,
        "status": "requested",
        "knowledge": knowledge_body,
    }
