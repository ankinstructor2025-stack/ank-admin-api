from fastapi import APIRouter, Depends, HTTPException, Query
import os
import requests

from app.deps.auth import require_user
from app.deps.db import get_db
from app.services.contracts_acl import require_contract_admin

router = APIRouter()


def _get_knowledge_base_url() -> str:
    """
    admin -> knowledge の中継先。
    環境変数 KNOWLEDGE_API_BASE_URL を優先し、無ければ同一プロジェクト内のURLを直書きしない。
    """
    base = (os.environ.get("KNOWLEDGE_API_BASE_URL") or "").strip()
    if not base:
        # ここは直書きしない。未設定なら 500 で止める（壊さないため）。
        raise HTTPException(status_code=500, detail="KNOWLEDGE_API_BASE_URL is not set")
    return base.rstrip("/")


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
        # 現在有効な対話データ
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

        # 対話データの一覧
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


@router.post("/v1/admin/dialogues/build-qa")
def build_qa(
    payload: dict,
    user=Depends(require_user),
    conn=Depends(get_db),
):
    """
    admin UI から {contract_id, object_key} を受け取り、
    - object_key があればそれを優先
    - 無ければ contracts.active_dialogue_object_key を使う
    - upload_logs に存在する dialogue object_key であることを確認
    - knowledge API に中継して、そのレスポンスを返す（現段階は echo でOK）
    """
    contract_id = (payload.get("contract_id") or "").strip()
    if not contract_id:
        raise HTTPException(status_code=400, detail="contract_id is required")

    object_key = (payload.get("object_key") or "").strip()  # UIが送ってくる想定

    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid in token")

    require_contract_admin(uid, contract_id, conn)

    # 1) object_key を決める（UI指定を優先）
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

    # 2) upload_logs に存在するか確認（dialogue限定）
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

    # 3) knowledge API に中継（今は echo でもOK）
    base_url = _get_knowledge_base_url()
    url = f"{base_url}/v1/knowledge/build-qa"

    try:
        # ここは「まず動かす」ため、admin側のIDトークン転送はしない（knowledge側はpublic想定）
        # 将来: knowledge側をprivateにするなら Authorization を転送する。
        resp = requests.post(
            url,
            json={"contract_id": contract_id, "object_key": object_key},
            timeout=15,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"failed to call knowledge api: {e}")

    # knowledge 側のエラーは、そのまま見える形で返す
    ct = (resp.headers.get("content-type") or "").lower()
    if "application/json" in ct:
        body = resp.json()
    else:
        body = {"raw": resp.text}

    if resp.status_code >= 400:
        raise HTTPException(status_code=502, detail={"knowledge_status": resp.status_code, "body": body})

    return {
        "ok": True,
        "contract_id": contract_id,
        "object_key": object_key,
        "status": "requested",
        "knowledge": body,
    }
