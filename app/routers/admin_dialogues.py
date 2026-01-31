# admin_dialogues.py
#
# 前提：
# - Cloud SQL を使わない（DB import / DB access をしない）
# - QA生成結果は "ファイル" として GCS に保存する（保存処理は knowledge 側）
# - admin は UI から受けた {tenant_id(or contract_id), object_key, output_format} を
#   knowledge の /v1/qa/build に中継し、qa_file_object_key を返す
#
# 注意：
# - /v1/admin/dialogues と /v1/admin/dialogues/activate は、DB前提のため一旦 501 で無効化
#   （importで落ちないことを優先）

from fastapi import APIRouter, Depends, HTTPException, Query
import os
import json
import urllib.request
import urllib.error

from app.deps.auth import require_user

router = APIRouter()


# -------------------------
# knowledge API bridge
# -------------------------
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


def _extract_qa_file_key(knowledge_body: dict):
    """
    knowledge の戻り値揺れに耐える（最低限）
      - {"qa_file_object_key": "..."}
      - {"result": {"qa_file_object_key": "..."}}
      - {"manifest": {"qa_file_object_key": "..."}}
    """
    if not isinstance(knowledge_body, dict):
        return None

    # 直下
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


# -------------------------
# DB前提の機能は一旦無効化（importで落ちないこと優先）
# -------------------------
@router.get("/v1/admin/dialogues")
def list_dialogues(
    contract_id: str = Query(...),
    user=Depends(require_user),
):
    raise HTTPException(status_code=501, detail="disabled: database is not used")


@router.post("/v1/admin/dialogues/activate")
def activate_dialogue(
    payload: dict,
    user=Depends(require_user),
):
    raise HTTPException(status_code=501, detail="disabled: database is not used")


# -------------------------
# 本命：QA生成（中継のみ）
# -------------------------
@router.post("/v1/qa/build")
def build_qa(
    payload: dict,
    user=Depends(require_user),
):
    """
    UIから {contract_id(or tenant_id), object_key, output_format} を受け取って knowledge に中継する。

    前提：
    - DBは使わない
    - knowledge 側が QA成果物を GCS に保存して、そのキー（qa_file_object_key）を返す
    - admin は qa_file_object_key を UI に返す（UIはそれを使ってDLする）
    """
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid in token")

    # tenant_id を正、互換で contract_id
    tenant_id = (payload.get("tenant_id") or "").strip()
    contract_id = (payload.get("contract_id") or "").strip()
    if not tenant_id:
        tenant_id = contract_id
    if not tenant_id:
        raise HTTPException(status_code=400, detail="tenant_id (or contract_id) is required")

    object_key = (payload.get("object_key") or "").strip()
    if not object_key:
        raise HTTPException(status_code=400, detail="object_key is required")

    # UIが選んだ形式（未対応は knowledge 側で弾いてOK）
    output_format = (payload.get("output_format") or "csv").strip() or "csv"

    # knowledgeへ中継
    base = _get_knowledge_base_url()
    url = f"{base}/v1/qa/build"

    knowledge_body = _http_post_json(
        url,
        {
            "tenant_id": tenant_id,
            "object_key": object_key,
            "output_format": output_format,
        },
        timeout_sec=120,
    )

    qa_file_key = _extract_qa_file_key(knowledge_body)

    return {
        "ok": True,
        "tenant_id": tenant_id,
        "contract_id": tenant_id,  # 互換（画面表示用）
        "object_key": object_key,
        "output_format": output_format,
        "knowledge": knowledge_body,
        # UIが「結果をダウンロード」で使うべきキー
        "qa_file_object_key": qa_file_key,
    }
