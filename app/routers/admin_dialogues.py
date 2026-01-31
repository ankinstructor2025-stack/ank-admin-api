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

# 既存の auth/guard に合わせる（ここはプロジェクト側の実装に依存）
from app.deps.auth import require_user


router = APIRouter()


def _json_response(resp) -> dict:
    """
    urllib のレスポンスを JSON として読む（dict前提）
    """
    raw = resp.read()
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        # JSONでない場合は文字列で返す
        return {"_raw": raw.decode("utf-8", errors="replace")}


def _http_post_json(url: str, payload: dict, timeout_sec: int = 60) -> dict:
    """
    knowledge 側に JSON を POST して、JSON を返す
    """
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            return _json_response(resp)
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
        raise HTTPException(status_code=502, detail=f"failed to call knowledge: {e.code} {body}")
    except urllib.error.URLError as e:
        raise HTTPException(status_code=502, detail=f"failed to call knowledge: {e}")


def _extract_qa_file_key(knowledge_body: dict) -> str | None:
    """
    knowledge 側の返却から qa ファイルの object_key を抽出する（揺れに耐える）
    """
    if not isinstance(knowledge_body, dict):
        return None

    # 直下
    for k in ("qa_file_object_key", "qa_file_key", "qa_object_key", "file_object_key", "object_key"):
        v = knowledge_body.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()

    # data配下
    data = knowledge_body.get("data")
    if isinstance(data, dict):
        for k in ("qa_file_object_key", "qa_file_key", "qa_object_key", "file_object_key", "object_key"):
            v = data.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()

    return None


@router.get("/v1/admin/dialogues")
def list_dialogues(
    tenant_id: str = Query(...),
    user=Depends(require_user),
):
    """
    旧：DB前提の一覧
    新方針：Cloud SQLを使わないため無効化
    """
    raise HTTPException(status_code=501, detail="not implemented (cloud sql disabled)")


@router.post("/v1/admin/dialogues/activate")
def activate_dialogue(
    tenant_id: str = Query(...),
    dialogue_id: str = Query(...),
    user=Depends(require_user),
):
    """
    旧：DB前提の activate
    新方針：Cloud SQLを使わないため無効化
    """
    raise HTTPException(status_code=501, detail="not implemented (cloud sql disabled)")


@router.post("/v1/qa/build")
def build_qa_file(
    body: dict,
    user=Depends(require_user),
):
    """
    UI → admin → knowledge の中継

    入力例：
      {
        "tenant_id": "ten_xxx",
        "object_key": "tenants/.../uploads/..",
        "output_format": "csv"
      }

    互換：
      tenant_id の代わりに contract_id を受けてもよい（UI都合）
    """
    knowledge_base = (os.getenv("KNOWLEDGE_API_BASE_URL") or "").strip()
    if not knowledge_base:
        raise HTTPException(status_code=500, detail="KNOWLEDGE_API_BASE_URL not set")

    tenant_id = (body.get("tenant_id") or body.get("contract_id") or "").strip()
    if not tenant_id:
        raise HTTPException(status_code=400, detail="tenant_id required")

    object_key = (body.get("object_key") or "").strip()
    if not object_key:
        raise HTTPException(status_code=400, detail="object_key required")

    output_format = (body.get("output_format") or "csv").strip().lower()
    if output_format not in ("csv", "json", "jsonl"):
        raise HTTPException(status_code=400, detail="output_format must be csv/json/jsonl")

    payload = {
        "tenant_id": tenant_id,
        "object_key": object_key,
        "output_format": output_format,
    }

    # knowledge 側に中継
    url = knowledge_base.rstrip("/") + "/v1/qa/build"
    knowledge_body = _http_post_json(url, payload, timeout_sec=120)

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

# --- added endpoints (non-DB / thin proxy) -----------------------------------

def _get_knowledge_base_url() -> str:
    """
    admin -> knowledge の中継先。
    Cloud Run の環境変数 KNOWLEDGE_API_BASE_URL に設定する。
    例: https://ank-knowledge-api-xxxx.asia-northeast1.run.app
    """
    base = (os.getenv("KNOWLEDGE_API_BASE_URL") or "").strip()
    if not base:
        raise HTTPException(status_code=500, detail="KNOWLEDGE_API_BASE_URL not set")
    return base.rstrip("/")


@router.post("/v1/qa/generate-file")
def qa_generate_file(
    body: dict,
    user=Depends(require_user),
):
    """
    UI → admin → knowledge の中継（generate-file）

    入力例：
      {
        "contract_id": "con_xxx",
        "object_key": "tenants/.../uploads/..",
        "format": "json"
      }

    備考：
    - この admin 側は Cloud SQL を使わない前提のため、ここでは DB による権限確認をしない。
    - 入口制御（ログイン必須）は require_user で行う。
    - contract_id は、そのまま knowledge 側へ渡す（knowledge 側で必要に応じて検証する）。
    """
    contract_id = (body.get("contract_id") or body.get("tenant_id") or "").strip()
    object_key = (body.get("object_key") or "").strip()
    fmt = (body.get("format") or body.get("output_format") or "json").strip().lower()

    if not contract_id:
        raise HTTPException(status_code=400, detail="contract_id required")
    if not object_key:
        raise HTTPException(status_code=400, detail="object_key required")
    if fmt not in ("csv", "json", "jsonl"):
        raise HTTPException(status_code=400, detail="format must be csv/json/jsonl")

    knowledge_base = _get_knowledge_base_url()
    url = knowledge_base + "/v1/qa/generate-file"

    payload = {
        "contract_id": contract_id,
        "object_key": object_key,
        "format": fmt,
    }

    knowledge_body = _http_post_json(url, payload, timeout_sec=180)

    # UIが知りたいキーを拾っておく（返りが揺れても耐える）
    qa_file_key = _extract_qa_file_key(knowledge_body)

    return {
        "ok": True,
        "contract_id": contract_id,
        "object_key": object_key,
        "format": fmt,
        "knowledge": knowledge_body,
        "qa_file_object_key": qa_file_key,
    }
