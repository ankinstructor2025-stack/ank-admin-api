# qa.py
#
# admin-api 側の「QAのみ（basic想定）」入口。
# /v1/qa/generate-file を受けて、knowledge-api に中継するだけの薄いルータ。
#
# 目的：
# - UI(qa_generate.js) から {contract_id, object_key, format} を受け取る
# - Firebase認証(require_user) で uid を取り、contract へのアクセス権(最低限 member) を確認
# - KNOWLEDGE_API_BASE_URL の /v1/qa/generate-file に POST して結果をそのまま返す
#
# 備考：
# - “30日で削除” は knowledge 側が GCS Lifecycle 等で実現する想定。
# - このファイルは endpoint を 1 本だけに絞っている（最小構成）。

from fastapi import APIRouter, Depends, HTTPException
import os
import json
import urllib.request
import urllib.error

from app.deps.auth import require_user

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


def _http_post_json(url: str, payload: dict, timeout_sec: int = 180) -> dict:
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
                # knowledgeがJSON以外を返した場合もUIログで見えるようにする
                return {"ok": False, "message": "knowledge returned non-json", "raw": body}

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


def _require_contract_member(uid: str, contract_id: str, conn) -> None:
    """
    最小のACL:
    user_contracts に (user_id, contract_id) が存在すれば member/admin として許可する。
    （role列があるなら、必要に応じてここで分岐してもいい）
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM user_contracts
            WHERE user_id = %s
              AND contract_id = %s
            LIMIT 1
            """,
            (uid, contract_id),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=403, detail="not allowed for this contract")


@router.post("/v1/qa/generate-file")
def qa_generate_file(
    payload: dict,
    user=Depends(require_user),
):
    """
    UIから {contract_id, object_key, format} を受け取り、knowledge の /v1/qa/generate-file に中継する。

    期待する payload:
    - contract_id: str (required)
    - object_key: str (required)  # uploadsのGCS object_key
    - format: str (optional)      # json / jsonl / csv

    返却：
    - knowledge が返した JSON をそのまま返す（download_url 方式でも直返し方式でもOK）
    """
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid in token")

    contract_id = (payload.get("contract_id") or "").strip()
    object_key = (payload.get("object_key") or "").strip()
    fmt = (payload.get("format") or "json").strip()

    if not contract_id:
        raise HTTPException(status_code=400, detail="contract_id is required")
    if not object_key:
        raise HTTPException(status_code=400, detail="object_key is required")

    # member以上で許可
    _require_contract_member(uid, contract_id, conn)

    # knowledgeへ中継
    base = _get_knowledge_base_url()
    url = f"{base}/v1/qa/generate-file"

    knowledge_body = _http_post_json(
        url,
        {"contract_id": contract_id, "object_key": object_key, "format": fmt},
        timeout_sec=180,
    )

    # そのまま返す（UIが ok/download_url 等を見る）
    return knowledge_body
