import os
import json
from typing import Optional, List, Dict, Any, Tuple

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from google.cloud import storage

from app.deps.auth import require_user
from app.core.settings import BUCKET_NAME
from app.core.openai_client import call_openai_json  # ← 既存の OpenAI JSON 呼び出し想定

router = APIRouter()

# =====================
# 判定用定数
# =====================
JUDGE_BYTES = 4_000  # 4KB

# =====================
# Input / Output
# =====================
class JudgeMethodIn(BaseModel):
    tenant_id: Optional[str] = None
    contract_id: Optional[str] = None
    object_key: str


class JudgeMethodOut(BaseModel):
    can_extract_qa: bool
    method: Optional[str] = None
    confidence: float = 0.0
    reasons: List[str] = []
    stats: Dict[str, Any] = {}


# =====================
# Utils
# =====================
def _resolve_tenant_id(req: JudgeMethodIn) -> str:
    if req.tenant_id:
        return req.tenant_id
    if req.contract_id:
        return req.contract_id
    raise HTTPException(status_code=400, detail="tenant_id (or contract_id) required")


def _gcs_read_for_judge(object_key: str) -> Tuple[str, int, int]:
    """
    判定用テキストを取得
    - 4KB以上: 先頭4KB
    - 4KB未満: 全文
    """
    if not BUCKET_NAME:
        raise HTTPException(status_code=500, detail="BUCKET_NAME is not set")

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(object_key)

    if not blob.exists():
        raise HTTPException(status_code=404, detail="object not found in GCS")

    total_bytes = blob.size or 0

    if total_bytes <= JUDGE_BYTES:
        data = blob.download_as_bytes()
        used_bytes = total_bytes
    else:
        data = blob.download_as_bytes(start=0, end=JUDGE_BYTES - 1)
        used_bytes = JUDGE_BYTES

    text = data.decode("utf-8", errors="replace")
    return text, used_bytes, total_bytes


# =====================
# OpenAI: 素材判定
# =====================
def _judge_material_by_openai(text: str) -> Dict[str, Any]:
    """
    QA素材として適切かどうかのみ判定する
    """
    prompt = f"""
次のテキストが「QA（質問と回答）」を作るための素材として適切か判定してください。

不適切な例:
- プログラムコード
- 設定ファイル
- 辞書データ
- プロンプト
- ルール定義

適切な例:
- 試験問題
- FAQ
- 議事録
- 説明文
- 対話ログ

出力は JSON のみ:
{{
  "can_extract_qa": true | false,
  "reason": "短い理由（日本語）",
  "confidence": 0.0
}}

テキスト:
<<<
{text}
>>>
""".strip()

    return call_openai_json(prompt)


# =====================
# OpenAI: 方式判定（暫定 D 固定でもOK）
# =====================
def _judge_method_by_openai(text: str) -> Dict[str, Any]:
    """
    素材OKの前提で、方式を判断
    """
    prompt = f"""
次のテキストからQAを作成する場合、最も適切な方式を判定してください。

出力は JSON のみ:
{{
  "method": "A" | "B" | "C" | "D",
  "confidence": 0.0,
  "reason": "短い理由（日本語）"
}}

テキスト:
<<<
{text}
>>>
""".strip()

    return call_openai_json(prompt)


# =====================
# API
# =====================
@router.post(
    "/v1/admin/dialogues/judge-method",
    response_model=JudgeMethodOut,
)
def judge_method(
    req: JudgeMethodIn,
    user=Depends(require_user),
):
    """
    判定フロー:
    1. 素材判定（4KB / 全文）
    2. OK の場合のみ方式判定
    """
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid")

    _ = _resolve_tenant_id(req)  # 将来ACL用。今は未使用

    text, used_bytes, total_bytes = _gcs_read_for_judge(req.object_key)

    # --- ① 素材判定 ---
    material = _judge_material_by_openai(text)

    if not material.get("can_extract_qa"):
        return JudgeMethodOut(
            can_extract_qa=False,
            method=None,
            confidence=material.get("confidence", 0.0),
            reasons=[material.get("reason", "QA生成に適さない内容です")],
            stats={
                "stage": "material_judge",
                "used_bytes": used_bytes,
                "total_bytes": total_bytes,
            },
        )

    # --- ② 方式判定 ---
    method_judge = _judge_method_by_openai(text)

    return JudgeMethodOut(
        can_extract_qa=True,
        method=method_judge.get("method", "D"),
        confidence=method_judge.get("confidence", 0.5),
        reasons=[method_judge.get("reason", "QA生成が可能です")],
        stats={
            "stage": "method_judge",
            "used_bytes": used_bytes,
            "total_bytes": total_bytes,
        },
    )
