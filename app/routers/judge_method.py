import os
import re
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from google.cloud import storage

from app.deps.auth import require_user
from app.deps.db import get_db
from app.core.settings import BUCKET_NAME
from app.services.contracts_acl import require_contract_admin

router = APIRouter()

MAX_SAMPLE_BYTES_DEFAULT = 2_000_000

class JudgeMethodIn(BaseModel):
    contract_id: str = Field(..., min_length=1)
    object_key: Optional[str] = None
    sample_bytes: int = Field(default=MAX_SAMPLE_BYTES_DEFAULT, ge=10_000, le=2_000_000)

class JudgeMethodOut(BaseModel):
    can_extract_qa: bool
    method: Optional[str] = None  # "A".."F"
    confidence: float = 0.0
    reasons: List[str] = []
    stats: Dict[str, Any] = {}

# -------------------------
# Helpers
# -------------------------
def _gcs_read_head_text(object_key: str, limit_bytes: int) -> str:
    if not BUCKET_NAME:
        raise HTTPException(status_code=500, detail="BUCKET_NAME is not set")

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(object_key)

    # download first N bytes
    data = blob.download_as_bytes(start=0, end=limit_bytes - 1)
    # 文字化けしても落ちないように
    return data.decode("utf-8", errors="replace")

def _ext_from_key(object_key: str) -> str:
    name = (object_key or "").split("/")[-1]
    i = name.rfind(".")
    return name[i:].lower() if i >= 0 else ""

def _looks_like_json(text: str) -> bool:
    s = text.lstrip()
    return s.startswith("{") or s.startswith("[")

def _try_parse_json(text: str) -> Optional[dict]:
    try:
        obj = json.loads(text)
        # messages配列（ChatGPT系）っぽいか判定
        if isinstance(obj, dict):
            if isinstance(obj.get("messages"), list):
                return {"kind": "messages"}
            if isinstance(obj.get("items"), list) or isinstance(obj.get("rows"), list):
                return {"kind": "list_like"}
        if isinstance(obj, list):
            # role/content を持つ dict の列かもしれない
            if obj and isinstance(obj[0], dict) and ("role" in obj[0] or "content" in obj[0]):
                return {"kind": "role_list"}
        return {"kind": "json"}
    except Exception:
        return None

def _try_parse_csv(text: str) -> Optional[dict]:
    # 先頭数行だけで判定（コスト削減）
    head = "\n".join(text.splitlines()[:200])
    try:
        f = StringIO(head)
        reader = csv.reader(f)
        rows = [r for r in reader if r]
        if len(rows) < 2:
            return None
        header = [c.strip().lower() for c in rows[0]]
        # speaker/text or role/content のどちらか
        has_speaker_text = ("speaker" in header and ("text" in header or "message" in header or "content" in header))
        has_role_content = ("role" in header and ("content" in header or "text" in header or "message" in header))
        if has_speaker_text or has_role_content:
            return {"header": header}
        return None
    except Exception:
        return None

_SPEAKER_PATTERNS = [
    r"^\s*(user|assistant|system)\s*[:：]",
    r"^\s*(u|a|s)\s*[:：]",
    r"^\s*[^\s]{1,20}\s*[:：]\s+",     # 「山田: 〜」みたいな
]
_QA_PATTERNS = [
    r"^\s*Q\s*[:：]",
    r"^\s*A\s*[:：]",
    r"^\s*質問\s*[:：]",
    r"^\s*回答\s*[:：]",
]
_QUOTE_PATTERNS = [
    r"^\s*>",                          # メール引用
    r"^\s*From:\s",
    r"^\s*Sent:\s",
    r"^\s*Subject:\s",
]

def _count_matches(lines: List[str], patterns: List[str]) -> int:
    regs = [re.compile(p, re.IGNORECASE) for p in patterns]
    n = 0
    for ln in lines:
        for rg in regs:
            if rg.search(ln):
                n += 1
                break
    return n

def _judge_by_heuristics(object_key: str, text: str) -> JudgeMethodOut:
    ext = _ext_from_key(object_key)
    lines = text.splitlines()
    nonempty = [ln for ln in lines if ln.strip()]
    stats = {
        "ext": ext,
        "sample_bytes": len(text.encode("utf-8", errors="ignore")),
        "lines": len(lines),
        "nonempty_lines": len(nonempty),
    }

    # 最低限：内容が薄すぎる
    if len(nonempty) < 10:
        return JudgeMethodOut(
            can_extract_qa=False,
            method=None,
            confidence=0.0,
            reasons=["内容が少なすぎます（行数が不足）"],
            stats=stats,
        )

    # JSON → 方式B
    if ext == ".json" or _looks_like_json(text):
        meta = _try_parse_json(text)
        if meta:
            return JudgeMethodOut(
                can_extract_qa=True,
                method="B",
                confidence=0.92,
                reasons=[f"JSONとして解析できる（{meta.get('kind')}）"],
                stats={**stats, "json_kind": meta.get("kind")},
            )

    # CSV → 方式C
    if ext == ".csv":
        meta = _try_parse_csv(text)
        if meta:
            return JudgeMethodOut(
                can_extract_qa=True,
                method="C",
                confidence=0.9,
                reasons=["CSVヘッダに speaker/text または role/content がある"],
                stats={**stats, "csv_header": meta.get("header")},
            )

    # ざっくりカウント
    speaker_markers = _count_matches(nonempty[:2000], _SPEAKER_PATTERNS)
    qa_markers = _count_matches(nonempty[:2000], _QA_PATTERNS)
    quote_markers = _count_matches(nonempty[:2000], _QUOTE_PATTERNS)

    stats.update({
        "speaker_markers": speaker_markers,
        "qa_markers": qa_markers,
        "quote_markers": quote_markers,
    })

    # 方式E（Q/A形式が強い）
    if qa_markers >= 6:
        return JudgeMethodOut(
            can_extract_qa=True,
            method="E",
            confidence=0.85,
            reasons=["Q/A 記法が一定数ある（Q:, A: など）"],
            stats=stats,
        )

    # 方式F（メール/チケット/引用っぽい）
    if quote_markers >= 6:
        return JudgeMethodOut(
            can_extract_qa=True,
            method="F",
            confidence=0.75,
            reasons=["引用/ヘッダ行が多い（メール/スレ形式の可能性）"],
            stats=stats,
        )

    # 方式A（話者ラベルがある対話）
    if speaker_markers >= 10:
        return JudgeMethodOut(
            can_extract_qa=True,
            method="A",
            confidence=0.78,
            reasons=["話者ラベルが複数回出現（User:, Assistant: など）"],
            stats=stats,
        )

    # 方式D（単一文書）
    # 対話でもQ/Aでもないが、文章量があるなら“生成寄りQA”は可能
    if len(nonempty) >= 30:
        return JudgeMethodOut(
            can_extract_qa=True,
            method="D",
            confidence=0.6,
            reasons=["対話/QA形式は弱いが、文章量があるため単一文書としてQA化（生成寄り）が可能"],
            stats=stats,
        )

    return JudgeMethodOut(
        can_extract_qa=False,
        method=None,
        confidence=0.0,
        reasons=["形式が判定できません（対話/QA/文書の特徴が弱い）"],
        stats=stats,
    )

# -------------------------
# Endpoint
# -------------------------
@router.post("/v1/admin/dialogues/judge-method", response_model=JudgeMethodOut)
def judge_method(
    req: JudgeMethodIn,
    user=Depends(require_user),
    conn=Depends(get_db),
):
    # 1) 認証（既存通り）
    uid = (user.get("uid") or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="no uid in token")

    contract_id = req.contract_id.strip()
    if not contract_id:
        raise HTTPException(status_code=400, detail="contract_id required")

    # 1.5) 契約 admin チェック（/v1/admin 配下は統一）
    require_contract_admin(uid, contract_id, conn)

    # 2) object_key を決める
    object_key = (req.object_key or "").strip()

    # 省略時：DBから active_dialogue_object_key を引く想定（要:あなたのテーブルに合わせて調整）
    if not object_key:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT active_dialogue_object_key FROM contracts WHERE contract_id=%s",
                (contract_id,),
            )
            row = cur.fetchone()
        object_key = (row[0] if row and row[0] else "") or ""
        if not object_key:
            raise HTTPException(status_code=400, detail="active dialogue not set (object_key required)")

    # 3) GCSからサンプルを読む（先頭だけ）
    try:
        text = _gcs_read_head_text(object_key, req.sample_bytes)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"failed to read gcs object: {e}")

    # 4) 方式判定
    result = _judge_by_heuristics(object_key, text)

    # 5) 返す
    return result
