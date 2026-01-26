import os
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

from app.deps.auth import require_user
from app.deps.db import get_db
from app.services.contracts_acl import require_contract_admin

router = APIRouter()

class InviteCreateIn(BaseModel):
    contract_id: str
    email: str

@router.post("/v1/invites")
def create_invite(
    payload: InviteCreateIn,
    conn=Depends(get_db),
    current_user=Depends(require_admin),
):
    token = uuid.uuid4().hex
    invite_url = f"{APP_BASE_URL}/invite.html?token={token}"

    with conn.cursor() as cur:
        # users が無ければ作る（保険）
        cur.execute(
            """
            INSERT INTO users (user_id, email, created_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (email) DO NOTHING
            """,
            (uuid.uuid4().hex, payload.email),
        )

        # user_contracts が無ければ作る（保険）
        cur.execute(
            """
            INSERT INTO user_contracts (user_id, contract_id, role, status)
            SELECT u.user_id, %s, 'member', 'invited'
            FROM users u
            WHERE u.email = %s
            ON CONFLICT DO NOTHING
            """,
            (payload.contract_id, payload.email),
        )

        cur.execute(
            """
            INSERT INTO invites (contract_id, email, token)
            VALUES (%s, %s, %s)
            RETURNING invite_id, created_at
            """,
            (payload.contract_id, payload.email, token),
        )
        row = cur.fetchone()
    conn.commit()

    invite_id, created_at = row

    sg_key = os.environ.get("SENDGRID_API_KEY")
    if not sg_key:
        raise HTTPException(status_code=500, detail="SENDGRID_API_KEY not set")

    subject = "招待メール：アカウント登録"
    body = (
        "次のリンクから登録してください。\n\n"
        f"{invite_url}\n\n"
        "このメールに心当たりがない場合は破棄してください。\n"
    )

    message = Mail(
        from_email=FROM_EMAIL,
        to_emails=payload.email,
        subject=subject,
        plain_text_content=body,
    )

    try:
        sg = SendGridAPIClient(sg_key)
        res = sg.send(message)
        if res.status_code not in (200, 201, 202):
            raise HTTPException(status_code=502, detail=f"SendGrid error: {res.status_code}")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"SendGrid send failed: {str(e)}")

    return {
        "ok": True,
        "invite_id": str(invite_id),
        "created_at": created_at.isoformat(),
        "email": payload.email,
        "contract_id": payload.contract_id,
    }

class InviteConsumeIn(BaseModel):
    token: str

@router.post("/v1/invites/consume")
def consume_invite(
    payload: InviteConsumeIn,
    user=Depends(require_user),
    conn=Depends(get_db),
):
    # ログインは必須（匿名では実行させない）
    if not user or not user.get("uid"):
        raise HTTPException(status_code=401, detail="not signed in")

    with conn.cursor() as cur:
        # 1) token から invite を取得
        cur.execute(
            """
            SELECT invite_id, contract_id, email
            FROM invites
            WHERE token = %s
            LIMIT 1
            """,
            (payload.token,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="invalid invite token")

        invite_id, contract_id, invited_email = row

        # 2) ※ email mismatch チェックはしない（運用で吸収）

        # 3) 事前作成済みの user_contracts を active にするだけ
        cur.execute(
            """
            UPDATE user_contracts uc
            SET status = 'active'
            FROM users u
            WHERE u.user_id = uc.user_id
              AND u.email = %s
              AND uc.contract_id = %s
            """,
            (invited_email, contract_id),
        )

        if cur.rowcount == 0:
            raise HTTPException(
                status_code=409,
                detail="user_contracts not precreated for this email/contract",
            )

        # 4) token を無効化（再利用防止）
        cur.execute(
            """
            UPDATE invites
            SET token = NULL
            WHERE invite_id = %s AND token = %s
            """,
            (invite_id, payload.token),
        )

    conn.commit()
    return {"ok": True, "contract_id": str(contract_id), "role": "member"}


MAX_SAMPLE_BYTES_DEFAULT = 200_000  # 200KBだけ読んで判定（十分）

# -------------------------
# Request/Response schema
# -------------------------

