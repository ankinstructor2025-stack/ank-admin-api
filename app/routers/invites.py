import os
import uuid

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

from app.deps.auth import require_user
from app.deps.db import get_db
from app.services.contracts_acl import require_contract_admin
from app.core.settings import APP_BASE_URL, FROM_EMAIL

router = APIRouter()

class InviteCreateIn(BaseModel):
    contract_id: str
    email: str

@router.post("/v1/invites")
def create_invite(
    payload: InviteCreateIn,
    user=Depends(require_user),
    conn=Depends(get_db),
):
    uid = user["uid"]
    require_contract_admin(uid, payload.contract_id, conn)

    token = uuid.uuid4().hex
    invite_url = f"{APP_BASE_URL}/invite.html?token={token}"

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO invites (contract_id, email, token)
            VALUES (%s, %s, %s)
            RETURNING invite_id
            """,
            (payload.contract_id, payload.email, token),
        )
        invite_id = cur.fetchone()[0]
    conn.commit()

    sg_key = os.environ.get("SENDGRID_API_KEY")
    if not sg_key:
        raise HTTPException(status_code=500, detail="SENDGRID_API_KEY not set")

    msg = Mail(
        from_email=FROM_EMAIL,
        to_emails=payload.email,
        subject="招待メール",
        plain_text_content=f"以下のURLから登録してください。\n{invite_url}",
    )

    SendGridAPIClient(sg_key).send(msg)

    return {"ok": True, "invite_id": str(invite_id)}

class InviteConsumeIn(BaseModel):
    token: str

@router.post("/v1/invites/consume")
def consume_invite(
    payload: InviteConsumeIn,
    user=Depends(require_user),
    conn=Depends(get_db),
):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT invite_id FROM invites WHERE token=%s",
            (payload.token,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="invalid token")

        cur.execute(
            "UPDATE invites SET token=NULL WHERE invite_id=%s",
            (row[0],),
        )
    conn.commit()

    return {"ok": True}
