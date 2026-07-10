"""Customer email — compose & send a branded email, reusable templates, log.

Mirrors the SMS module: manual compose/send from a customer or lead, saved
templates with {{merge}} tags, and an audit log. Sends go through Resend
(app.services.customer_email). Sales agents may only email their own
customers/leads (same ownership rule as the rest of the CRM).
"""
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query

from app.db.client import get_client
from app.auth.deps import get_current_user, require_admin, UserContext
from app.auth.ownership import assert_customer_access, assert_lead_access
from app.services.audit import audit
from app.services.customer_email import (
    send_email, compose_email_html, render_email_body,
)

router = APIRouter()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Send ──────────────────────────────────────────────────────────────────────

@router.post("/send")
def send_customer_email(data: dict = Body(...), user: UserContext = Depends(get_current_user)):
    to = str(data.get("to") or "").strip()
    subject = str(data.get("subject") or "").strip()
    body = str(data.get("body") or "").strip()
    if not to or "@" not in to:
        raise HTTPException(status_code=400, detail="A valid recipient email is required.")
    if not subject:
        raise HTTPException(status_code=400, detail="Subject is required.")
    if not body:
        raise HTTPException(status_code=400, detail="Message is required.")

    db = get_client()
    lead_id = data.get("lead_id") or None
    customer_id = data.get("customer_id") or None
    # Enforce record ownership before sending on a contact's behalf.
    if customer_id:
        assert_customer_access(db, user, customer_id)
    if lead_id:
        assert_lead_access(db, user, lead_id)

    rendered_body = render_email_body(body, data.get("variables") or {})
    rendered_subject = render_email_body(subject, data.get("variables") or {})
    html = compose_email_html(rendered_body)

    result = send_email(to, rendered_subject, html)

    db.table("email_messages").insert({
        "user_id":            user.user_id,
        "lead_id":            lead_id,
        "customer_id":        customer_id,
        "deal_id":            data.get("deal_id") or None,
        "to_email":           to,
        "subject":            rendered_subject,
        "body":               html,
        "status":             "sent" if result.get("ok") else "failed",
        "provider_message_id": result.get("id"),
        "error":              None if result.get("ok") else result.get("error"),
    }).execute()

    audit(db, "email_messages", customer_id or lead_id or to, "sent_email", None,
          {"to": to, "subject": rendered_subject, "ok": result.get("ok")},
          reason="Customer email sent from CRM", actor=user.email or "staff")

    if not result.get("ok"):
        raise HTTPException(status_code=502, detail=result.get("error") or "Email failed to send.")
    return {"ok": True, "id": result.get("id")}


# ── Logs ──────────────────────────────────────────────────────────────────────

@router.get("/logs")
def email_logs(
    lead_id:     Optional[str] = Query(None),
    customer_id: Optional[str] = Query(None),
    deal_id:     Optional[str] = Query(None),
    limit:       int           = Query(50),
    offset:      int           = Query(0),
    user: UserContext = Depends(get_current_user),
):
    db = get_client()
    q = db.table("email_messages").select(
        "id, to_email, subject, status, error, created_at, lead_id, customer_id, deal_id")
    if lead_id:
        q = q.eq("lead_id", lead_id)
    if customer_id:
        q = q.eq("customer_id", customer_id)
    if deal_id:
        q = q.eq("deal_id", deal_id)
    res = q.order("created_at", desc=True).range(offset, offset + limit - 1).execute()
    return res.data or []


# ── Templates ─────────────────────────────────────────────────────────────────

@router.get("/templates")
def list_email_templates(user: UserContext = Depends(get_current_user)):
    db = get_client()
    res = db.table("email_templates").select("*").order("name").execute()
    return res.data or []


@router.post("/templates")
def create_email_template(data: dict = Body(...), user: UserContext = Depends(require_admin)):
    name = str(data.get("name") or "").strip()
    subject = str(data.get("subject") or "").strip()
    body = str(data.get("body") or "").strip()
    if not name or not subject or not body:
        raise HTTPException(status_code=400, detail="name, subject, and body are required.")
    db = get_client()
    res = db.table("email_templates").insert({
        "name":        name,
        "subject":     subject,
        "body":        body,
        "description": str(data.get("description") or "").strip() or None,
        "is_active":   bool(data.get("is_active", True)),
    }).execute()
    if not res.data:
        raise HTTPException(status_code=500, detail="Failed to create template.")
    return res.data[0]


@router.patch("/templates/{template_id}")
def update_email_template(template_id: str, data: dict = Body(...),
                          user: UserContext = Depends(require_admin)):
    db = get_client()
    allowed = {"name", "subject", "body", "description", "is_active"}
    payload = {k: v for k, v in data.items() if k in allowed}
    if not payload:
        raise HTTPException(status_code=400, detail="No valid fields to update.")
    payload["updated_at"] = _now()
    res = db.table("email_templates").update(payload).eq("id", template_id).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Template not found.")
    return res.data[0]


@router.delete("/templates/{template_id}")
def delete_email_template(template_id: str, user: UserContext = Depends(require_admin)):
    db = get_client()
    db.table("email_templates").delete().eq("id", template_id).execute()
    return {"ok": True}


# ── Preview ───────────────────────────────────────────────────────────────────

@router.post("/preview")
def preview_email(data: dict = Body(...), user: UserContext = Depends(get_current_user)):
    variables = data.get("variables") or {}
    subject = render_email_body(str(data.get("subject") or ""), variables)
    body = render_email_body(str(data.get("body") or ""), variables)
    return {"subject": subject, "html": compose_email_html(body)}
