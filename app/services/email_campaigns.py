"""Bulk email campaign engine — auto-drip within the Resend plan's daily cap.

A campaign is a saved message + a frozen recipient list (each with its own
personalization snapshot). `process_campaigns()` runs on a schedule and from a
background task right after creation; each run sends as many pending recipients
as the daily allowance permits, then stops. Nothing is dropped — leftover
recipients simply go out on the next run/day until the campaign completes.

The daily cap (EMAIL_DAILY_CAP, default 100 = Resend free tier) counts EVERY
'sent' email_messages row today, so one-off sends and campaign sends share the
same budget and we never exceed the provider limit.
"""
import os
from datetime import datetime, timezone

from app.db.client import get_client
from app.services.customer_email import send_email, compose_email_html, render_email_body

try:
    from zoneinfo import ZoneInfo
    _CENTRAL = ZoneInfo("America/Chicago")
except Exception:  # pragma: no cover - zoneinfo always present on 3.9+
    _CENTRAL = timezone.utc

DAILY_CAP = int(os.environ.get("EMAIL_DAILY_CAP", "100"))   # Resend free tier
PER_RUN   = int(os.environ.get("EMAIL_CAMPAIGN_PER_RUN", "40"))  # smooth bursts


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _today_start_utc_iso() -> str:
    """Midnight *Central* today, expressed in UTC — the window for 'sent today'."""
    now = datetime.now(_CENTRAL)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return start.astimezone(timezone.utc).isoformat()


def sent_today(db) -> int:
    res = db.table("email_messages").select("id", count="exact") \
        .eq("status", "sent").gte("created_at", _today_start_utc_iso()).execute()
    return res.count or 0


def _count(db, campaign_id: str, status: str) -> int:
    return db.table("email_campaign_recipients").select("id", count="exact") \
        .eq("campaign_id", campaign_id).eq("status", status).execute().count or 0


def _refresh_campaign(db, campaign_id: str) -> None:
    sent = _count(db, campaign_id, "sent")
    failed = _count(db, campaign_id, "failed")
    pending = _count(db, campaign_id, "pending")
    upd = {"sent_count": sent, "failed_count": failed,
           "last_run_at": _now_iso(), "updated_at": _now_iso()}
    if pending == 0:
        upd["status"] = "completed"
        upd["completed_at"] = _now_iso()
    db.table("email_campaigns").update(upd).eq("id", campaign_id).execute()


def process_campaigns(max_send: int = None) -> dict:
    """Send one drip: up to the remaining daily allowance across active campaigns."""
    db = get_client()
    allowance = max(0, DAILY_CAP - sent_today(db))
    per_run = PER_RUN if max_send is None else max_send
    budget = min(allowance, per_run)
    if budget <= 0:
        return {"sent": 0, "budget": 0, "reason": "daily cap reached" if allowance <= 0 else "no budget"}

    sent_total = 0
    campaigns = db.table("email_campaigns").select("*") \
        .eq("status", "sending").order("created_at").execute().data or []

    for camp in campaigns:
        if budget <= 0:
            break
        recips = db.table("email_campaign_recipients").select("*") \
            .eq("campaign_id", camp["id"]).eq("status", "pending") \
            .order("created_at").limit(budget).execute().data or []
        if not recips:
            _refresh_campaign(db, camp["id"])   # nothing left → mark completed
            continue
        for r in recips:
            if budget <= 0:
                break
            v = r.get("variables") or {}
            subject = render_email_body(camp["subject"], v)
            body = render_email_body(camp["body"], v)
            html = compose_email_html(body)
            result = send_email(r["to_email"], subject, html)
            ok = bool(result.get("ok"))
            db.table("email_campaign_recipients").update({
                "status": "sent" if ok else "failed",
                "provider_message_id": result.get("id"),
                "error": None if ok else result.get("error"),
                "sent_at": _now_iso(),
            }).eq("id", r["id"]).execute()
            db.table("email_messages").insert({
                "user_id": camp.get("created_by"),
                "lead_id": r.get("lead_id"),
                "customer_id": r.get("customer_id"),
                "campaign_id": camp["id"],
                "to_email": r["to_email"],
                "subject": subject,
                "body": html,
                "status": "sent" if ok else "failed",
                "provider_message_id": result.get("id"),
                "error": None if ok else result.get("error"),
            }).execute()
            budget -= 1
            sent_total += 1
        _refresh_campaign(db, camp["id"])

    return {"sent": sent_total, "budget_remaining": budget}
