"""Bulk email campaigns — build an audience, then auto-drip it out.

The audience is defined the same two ways the Customers tab offers:
  • "selected"  → an explicit list of lead ids the user checked, or
  • "filter"    → everyone matching the current filters (provider, contract end
                  date, active/inactive, city/state/zip, last name, segment).

Recipients are resolved once at creation and frozen (email + personalization
snapshot), so later edits to a customer never change an in-flight campaign.
Sending is handled by app.services.email_campaigns (respects the daily cap).

Manager/admin only — bulk sending is a privileged action.
"""
import math
from datetime import datetime, timezone

from fastapi import APIRouter, Body, BackgroundTasks, Depends, HTTPException, Query

from app.db.client import get_client
from app.auth.deps import require_manager, UserContext
from app.services.audit import audit
from app.services.merge_vars import lead_merge_vars
from app.services.email_campaigns import process_campaigns, DAILY_CAP
from app.api.v1.leads import (
    collect_matching_customers, _build_customer_filters, _auto_promote_deals,
)

router = APIRouter()

_FILTER_KEYS = ["search", "provider", "end_from", "end_to", "status",
                "city", "state", "zip", "last_name", "segment"]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _valid_email(e) -> bool:
    e = (e or "").strip()
    return bool(e) and "@" in e


def _pending(c: dict) -> int:
    return max(0, (c.get("total_recipients") or 0) - (c.get("sent_count") or 0) - (c.get("failed_count") or 0))


def _resolve_recipients(db, data: dict) -> list:
    """Return [{lead_id, email, variables}] for the requested audience."""
    mode = data.get("mode") or ("selected" if data.get("lead_ids") else "filter")
    out = []
    if mode == "selected":
        ids = [x for x in (data.get("lead_ids") or []) if x]
        for i in range(0, len(ids), 200):
            chunk = ids[i:i + 200]
            rows = db.table("leads").select("*, lead_deals(*)").in_("id", chunk).execute().data or []
            for lead in rows:
                deals = _auto_promote_deals(db, lead.pop("lead_deals", []) or [])
                out.append({
                    "lead_id": lead["id"],
                    "email": (lead.get("email") or "").strip(),
                    "variables": lead_merge_vars(lead, deals),
                })
    else:
        raw = data.get("filters") or {}
        f = _build_customer_filters(*[raw.get(k) for k in _FILTER_KEYS])
        for m in collect_matching_customers(db, None, f):
            out.append({
                "lead_id": m["lead_id"],
                "email": (m.get("email") or "").strip(),
                "variables": m.get("variables") or {},
            })
    return out


@router.post("")
def create_campaign(background: BackgroundTasks, data: dict = Body(...),
                    user: UserContext = Depends(require_manager)):
    name = str(data.get("name") or "").strip()
    subject = str(data.get("subject") or "").strip()
    body = str(data.get("body") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Campaign name is required.")
    if not subject:
        raise HTTPException(status_code=400, detail="Subject is required.")
    if not body:
        raise HTTPException(status_code=400, detail="Message is required.")

    db = get_client()
    recips = _resolve_recipients(db, data)

    # Drop recipients with no email, and de-dupe by address so nobody is emailed twice.
    valid, skipped, seen = [], 0, set()
    for r in recips:
        if not _valid_email(r["email"]):
            skipped += 1
            continue
        key = r["email"].lower()
        if key in seen:
            continue
        seen.add(key)
        valid.append(r)

    if not valid:
        raise HTTPException(status_code=400,
                            detail="No recipients with a valid email match this selection.")

    daily_cap = data.get("daily_cap") or None
    mode = data.get("mode") or ("selected" if data.get("lead_ids") else "filter")
    campaign = db.table("email_campaigns").insert({
        "name": name,
        "subject": subject,
        "body": body,
        "status": "sending",
        "daily_cap": daily_cap,
        "audience": {"mode": mode, "filters": data.get("filters") or None,
                     "selected_count": len(data.get("lead_ids") or [])},
        "total_recipients": len(valid),
        "skipped_no_email": skipped,
        "created_by": user.user_id,
        "created_by_name": user.email or user.sales_agent_name or "staff",
    }).execute().data[0]

    rows = [{
        "campaign_id": campaign["id"],
        "lead_id": r["lead_id"],
        "to_email": r["email"],
        "variables": r["variables"],
        "status": "pending",
    } for r in valid]
    for i in range(0, len(rows), 500):
        db.table("email_campaign_recipients").insert(rows[i:i + 500]).execute()

    audit(db, "email_campaigns", campaign["id"], "created_campaign", None,
          {"name": name, "recipients": len(valid), "skipped": skipped},
          reason="Bulk email campaign created", actor=user.email or "staff")

    # Send the first batch right away (still bounded by the daily cap).
    background.add_task(process_campaigns)

    cap = daily_cap or DAILY_CAP
    return {
        "id": campaign["id"],
        "total_recipients": len(valid),
        "skipped_no_email": skipped,
        "daily_cap": cap,
        "est_days": max(1, math.ceil(len(valid) / cap)),
    }


@router.get("")
def list_campaigns(limit: int = Query(100), user: UserContext = Depends(require_manager)):
    db = get_client()
    rows = db.table("email_campaigns").select("*").order("created_at", desc=True) \
        .limit(limit).execute().data or []
    for c in rows:
        c["pending_count"] = _pending(c)
    return rows


@router.get("/{campaign_id}")
def get_campaign(campaign_id: str, user: UserContext = Depends(require_manager)):
    db = get_client()
    c = (db.table("email_campaigns").select("*").eq("id", campaign_id).limit(1).execute().data or [None])[0]
    if not c:
        raise HTTPException(status_code=404, detail="Campaign not found")
    c["pending_count"] = _pending(c)
    c["recent"] = db.table("email_campaign_recipients") \
        .select("to_email, status, error, sent_at").eq("campaign_id", campaign_id) \
        .order("sent_at", desc=True).limit(50).execute().data or []
    return c


def _set_status(db, campaign_id: str, new_status: str, allowed_from: set) -> dict:
    c = (db.table("email_campaigns").select("status").eq("id", campaign_id).limit(1).execute().data or [None])[0]
    if not c:
        raise HTTPException(status_code=404, detail="Campaign not found")
    if c["status"] not in allowed_from:
        raise HTTPException(status_code=400,
                            detail=f"Campaign is {c['status']}; cannot change to {new_status}.")
    db.table("email_campaigns").update({"status": new_status, "updated_at": _now_iso()}) \
        .eq("id", campaign_id).execute()
    return {"ok": True, "status": new_status}


@router.post("/{campaign_id}/pause")
def pause_campaign(campaign_id: str, background: BackgroundTasks,
                   user: UserContext = Depends(require_manager)):
    return _set_status(get_client(), campaign_id, "paused", {"sending"})


@router.post("/{campaign_id}/resume")
def resume_campaign(campaign_id: str, background: BackgroundTasks,
                    user: UserContext = Depends(require_manager)):
    res = _set_status(get_client(), campaign_id, "sending", {"paused"})
    background.add_task(process_campaigns)   # pick it back up immediately
    return res


@router.post("/{campaign_id}/cancel")
def cancel_campaign(campaign_id: str, user: UserContext = Depends(require_manager)):
    return _set_status(get_client(), campaign_id, "canceled", {"sending", "paused"})
