"""Enrollment system.

Public (no auth): customers submit enrollments from the website and check
their status. Admin: work the queue, configure per-provider API
integrations, and dispatch/re-dispatch submissions.
"""
import os
import re
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

from app.db.client import get_client
from app.auth.deps import require_admin, get_current_user, UserContext
from app.services.audit import audit
from app.services.enrollment_dispatch import dispatch_enrollment, build_request
from app.services.file_parser.provider_parsers import normalize_esiid, is_valid_esiid

router = APIRouter()

STATUSES = ["submitted", "needs_review", "sent_to_provider", "accepted",
            "rejected", "cancelled", "active"]


# ── Public: submit + status ───────────────────────────────────────────────────

class PublicEnrollment(BaseModel):
    first_name: str = Field(min_length=1, max_length=100)
    last_name: str = Field(min_length=1, max_length=100)
    email: Optional[str] = Field(default=None, max_length=200)
    phone: str = Field(min_length=7, max_length=30)
    language: Optional[str] = None

    service_address: str = Field(min_length=3, max_length=300)
    service_city: str = Field(min_length=2, max_length=100)
    service_state: str = "TX"
    service_zip: str = Field(min_length=5, max_length=10)
    esiid: Optional[str] = None
    enrollment_type: str = "switch"          # switch | move_in
    requested_start_date: Optional[str] = None

    plan_id: Optional[int] = None
    plan_name: Optional[str] = None
    provider: Optional[str] = None
    rate: Optional[float] = None
    term_months: Optional[int] = None

    terms_accepted: bool = False
    company_website: Optional[str] = None    # honeypot — must stay empty


@router.post("/public")
def submit_public_enrollment(body: PublicEnrollment, request: Request):
    if body.company_website:                  # bot filled the honeypot
        return {"ok": True, "reference": "SGP-000000"}
    if not body.terms_accepted:
        raise HTTPException(status_code=400, detail="Please accept the terms to enroll.")
    if not re.sub(r"\D", "", body.phone):
        raise HTTPException(status_code=400, detail="A valid phone number is required.")

    db = get_client()

    esiid = normalize_esiid(body.esiid) if body.esiid else None
    if esiid and not is_valid_esiid(esiid):
        raise HTTPException(status_code=400,
                            detail="That ESI ID doesn't look right — it's 17 or 22 digits, on your electric bill. You can also leave it blank.")

    # resolve plan details from landing_plans when a plan_id is given
    plan = None
    if body.plan_id is not None:
        p = db.table("landing_plans").select("*").eq("id", body.plan_id).limit(1).execute().data
        plan = p[0] if p else None

    now = datetime.now(timezone.utc).isoformat()
    client_ip = (request.headers.get("x-forwarded-for") or
                 (request.client.host if request.client else ""))\
        .split(",")[0].strip()[:60]

    # very light abuse guard: max 15 public submissions per hour system-wide burst per IP
    recent = db.table("enrollments").select("id", count="exact") \
        .eq("client_ip", client_ip).gte("created_at",
            datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0).isoformat()) \
        .limit(1).execute().count or 0
    if recent >= 15:
        raise HTTPException(status_code=429, detail="Too many submissions — please call us instead.")

    record = {
        "status": "submitted", "source": "web",
        "first_name": body.first_name.strip(), "last_name": body.last_name.strip(),
        "email": (body.email or "").strip() or None, "phone": body.phone.strip(),
        "language": body.language,
        "service_address": body.service_address.strip(),
        "service_city": body.service_city.strip(),
        "service_state": (body.service_state or "TX").strip()[:2].upper(),
        "service_zip": re.sub(r"\D", "", body.service_zip)[:10],
        "esiid": esiid,
        "enrollment_type": body.enrollment_type if body.enrollment_type in ("switch", "move_in") else "switch",
        "requested_start_date": body.requested_start_date,
        "plan_id": body.plan_id,
        "plan_name": (plan or {}).get("plan_name") or body.plan_name,
        "provider": (plan or {}).get("provider") or body.provider,
        "rate": (plan or {}).get("rate") if plan else body.rate,
        "term_months": (plan or {}).get("term_months") if plan else body.term_months,
        "terms_accepted_at": now, "client_ip": client_ip,
    }
    enr = db.table("enrollments").insert(record).execute().data[0]

    # mirror into the CRM pipeline: lead + Future deal
    lead_id = deal_id = None
    try:
        lead = db.table("leads").insert({
            "first_name": record["first_name"], "last_name": record["last_name"],
            "address": record["service_address"], "city": record["service_city"],
            "state": record["service_state"], "zip": record["service_zip"],
            "phone": record["phone"], "email": record["email"],
            "status": "lead", "sales_agent": "Website",
        }).execute().data[0]
        lead_id = lead["id"]
        deal = db.table("lead_deals").insert({
            "lead_id": lead_id, "status": "Future",
            "supplier": record["provider"], "plan_name": record["plan_name"],
            "rate": record["rate"],
            "contract_term": f"{record['term_months']} Months" if record.get("term_months") else None,
            "service_address": record["service_address"], "service_city": record["service_city"],
            "service_state": record["service_state"], "service_zip": record["service_zip"],
            "esiid": esiid, "sales_agent": "Website",
            "start_date": record["requested_start_date"],
        }).execute().data[0]
        deal_id = deal["id"]
        db.table("enrollments").update({"lead_id": lead_id, "deal_id": deal_id}).eq("id", enr["id"]).execute()
    except Exception:
        pass  # enrollment stands alone even if CRM mirroring hiccups

    audit(db, "enrollments", enr["id"], "public_submission", None,
          {"provider": record["provider"], "plan": record["plan_name"], "ip": client_ip},
          reason="Customer self-service enrollment", actor="public-web")

    # notify the office (best effort)
    try:
        import resend
        if not getattr(resend, "api_key", None):
            resend.api_key = os.environ.get("RESEND_API_KEY", "")
        if resend.api_key:
            resend.Emails.send({
                "from": os.environ.get("REMINDER_FROM_EMAIL", "reminders@saigonpower.com"),
                "to": [os.environ.get("ADMIN_ALERT_EMAIL", "lance112709@gmail.com")],
                "subject": f"⚡ New enrollment: {record['first_name']} {record['last_name']} — {record['plan_name'] or 'no plan'}",
                "html": f"<p><b>{record['first_name']} {record['last_name']}</b> ({record['phone']}) enrolled online.</p>"
                        f"<p>{record['service_address']}, {record['service_city']} {record['service_zip']}<br>"
                        f"Plan: {record['plan_name'] or '—'} · {record['provider'] or '—'} · "
                        f"{record['rate'] or '—'}¢ · {record['term_months'] or '—'} mo</p>"
                        f"<p>Open the CRM → Enrollments to review.</p>",
            })
    except Exception:
        pass

    # auto-dispatch when this provider has a live API integration
    dispatch = dispatch_enrollment(db, enr["id"], actor="auto-dispatch")

    return {
        "ok": True,
        "reference": f"SGP-{enr['id'][:8].upper()}",
        "id": enr["id"],
        "auto_submitted": bool(dispatch.get("success")) and dispatch.get("mode") == "rest",
    }


@router.get("/public/{id}/status")
def public_status(id: str, last_name: str = Query(...)):
    db = get_client()
    enr = db.table("enrollments").select("status,first_name,last_name,plan_name,provider,created_at,requested_start_date") \
        .eq("id", id).ilike("last_name", last_name.strip()).limit(1).execute().data
    if not enr:
        raise HTTPException(status_code=404, detail="Enrollment not found — check your reference and last name.")
    e = enr[0]
    FRIENDLY = {
        "submitted": "Received — our team is reviewing it",
        "needs_review": "In review by our team",
        "sent_to_provider": "Submitted to your electricity provider",
        "accepted": "Approved by the provider 🎉",
        "active": "Service is active 🎉",
        "rejected": "We hit a snag — we'll call you",
        "cancelled": "Cancelled",
    }
    return {"status": e["status"], "message": FRIENDLY.get(e["status"], e["status"]),
            "plan": e["plan_name"], "provider": e["provider"],
            "submitted_at": e["created_at"], "requested_start_date": e["requested_start_date"]}


# ── Admin: queue ──────────────────────────────────────────────────────────────

@router.get("")
def list_enrollments(
    status: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    limit: int = Query(100),
    offset: int = Query(0),
    user: UserContext = Depends(get_current_user),
):
    db = get_client()
    q = db.table("enrollments").select("*").order("created_at", desc=True)
    if status:
        q = q.eq("status", status)
    rows = q.range(offset, offset + limit - 1).execute().data or []
    if search:
        s = search.lower()
        rows = [r for r in rows if s in f"{r['first_name']} {r['last_name']}".lower()
                or s in (r.get("phone") or "") or s in (r.get("esiid") or "")
                or s in (r.get("service_address") or "").lower()]
    counts = {}
    for st in STATUSES:
        counts[st] = db.table("enrollments").select("id", count="exact").eq("status", st).limit(1).execute().count or 0
    return {"enrollments": rows, "counts": counts}


@router.patch("/{id}")
def update_enrollment(id: str, data: dict = Body(...), user: UserContext = Depends(require_admin)):
    db = get_client()
    allowed = {"status", "notes", "esiid", "requested_start_date", "provider_confirmation"}
    payload = {k: v for k, v in data.items() if k in allowed}
    if "status" in payload and payload["status"] not in STATUSES:
        raise HTTPException(status_code=400, detail=f"Invalid status. Use one of {STATUSES}")
    if "esiid" in payload and payload["esiid"]:
        payload["esiid"] = normalize_esiid(payload["esiid"])
    if not payload:
        raise HTTPException(status_code=400, detail="Nothing to update")
    payload["updated_at"] = datetime.now(timezone.utc).isoformat()
    old = db.table("enrollments").select("status,notes,esiid").eq("id", id).limit(1).execute().data
    res = db.table("enrollments").update(payload).eq("id", id).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Enrollment not found")
    audit(db, "enrollments", id, "update", old[0] if old else None, payload, actor=user.email or "admin")

    # when the enrollment goes active, flip its CRM deal live too
    if payload.get("status") == "active":
        enr = res.data[0]
        if enr.get("deal_id"):
            db.table("lead_deals").update({"status": "Active"}).eq("id", enr["deal_id"]).execute()
        if enr.get("lead_id"):
            db.table("leads").update({"status": "converted"}).eq("id", enr["lead_id"]).execute()
    return res.data[0]


@router.post("/{id}/dispatch")
def dispatch(id: str, force: bool = Query(False), user: UserContext = Depends(require_admin)):
    db = get_client()
    return dispatch_enrollment(db, id, actor=user.email or "admin", force=force)


# ── Admin: provider integrations ─────────────────────────────────────────────

@router.get("/integrations")
def list_integrations(user: UserContext = Depends(require_admin)):
    db = get_client()
    return db.table("provider_integrations").select("*").order("provider_name").execute().data or []


@router.put("/integrations/{provider_name}")
def upsert_integration(provider_name: str, data: dict = Body(...), user: UserContext = Depends(require_admin)):
    db = get_client()
    allowed = {"integration_type", "endpoint_url", "http_method", "auth_type",
               "auth_credentials", "extra_headers", "field_mapping", "is_active", "test_mode"}
    payload = {k: v for k, v in data.items() if k in allowed}
    payload["updated_at"] = datetime.now(timezone.utc).isoformat()
    existing = db.table("provider_integrations").select("id").eq("provider_name", provider_name).limit(1).execute().data
    if existing:
        res = db.table("provider_integrations").update(payload).eq("id", existing[0]["id"]).execute()
    else:
        res = db.table("provider_integrations").insert({"provider_name": provider_name, **payload}).execute()
    audit(db, "provider_integrations", provider_name, "upsert",
          None, {k: v for k, v in payload.items() if k != "auth_credentials"},
          actor=user.email or "admin")
    return res.data[0]


@router.post("/integrations/{provider_name}/preview")
def preview_integration(provider_name: str, user: UserContext = Depends(require_admin)):
    """Render the exact request that would be sent, using a sample enrollment
    (or the most recent real one for this provider). Nothing is transmitted."""
    db = get_client()
    integ = db.table("provider_integrations").select("*").eq("provider_name", provider_name).limit(1).execute().data
    if not integ:
        raise HTTPException(status_code=404, detail="Integration not configured yet — save it first.")
    sample = db.table("enrollments").select("*").ilike("provider", provider_name) \
        .order("created_at", desc=True).limit(1).execute().data
    enrollment = sample[0] if sample else {
        "id": "00000000-0000-0000-0000-000000000000",
        "first_name": "Test", "last_name": "Customer", "email": "test@example.com",
        "phone": "8325551234", "service_address": "123 Main St",
        "service_city": "Houston", "service_state": "TX", "service_zip": "77001",
        "esiid": "1008901000000000000000", "enrollment_type": "switch",
        "requested_start_date": "2026-08-01", "plan_name": "Sample Plan 12",
        "provider": provider_name, "rate": 12.5, "term_months": 12,
    }
    req = build_request(integ[0], enrollment)
    # never echo secrets back
    req["headers"] = {k: ("•••" if k.lower() in ("authorization",) or "key" in k.lower() else v)
                      for k, v in req["headers"].items()}
    return {"request": req, "used_sample": not bool(sample)}
