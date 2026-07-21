"""GiaDienRe website subscription intake + CRM module.

Public (no auth): the giadienre.com website posts subscriptions and
bill-analysis requests here. Every submission is deduped against
crm_customers by email/phone — the CRM stays the single source of truth.

Authed: list / detail / status / notes / assignment / stats / CSV export
for the "GiaDienRe Subscription" tab in the CRM.
"""
import csv
import io
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Body, Depends, Header, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.db.client import get_client
from app.auth.deps import get_current_user, require_admin, UserContext
from app.services.audit import audit
from app.services import helcim

router = APIRouter()

STATUSES = ["NEW", "CONTACTED", "ACTIVE", "CANCELLED"]
FORM_TYPES = ["signup", "bill_analysis"]
PLAN_NAMES = {
    # giadienre.com — current single plan ($9.99/mo)
    "plus": "Saigon Power Plus",
    # giadienre.com — legacy two-plan lineup (existing subscribers)
    "managed": "Saigon Power Managed",
    "managed-plus": "Saigon Power Managed Plus",
    # saigonllc.com (keys match packages/shared PLAN_CATALOG)
    "MONTHLY": "Saigon Membership",
    "ANNUAL": "Saigon Membership (Annual)",
    "FAMILY_MONTHLY": "Saigon Family Membership",
    "BUSINESS_MONTHLY": "Saigon Business Membership",
    # saigonpowertx.com/membership — SAIGON POWER PLUS
    "POWER_PLUS_RES": "SAIGON POWER PLUS — Residential",
    "POWER_PLUS_COM": "SAIGON POWER PLUS — Commercial",
}
LEAD_SOURCE = "GiaDienRe Website"            # default (giadienre.com)

# Each member website tags its subscribers with a lead_source so the CRM can
# show them in separate tabs (GiaDienRe Subscription vs SAIGON Subscription).
SITES = {
    "GiaDienRe Website": {"label": "GiaDienRe", "domain": "giadienre.com",
                          "tab": "GiaDienRe Subscription"},
    "SaigonLLC Website": {"label": "SAIGON", "domain": "saigonllc.com",
                          "tab": "SAIGON Subscription"},
    "SaigonPowerTX Website": {"label": "POWER PLUS", "domain": "saigonpowertx.com",
                              "tab": "POWER PLUS Membership"},
}


def _digits(s: Optional[str]) -> str:
    return re.sub(r"\D", "", s or "")


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ── Public: website intake ────────────────────────────────────────────────────

class PublicSubscription(BaseModel):
    full_name: str = Field(min_length=2, max_length=200)
    email: Optional[str] = Field(default=None, max_length=200)
    phone: Optional[str] = Field(default=None, max_length=30)

    service_address: Optional[str] = Field(default=None, max_length=300)
    city: Optional[str] = Field(default=None, max_length=100)
    state: Optional[str] = Field(default="TX", max_length=30)
    zip: Optional[str] = Field(default=None, max_length=10)
    utility_provider: Optional[str] = Field(default=None, max_length=120)
    current_provider: Optional[str] = Field(default=None, max_length=120)
    contract_end_date: Optional[str] = None

    plan_id: Optional[str] = None            # plus (current) | legacy tiers
    billing_cycle: Optional[str] = None      # monthly | annual
    password: Optional[str] = Field(default=None, min_length=8, max_length=128,
                                    repr=False)  # portal login (hashed, never stored raw)
    form_type: str = "signup"                # signup | bill_analysis
    lead_source: Optional[str] = None        # which website (see SITES)
    extra: Optional[dict] = None             # any additional website fields

    company_website: Optional[str] = None    # honeypot — must stay empty


def _find_crm_customer(db, email: str, phone_digits: str) -> Optional[dict]:
    """Dedupe against crm_customers by email first, then phone."""
    if email:
        rows = db.table("crm_customers").select("*").ilike("email", email) \
            .limit(1).execute().data
        if rows:
            return rows[0]
    if len(phone_digits) >= 7:
        d10 = phone_digits[-10:]
        variants = {
            phone_digits, d10,
            f"({d10[:3]}) {d10[3:6]}-{d10[6:]}" if len(d10) == 10 else d10,
            f"{d10[:3]}-{d10[3:6]}-{d10[6:]}" if len(d10) == 10 else d10,
            f"1{d10}" if len(d10) == 10 else d10,
        }
        ors = ",".join(f"phone.eq.{v}" for v in variants if v)
        rows = db.table("crm_customers").select("*").or_(ors).limit(1).execute().data
        if rows:
            return rows[0]
    return None


def _upsert_crm_customer(db, body: PublicSubscription, email: str,
                         phone_digits: str, source_label: str = "GiaDienRe") -> Optional[str]:
    """Create the CRM customer if missing, else fill blanks on the existing
    record. Returns the crm_customers id (None only if the write fails)."""
    existing = _find_crm_customer(db, email, phone_digits)
    name_parts = body.full_name.strip().split()
    first_name = name_parts[0] if name_parts else None
    last_name = " ".join(name_parts[1:]) or None

    if existing:
        updates = {}
        if email and not existing.get("email"):
            updates["email"] = email
        if body.phone and not existing.get("phone"):
            updates["phone"] = body.phone.strip()
        if body.service_address and not existing.get("mailing_address"):
            updates["mailing_address"] = body.service_address.strip()
        if body.city and not existing.get("city"):
            updates["city"] = body.city.strip()
        if body.zip and not existing.get("postal_code"):
            updates["postal_code"] = _digits(body.zip)[:10]
        if updates:
            updates["updated_at"] = _now().isoformat()
            db.table("crm_customers").update(updates).eq("id", existing["id"]).execute()
        return existing["id"]

    new_cust = {
        "full_name": body.full_name.strip(),
        "first_name": first_name,
        "last_name": last_name,
        "email": email or None,
        "phone": (body.phone or "").strip() or None,
        "mailing_address": (body.service_address or "").strip() or None,
        "city": (body.city or "").strip() or None,
        "state": (body.state or "TX").strip()[:2].upper(),
        "postal_code": _digits(body.zip)[:10] or None,
        "notes": source_label,   # source label shown as a badge in the CRM
        "created_by": f"Online signup ({source_label})",
    }
    res = db.table("crm_customers").insert(new_cust).execute()
    return res.data[0]["id"] if res.data else None


@router.post("/subscribe")
def submit_subscription(body: PublicSubscription, request: Request):
    if body.company_website:                 # bot filled the honeypot
        return {"ok": True, "reference": "GDR-000000"}

    email = (body.email or "").strip().lower()
    phone_digits = _digits(body.phone)
    if not email and len(phone_digits) < 7:
        raise HTTPException(status_code=400,
                            detail="An email address or phone number is required.")
    if body.form_type not in FORM_TYPES:
        raise HTTPException(status_code=400, detail="Invalid form type.")
    if body.plan_id and body.plan_id not in PLAN_NAMES:
        raise HTTPException(status_code=400, detail="Invalid plan.")
    if body.billing_cycle and body.billing_cycle not in ("monthly", "annual"):
        raise HTTPException(status_code=400, detail="Invalid billing cycle.")

    lead_source = body.lead_source if body.lead_source in SITES else LEAD_SOURCE
    site = SITES[lead_source]

    contract_end = None
    if body.contract_end_date:
        try:
            contract_end = datetime.strptime(body.contract_end_date[:10], "%Y-%m-%d").date().isoformat()
        except ValueError:
            contract_end = None

    db = get_client()
    now = _now().isoformat()
    client_ip = (request.headers.get("x-forwarded-for") or
                 (request.client.host if request.client else "")) \
        .split(",")[0].strip()[:60]

    # abuse guard: max 15 submissions per IP per hour
    hour_start = _now().replace(minute=0, second=0, microsecond=0).isoformat()
    recent = db.table("giadienre_subscriptions").select("id", count="exact") \
        .eq("client_ip", client_ip).gte("last_submission_at", hour_start) \
        .limit(1).execute().count or 0
    if recent >= 15:
        raise HTTPException(status_code=429,
                            detail="Too many submissions — please call us instead.")

    # duplicate subscription? (same email or phone) → update, don't re-create
    existing = None
    if email:
        rows = db.table("giadienre_subscriptions").select("*") \
            .ilike("email", email).limit(1).execute().data
        existing = rows[0] if rows else None
    if not existing and len(phone_digits) >= 7:
        rows = db.table("giadienre_subscriptions").select("*") \
            .eq("phone_digits", phone_digits).limit(1).execute().data
        existing = rows[0] if rows else None

    fields = {
        "full_name": body.full_name.strip(),
        "email": email or None,
        "phone": (body.phone or "").strip() or None,
        "phone_digits": phone_digits or None,
        "service_address": (body.service_address or "").strip() or None,
        "city": (body.city or "").strip() or None,
        "state": (body.state or "TX").strip()[:2].upper(),
        "zip": _digits(body.zip)[:10] or None,
        "utility_provider": (body.utility_provider or "").strip() or None,
        "current_provider": (body.current_provider or "").strip() or None,
        "contract_end_date": contract_end,
        "extra": body.extra or {},
        "client_ip": client_ip,
    }

    if existing:
        # idempotency: an identical re-submission inside 2 minutes returns the
        # same reference without re-notifying (double-click / retry protection)
        last = existing.get("last_submission_at") or existing.get("created_at") or ""
        try:
            recent_dupe = (_now() - datetime.fromisoformat(last.replace("Z", "+00:00"))) < timedelta(minutes=2)
        except ValueError:
            recent_dupe = False

        updates = {k: v for k, v in fields.items() if v not in (None, "", {})}
        if "extra" in updates:   # merge — never clobber portal_auth / bills / pending_checkout
            updates["extra"] = {**(existing.get("extra") or {}), **updates["extra"]}
        # a real signup upgrades a bill-analysis record; never downgrade
        if body.form_type == "signup":
            updates["form_type"] = "signup"
            if body.plan_id:
                updates["plan_id"] = body.plan_id
                updates["plan_name"] = PLAN_NAMES.get(body.plan_id)
            if body.billing_cycle:
                updates["billing_cycle"] = body.billing_cycle
        if not recent_dupe:
            updates["submission_count"] = (existing.get("submission_count") or 1) + 1
        updates["last_submission_at"] = now
        updates["updated_at"] = now
        db.table("giadienre_subscriptions").update(updates).eq("id", existing["id"]).execute()
        sub_id = existing["id"]
        if recent_dupe:
            return {"ok": True, "reference": f"GDR-{sub_id[:8].upper()}", "id": sub_id}
        crm_customer_id = existing.get("crm_customer_id")
        action = "resubmission"
    else:
        record = dict(fields)
        record.update({
            "form_type": body.form_type,
            "plan_id": body.plan_id,
            "plan_name": PLAN_NAMES.get(body.plan_id or ""),
            "billing_cycle": body.billing_cycle,
            "status": "NEW",
            "lead_source": lead_source,
            "subscribed_at": now,
            "last_submission_at": now,
        })
        sub = db.table("giadienre_subscriptions").insert(record).execute().data[0]
        sub_id = sub["id"]
        crm_customer_id = None
        action = "subscription"

    # portal password (optional) — hash and store; never overwrite an existing one
    if body.password:
        from app.auth.core import hash_password
        cur = db.table("giadienre_subscriptions").select("extra") \
            .eq("id", sub_id).limit(1).execute().data
        cur_extra = dict((cur[0].get("extra") if cur else {}) or {})
        if not (cur_extra.get("portal_auth") or {}).get("password_hash"):
            cur_extra["portal_auth"] = {"password_hash": hash_password(body.password),
                                        "set_at": now}
            db.table("giadienre_subscriptions").update({"extra": cur_extra}) \
                .eq("id", sub_id).execute()

    # CRM mirroring: create or dedupe the customer record (best effort —
    # the subscription stands alone even if this hiccups)
    try:
        if not crm_customer_id:
            crm_customer_id = _upsert_crm_customer(db, body, email, phone_digits,
                                                   source_label=site["label"])
            if crm_customer_id:
                db.table("giadienre_subscriptions").update(
                    {"crm_customer_id": crm_customer_id}).eq("id", sub_id).execute()
        if crm_customer_id:
            audit(db, "crm_customers", crm_customer_id, f"giadienre_{action}",
                  None, {"subscription_id": sub_id, "form_type": body.form_type,
                         "plan": body.plan_id, "at": now},
                  reason=f"Customer {action} via {lead_source}", actor="giadienre-web")
    except Exception:
        pass

    # audit trail for the subscription event itself
    audit(db, "giadienre_subscriptions", sub_id, action, None,
          {"form_type": body.form_type, "plan": body.plan_id,
           "billing": body.billing_cycle, "ip": client_ip},
          reason=f"Website {action}", actor="giadienre-web")

    # in-CRM notification (shows in AI Operations alerts + sidebar badge)
    try:
        kind = "subscription" if body.form_type == "signup" else "bill-analysis request"
        db.table("ai_alerts").insert({
            "type": "giadienre_subscription", "entity_type": "giadienre_subscription",
            "entity_id": str(sub_id), "status": "open", "severity": "medium",
            "message": f"New {site['label']} {kind}: {body.full_name.strip()}"
                       f"{' — ' + PLAN_NAMES[body.plan_id] if body.plan_id in PLAN_NAMES else ''}",
            "metadata": {"form_type": body.form_type, "plan": body.plan_id,
                         "lead_source": lead_source,
                         "crm_customer_id": crm_customer_id},
        }).execute()
    except Exception:
        pass

    # email the office (best effort)
    try:
        import resend
        if not getattr(resend, "api_key", None):
            resend.api_key = os.environ.get("RESEND_API_KEY", "")
        if resend.api_key:
            kind = "subscription" if body.form_type == "signup" else "bill-analysis request"
            plan_line = (f"Plan: {PLAN_NAMES.get(body.plan_id, '—')} · "
                         f"{body.billing_cycle or '—'}" if body.form_type == "signup" else
                         f"Current REP: {body.current_provider or '—'} · "
                         f"Utility: {body.utility_provider or '—'}")
            resend.Emails.send({
                "from": os.environ.get("REMINDER_FROM_EMAIL", "reminders@saigonpower.com"),
                "to": [os.environ.get("ADMIN_ALERT_EMAIL", "lance112709@gmail.com")],
                "subject": f"🔌 New {site['label']} {kind}: {body.full_name.strip()}",
                "html": f"<p><b>{body.full_name.strip()}</b> ({body.phone or body.email}) "
                        f"submitted a {kind} on {site['domain']}.</p>"
                        f"<p>{body.service_address or ''} {body.city or ''} {body.zip or ''}<br>"
                        f"{plan_line}</p>"
                        f"<p>Open the CRM → {site['tab']} to review.</p>",
            })
    except Exception:
        pass

    return {"ok": True, "reference": f"GDR-{sub_id[:8].upper()}", "id": sub_id}


# ── Authed: CRM module ────────────────────────────────────────────────────────
# NOTE: static paths must be registered BEFORE /{id} routes.

def _apply_filters(q, status, form_type, plan, agent, date_from, date_to,
                   lead_source=None):
    if lead_source:
        q = q.eq("lead_source", lead_source)
    if status:
        q = q.eq("status", status.upper())
    if form_type:
        q = q.eq("form_type", form_type)
    if plan:
        q = q.eq("plan_id", plan)
    if agent:
        q = q.eq("assigned_agent", agent)
    if date_from:
        q = q.gte("created_at", date_from)
    if date_to:
        q = q.lte("created_at", f"{date_to}T23:59:59")
    return q


SORTABLE = {"created_at", "subscribed_at", "updated_at", "full_name", "status",
            "plan_id", "city"}


def _search_match(r: dict, s: str) -> bool:
    s = s.lower()
    return (s in (r.get("full_name") or "").lower()
            or s in (r.get("email") or "").lower()
            or s in (r.get("phone") or "")
            or s in _digits(r.get("phone"))
            or s in (r.get("service_address") or "").lower()
            or s in (r.get("city") or "").lower())


def _sanitize_sub(row: dict) -> dict:
    """Strip payment tokens and the portal password hash before the staff UI.

    The masked display fields (card_last4/brand/expiry) are all the UI needs;
    vault tokens, in-flight checkout secrets, and portal_auth stay server-side.
    """
    row = dict(row)
    for secret in ("helcim_card_token", "helcim_customer_code"):
        row.pop(secret, None)
    if isinstance(row.get("extra"), dict):
        row["extra"] = {k: v for k, v in row["extra"].items()
                        if k not in ("pending_checkout", "portal_auth")}
    return row


@router.get("/subscriptions")
def list_subscriptions(
    search: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    form_type: Optional[str] = Query(None),
    plan: Optional[str] = Query(None),
    assigned_agent: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    lead_source: Optional[str] = Query(None),
    sort_by: str = Query("created_at"),
    sort_dir: str = Query("desc"),
    limit: int = Query(50, le=200),
    offset: int = Query(0),
    user: UserContext = Depends(get_current_user),
):
    db = get_client()
    sort_col = sort_by if sort_by in SORTABLE else "created_at"
    q = db.table("giadienre_subscriptions").select("*", count="exact")
    q = _apply_filters(q, status, form_type, plan, assigned_agent, date_from, date_to,
                       lead_source)
    q = q.order(sort_col, desc=(sort_dir != "asc"))

    if search:
        # search happens in Python across paginated chunks (fields span
        # name/email/phone/address, and phone needs digit-normalizing)
        found, off = [], 0
        while True:
            page = q.range(off, off + 999).execute().data or []
            found.extend(r for r in page if _search_match(r, search))
            if len(page) < 1000:
                break
            off += 1000
        return {"subscriptions": [_sanitize_sub(r) for r in found[offset:offset + limit]],
                "total": len(found)}

    res = q.range(offset, offset + limit - 1).execute()
    return {"subscriptions": [_sanitize_sub(r) for r in (res.data or [])],
            "total": res.count or 0}


@router.get("/subscriptions/stats")
def subscription_stats(lead_source: Optional[str] = Query(None),
                       user: UserContext = Depends(get_current_user)):
    db = get_client()
    t = db.table("giadienre_subscriptions")

    def _count(**eqs):
        q = t.select("id", count="exact")
        if lead_source:
            q = q.eq("lead_source", lead_source)
        for k, v in eqs.items():
            q = q.eq(k, v) if not k.startswith("gte_") else q.gte(k[4:], v)
        return q.limit(1).execute().count or 0

    now = _now()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week = today - timedelta(days=today.weekday())      # Monday
    month = today.replace(day=1)

    total = _count()
    active = _count(status="ACTIVE")
    cancelled = _count(status="CANCELLED")
    new_ = _count(status="NEW")
    contacted = _count(status="CONTACTED")

    stats = {
        "total": total,
        "today": _count(gte_created_at=today.isoformat()),
        "this_week": _count(gte_created_at=week.isoformat()),
        "this_month": _count(gte_created_at=month.isoformat()),
        "new": new_,
        "contacted": contacted,
        "active": active,
        "cancelled": cancelled,
        "signups": _count(form_type="signup"),
        "bill_analysis": _count(form_type="bill_analysis"),
        "conversion_rate": round(active / total * 100, 1) if total else 0.0,
    }

    # monthly growth series — last 12 months
    months: dict = {}
    cursor = month
    for _ in range(12):
        months[cursor.strftime("%Y-%m")] = 0
        cursor = (cursor - timedelta(days=1)).replace(day=1)
    floor = min(months.keys())
    off = 0
    while True:
        q = t.select("created_at").gte("created_at", f"{floor}-01")
        if lead_source:
            q = q.eq("lead_source", lead_source)
        page = q.range(off, off + 999).execute().data or []
        for r in page:
            key = (r.get("created_at") or "")[:7]
            if key in months:
                months[key] += 1
        if len(page) < 1000:
            break
        off += 1000
    stats["monthly"] = [{"month": k, "count": months[k]} for k in sorted(months)]
    return stats


@router.get("/subscriptions/new-count")
def new_subscriptions_count(since: Optional[str] = Query(None),
                            lead_source: Optional[str] = Query(None),
                            user: UserContext = Depends(get_current_user)):
    """Sidebar badge: subscriptions created after `since` (defaults to 7 days)."""
    db = get_client()
    cutoff = since or (_now() - timedelta(days=7)).isoformat()
    q = db.table("giadienre_subscriptions").select("id", count="exact") \
        .gte("created_at", cutoff)
    if lead_source:
        q = q.eq("lead_source", lead_source)
    count = q.limit(1).execute().count or 0
    return {"count": count}


@router.get("/subscriptions/export")
def export_subscriptions(
    status: Optional[str] = Query(None),
    form_type: Optional[str] = Query(None),
    plan: Optional[str] = Query(None),
    assigned_agent: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    lead_source: Optional[str] = Query(None),
    user: UserContext = Depends(get_current_user),
):
    db = get_client()
    rows, off = [], 0
    while True:
        q = db.table("giadienre_subscriptions").select("*")
        q = _apply_filters(q, status, form_type, plan, assigned_agent, date_from, date_to,
                           lead_source)
        page = q.order("created_at", desc=True).range(off, off + 999).execute().data or []
        rows.extend(page)
        if len(page) < 1000:
            break
        off += 1000

    cols = ["full_name", "email", "phone", "service_address", "city", "state", "zip",
            "utility_provider", "current_provider", "contract_end_date",
            "plan_name", "billing_cycle", "form_type", "status", "assigned_agent",
            "lead_source", "subscribed_at", "updated_at", "crm_customer_id"]
    headers = ["Full Name", "Email", "Phone", "Service Address", "City", "State", "ZIP",
               "Utility Provider", "Current Provider", "Contract End",
               "Plan", "Billing", "Form Type", "Status", "Assigned Agent",
               "Lead Source", "Subscribed At", "Last Updated", "CRM Customer ID"]

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(headers)
    for r in rows:
        w.writerow([r.get(c) if r.get(c) is not None else "" for c in cols])
    buf.seek(0)
    stamp = _now().strftime("%Y%m%d")
    return StreamingResponse(
        iter([buf.getvalue()]), media_type="text/csv",
        headers={"Content-Disposition":
                 f"attachment; filename=giadienre_subscriptions_{stamp}.csv"})


@router.get("/subscriptions/{id}")
def get_subscription(id: str, user: UserContext = Depends(get_current_user)):
    db = get_client()
    rows = db.table("giadienre_subscriptions").select("*").eq("id", id) \
        .limit(1).execute().data
    if not rows:
        raise HTTPException(status_code=404, detail="Subscription not found")
    sub = _sanitize_sub(rows[0])
    customer = None
    if sub.get("crm_customer_id"):
        c = db.table("crm_customers").select("*").eq("id", sub["crm_customer_id"]) \
            .limit(1).execute().data
        customer = c[0] if c else None
    activity = db.table("audit_log").select("action,new_value,reason,actor,created_at") \
        .eq("table_name", "giadienre_subscriptions").eq("record_id", str(id)) \
        .order("created_at", desc=True).limit(50).execute().data or []
    return {"subscription": sub, "customer": customer, "activity": activity}


@router.patch("/subscriptions/{id}")
def update_subscription(id: str, data: dict = Body(...),
                        user: UserContext = Depends(get_current_user)):
    db = get_client()
    allowed = {"status", "assigned_agent", "full_name", "email", "phone",
               "service_address", "city", "state", "zip",
               "utility_provider", "current_provider", "contract_end_date",
               "plan_id", "billing_cycle"}
    payload = {k: v for k, v in data.items() if k in allowed}
    if not payload:
        raise HTTPException(status_code=400, detail="Nothing to update")
    if "status" in payload:
        payload["status"] = str(payload["status"]).upper()
        if payload["status"] not in STATUSES:
            raise HTTPException(status_code=400,
                                detail=f"Invalid status. Use one of {STATUSES}")
    if "plan_id" in payload and payload["plan_id"]:
        if payload["plan_id"] not in PLAN_NAMES:
            raise HTTPException(status_code=400, detail="Invalid plan.")
        payload["plan_name"] = PLAN_NAMES[payload["plan_id"]]
    if "phone" in payload:
        payload["phone_digits"] = _digits(payload.get("phone")) or None
    payload["updated_at"] = _now().isoformat()

    old = db.table("giadienre_subscriptions").select("status,assigned_agent") \
        .eq("id", id).limit(1).execute().data
    res = db.table("giadienre_subscriptions").update(payload).eq("id", id).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Subscription not found")
    audit(db, "giadienre_subscriptions", id, "update", old[0] if old else None,
          payload, actor=user.email or user.name or "crm-user")
    return res.data[0]


@router.get("/subscriptions/{id}/notes")
def list_notes(id: str, user: UserContext = Depends(get_current_user)):
    db = get_client()
    return db.table("giadienre_subscription_notes").select("*") \
        .eq("subscription_id", id).order("created_at", desc=True).execute().data or []


@router.post("/subscriptions/{id}/notes")
def create_note(id: str, data: dict = Body(...),
                user: UserContext = Depends(get_current_user)):
    content = (data.get("content") or "").strip()
    if not content:
        raise HTTPException(status_code=400, detail="Note content is required")
    db = get_client()
    note = {
        "subscription_id": id,
        "content": content[:5000],
        "author_name": (data.get("author_name") or user.name or "").strip()[:200],
        "is_internal": bool(data.get("is_internal")),
    }
    return db.table("giadienre_subscription_notes").insert(note).execute().data[0]


@router.delete("/subscriptions/{id}/notes/{note_id}")
def delete_note(id: str, note_id: str, user: UserContext = Depends(require_admin)):
    db = get_client()
    db.table("giadienre_subscription_notes").delete().eq("id", note_id) \
        .eq("subscription_id", id).execute()
    return {"ok": True}


# ── Billing (Helcim) ─────────────────────────────────────────────────────────
# Card data never touches this server: HelcimPay.js tokenizes in Helcim's
# iframe. We initialize sessions, validate the signed result, and store tokens.

def _next_billing(billing_cycle: Optional[str]) -> str:
    now = _now().date()
    if billing_cycle == "annual":
        try:
            return now.replace(year=now.year + 1).isoformat()
        except ValueError:                       # Feb 29
            return now.replace(year=now.year + 1, day=28).isoformat()
    month = now.month + 1
    year = now.year + (1 if month > 12 else 0)
    month = 1 if month > 12 else month
    day = min(now.day, 28)
    return now.replace(year=year, month=month, day=day).isoformat()


def _get_sub_or_404(db, sub_id: str) -> dict:
    rows = db.table("giadienre_subscriptions").select("*").eq("id", sub_id) \
        .limit(1).execute().data
    if not rows:
        raise HTTPException(status_code=404, detail="Subscription not found")
    return rows[0]


def _find_sub_by_phone(db, p10: str) -> Optional[dict]:
    rows = db.table("giadienre_subscriptions").select("*") \
        .or_(f"phone_digits.eq.{p10},phone_digits.eq.1{p10}") \
        .order("created_at", desc=True).limit(1).execute().data
    return rows[0] if rows else None


def _store_pending(db, sub: dict, checkout_token: str, secret_token: str, kind: str):
    extra = dict(sub.get("extra") or {})
    extra["pending_checkout"] = {"token": checkout_token, "secret": secret_token,
                                 "type": kind, "at": _now().isoformat()}
    db.table("giadienre_subscriptions").update({"extra": extra}) \
        .eq("id", sub["id"]).execute()


def _pop_pending(db, sub: dict, checkout_token: str, kind: str) -> str:
    pending = (sub.get("extra") or {}).get("pending_checkout") or {}
    if not pending.get("secret") or pending.get("token") != checkout_token \
            or pending.get("type") != kind:
        raise HTTPException(status_code=400,
                            detail="Payment session expired — please try again.")
    extra = dict(sub.get("extra") or {})
    extra.pop("pending_checkout", None)
    db.table("giadienre_subscriptions").update({"extra": extra}) \
        .eq("id", sub["id"]).execute()
    return pending["secret"]


def _apply_card(db, sub: dict, info: dict, updates: Optional[dict] = None) -> dict:
    payload = dict(updates or {})
    if info.get("customer_code"):
        payload["helcim_customer_code"] = info["customer_code"]
    if info.get("card_token"):
        payload["helcim_card_token"] = info["card_token"]
    if info.get("card_last4"):
        payload["card_last4"] = info["card_last4"]
    if info.get("card_brand"):
        payload["card_brand"] = info["card_brand"]
    if info.get("customer_code") and info.get("card_token"):
        expiry = helcim.card_expiry_for(info["customer_code"], info["card_token"])
        if expiry:
            payload["card_expiry"] = expiry
    payload["card_updated_at"] = _now().isoformat()
    payload["updated_at"] = _now().isoformat()
    return db.table("giadienre_subscriptions").update(payload) \
        .eq("id", sub["id"]).execute().data[0]


# — Signup payment (public; keyed by unguessable subscription UUID) —

@router.post("/billing/pay-session")
def billing_pay_session(data: dict = Body(...)):
    if not helcim.is_configured():
        raise HTTPException(status_code=503, detail="Payments are not configured yet.")
    db = get_client()
    sub = _get_sub_or_404(db, str(data.get("subscription_id") or ""))
    key = (sub.get("plan_id"), sub.get("billing_cycle"))
    amount = helcim.PLAN_PRICES.get(key)
    if not amount:
        raise HTTPException(status_code=400, detail="No membership plan selected.")
    try:
        session = helcim.initialize_purchase(
            amount=amount,
            contact_name=sub["full_name"],
            email=sub.get("email"), phone=sub.get("phone"),
            street=sub.get("service_address"), city=sub.get("city"),
            postal_code=sub.get("zip"),
            customer_code=sub.get("helcim_customer_code"),
        )
    except helcim.HelcimError as e:
        audit(db, "giadienre_subscriptions", sub["id"], "helcim_error", None,
              {"stage": "pay-session", "error": str(e)[:300]}, actor="giadienre-web")
        raise HTTPException(status_code=502,
                            detail="Payment system is unavailable — please try again shortly.")
    _store_pending(db, sub, session["checkoutToken"], session["secretToken"], "purchase")
    return {"checkoutToken": session["checkoutToken"], "amount": amount}


@router.post("/billing/pay-confirm")
def billing_pay_confirm(data: dict = Body(...)):
    db = get_client()
    sub = _get_sub_or_404(db, str(data.get("subscription_id") or ""))
    checkout_token = str(data.get("checkoutToken") or "")
    secret = _pop_pending(db, sub, checkout_token, "purchase")
    event = data.get("event")
    if not helcim.validate_event(event, secret):
        raise HTTPException(status_code=400, detail="Payment could not be verified.")
    info = helcim.extract_card_info(event)
    if not info["approved"]:
        raise HTTPException(status_code=402, detail="The card was declined.")

    next_billing = _next_billing(sub.get("billing_cycle"))
    updated = _apply_card(db, sub, info, {
        "status": "ACTIVE",
        "last_payment_at": _now().isoformat(),
        "last_payment_amount": info.get("amount"),
        "next_billing_date": next_billing,
    })

    # auto-renewal: Helcim subscription bills the default card each anniversary
    if info.get("customer_code") and sub.get("plan_id") and sub.get("billing_cycle"):
        try:
            hsub = helcim.create_subscription(info["customer_code"], sub["plan_id"],
                                              sub["billing_cycle"], next_billing)
            if hsub:
                db.table("giadienre_subscriptions").update(
                    {"helcim_subscription_id": hsub}).eq("id", sub["id"]).execute()
        except Exception as e:
            audit(db, "giadienre_subscriptions", sub["id"], "helcim_error", None,
                  {"stage": "create-subscription", "error": str(e)[:300]},
                  actor="giadienre-web")

    audit(db, "giadienre_subscriptions", sub["id"], "payment", None,
          {"transaction_id": info.get("transaction_id"), "amount": info.get("amount"),
           "card": f"{info.get('card_brand')} •••{info.get('card_last4')}"},
          reason="Membership payment via GiaDienRe website", actor="giadienre-web")
    try:
        db.table("ai_alerts").insert({
            "type": "giadienre_payment", "entity_type": "giadienre_subscription",
            "entity_id": str(sub["id"]), "status": "open", "severity": "low",
            "message": f"💳 GiaDienRe payment: {sub['full_name']} paid "
                       f"${info.get('amount')} ({sub.get('plan_name') or 'membership'})",
            "metadata": {"transaction_id": info.get("transaction_id")},
        }).execute()
    except Exception:
        pass
    return {"ok": True, "status": updated.get("status"),
            "card_last4": updated.get("card_last4")}


# — Customer portal card management (customer JWT from /api/v1/portal) —

from app.api.v1.customer_portal import portal_user  # noqa: E402


@router.post("/billing/card-session")
def billing_card_session(user: dict = Depends(portal_user)):
    if not helcim.is_configured():
        raise HTTPException(status_code=503, detail="Payments are not configured yet.")
    db = get_client()
    sub = _find_sub_by_phone(db, user["phone"])
    if not sub:
        raise HTTPException(status_code=404,
                            detail="No GiaDienRe membership found for this account.")
    try:
        session = helcim.initialize_verify(
            customer_code=sub.get("helcim_customer_code"),
            contact_name=sub.get("full_name"),
            email=sub.get("email"), phone=sub.get("phone"),
        )
    except helcim.HelcimError as e:
        audit(db, "giadienre_subscriptions", sub["id"], "helcim_error", None,
              {"stage": "card-session", "error": str(e)[:300]},
              actor=f"customer:{user['phone']}")
        raise HTTPException(status_code=502,
                            detail="Payment system is unavailable — please try again shortly.")
    _store_pending(db, sub, session["checkoutToken"], session["secretToken"], "verify")
    return {"checkoutToken": session["checkoutToken"]}


@router.post("/billing/card-confirm")
def billing_card_confirm(data: dict = Body(...), user: dict = Depends(portal_user)):
    db = get_client()
    sub = _find_sub_by_phone(db, user["phone"])
    if not sub:
        raise HTTPException(status_code=404,
                            detail="No GiaDienRe membership found for this account.")
    checkout_token = str(data.get("checkoutToken") or "")
    secret = _pop_pending(db, sub, checkout_token, "verify")
    event = data.get("event")
    if not helcim.validate_event(event, secret):
        raise HTTPException(status_code=400, detail="Card could not be verified.")
    info = helcim.extract_card_info(event)
    if not info["approved"]:
        raise HTTPException(status_code=402, detail="The card was declined.")

    updated = _apply_card(db, sub, info)
    audit(db, "giadienre_subscriptions", sub["id"], "card_update", None,
          {"card": f"{info.get('card_brand')} •••{info.get('card_last4')}"},
          reason="Customer updated card via portal", actor=f"customer:{user['phone']}")
    return {"ok": True, "card_last4": updated.get("card_last4"),
            "card_brand": updated.get("card_brand"),
            "card_expiry": updated.get("card_expiry")}


# — Customer portal: AI-extracted bill intake (customer JWT from /api/v1/portal) —
#
# giadienre.com runs the OCR (Claude vision) in its own backend, lets the
# customer review the extracted fields, then posts the confirmed data here.
# We store it on the customer's subscription, surface it to staff as a task,
# and the daily monitor (below) watches the contract end date from then on.

class PortalBillOcr(BaseModel):
    data: str                                # base64 file contents (no data: prefix)
    mediaType: str


_BILL_OCR_TYPES = {"application/pdf", "image/png", "image/jpeg", "image/webp", "image/gif"}
_BILL_OCR_MAX_BASE64 = 14_000_000            # ~10MB binary
_BILL_OCR_SYSTEM = (
    "You are an OCR + data-extraction engine for Texas residential electricity bills. "
    "Read the attached bill carefully and extract only what is actually present. "
    "Never guess or fabricate a value — if a field is not clearly on the bill, return null for it. "
    "Rates must be numeric cents per kWh (e.g. 14.2, not \"14.2¢\"). Usage must be numeric kWh. "
    "Dates must be YYYY-MM-DD. The ESI ID is a long number (often 17 digits) identifying the meter."
)
_BILL_OCR_ASK = (
    "Extract the utility fields from this electric bill. Return ONLY a JSON object with exactly "
    "these keys: customer_name, service_address, provider (the Retail Electric Provider), esi_id, "
    "current_rate (number, ¢/kWh), contract_end_date (YYYY-MM-DD), average_kwh (number), "
    "tdu (Transmission/Distribution Utility, e.g. CenterPoint, Oncor), meter_number, "
    "confidence (\"high\"|\"medium\"|\"low\"). Use null for anything not on the bill."
)


@router.post("/portal/bill-ocr")
def portal_bill_ocr(body: PortalBillOcr, user: dict = Depends(portal_user)):
    """AI reads an uploaded bill (image/PDF) and returns the extracted fields for
    the customer to review. Falls back gracefully when the AI key isn't set —
    the portal form then works as manual entry (same behavior as giadienre.com)."""
    if body.mediaType not in _BILL_OCR_TYPES:
        raise HTTPException(status_code=415, detail="Only PDF, JPG, PNG, or WEBP files are supported.")
    if len(body.data) > _BILL_OCR_MAX_BASE64:
        raise HTTPException(status_code=413, detail="That file is too large — please upload one under 10MB.")

    key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    if not key:
        raise HTTPException(
            status_code=503,
            detail="AI bill reading isn't activated yet — you can enter the details manually below, "
                   "or call (832) 937-9999 and we'll do it for you.")

    try:
        import json as _json
        import anthropic
        client = anthropic.Anthropic(api_key=key)
        block = {
            "type": "document" if body.mediaType == "application/pdf" else "image",
            "source": {"type": "base64", "media_type": body.mediaType, "data": body.data},
        }
        resp = client.messages.create(
            model="claude-opus-4-8", max_tokens=1500, system=_BILL_OCR_SYSTEM,
            messages=[{"role": "user", "content": [block, {"type": "text", "text": _BILL_OCR_ASK}]}])
        text = "".join(getattr(b, "text", "") for b in resp.content)
        m = re.search(r"\{.*\}", text, re.S)
        extracted = _json.loads(m.group(0)) if m else {}
        audit(get_client(), "giadienre_subscriptions", None, "portal_bill_ocr", None,
              {"confidence": extracted.get("confidence"), "mediaType": body.mediaType},
              reason="Portal bill read by AI", actor=f"customer:{user['phone']}")
        return {"ok": True, "extracted": extracted}
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(
            status_code=502,
            detail="We couldn't read that bill — you can enter the details manually, "
                   "or try again with a clearer photo.")


class PortalBill(BaseModel):
    customer_name: Optional[str] = Field(default=None, max_length=200)
    service_address: Optional[str] = Field(default=None, max_length=300)
    provider: Optional[str] = Field(default=None, max_length=120)       # REP
    esi_id: Optional[str] = Field(default=None, max_length=40)
    current_rate: Optional[float] = None                               # ¢/kWh
    contract_end_date: Optional[str] = None                            # YYYY-MM-DD
    average_kwh: Optional[float] = None
    tdu: Optional[str] = Field(default=None, max_length=120)
    meter_number: Optional[str] = Field(default=None, max_length=60)
    bill_file_name: Optional[str] = Field(default=None, max_length=300)
    source: str = "portal_ocr"                                          # portal_ocr | portal_manual


def _valid_date(s: Optional[str]) -> Optional[str]:
    try:
        return datetime.strptime(str(s)[:10], "%Y-%m-%d").date().isoformat()
    except Exception:
        return None


@router.post("/portal/bill")
def portal_bill(body: PortalBill, user: dict = Depends(portal_user)):
    db = get_client()
    sub = _find_sub_by_phone(db, user["phone"])
    if not sub:
        raise HTTPException(status_code=404,
                            detail="No GiaDienRe membership found for this account.")

    end_date = _valid_date(body.contract_end_date)
    bill = {k: v for k, v in body.model_dump().items() if v not in (None, "")}
    bill["contract_end_date"] = end_date
    bill["received_at"] = _now().isoformat()

    # extra JSONB keeps the full extraction history (latest 12 bills)
    extra = dict(sub.get("extra") or {})
    bills = (extra.get("bills") or [])[-11:]
    bills.append(bill)
    extra["bills"] = bills
    extra["latest_bill"] = bill

    updates = {"extra": extra, "updated_at": _now().isoformat()}
    # fill/refresh the structured columns the CRM and monitor key off
    if body.service_address and not sub.get("service_address"):
        updates["service_address"] = body.service_address.strip()
    if body.provider:
        updates["current_provider"] = body.provider.strip()
    if body.tdu:
        updates["utility_provider"] = body.tdu.strip()
    if end_date:
        updates["contract_end_date"] = end_date
    db.table("giadienre_subscriptions").update(updates).eq("id", sub["id"]).execute()

    rate = f"{body.current_rate}¢/kWh" if body.current_rate is not None else "—"
    usage = f"{body.average_kwh:g} kWh/mo" if body.average_kwh is not None else "—"
    summary = (f"Bill received via portal ({body.source}). "
               f"REP: {body.provider or '—'} · Rate: {rate} · Usage: {usage} · "
               f"TDU: {body.tdu or '—'} · ESI: {body.esi_id or '—'} · "
               f"Contract ends: {end_date or '—'}"
               + (f" · File: {body.bill_file_name}" if body.bill_file_name else ""))
    try:
        db.table("giadienre_subscription_notes").insert({
            "subscription_id": sub["id"], "content": summary,
            "author_name": "GiaDienRe AI", "is_internal": False,
        }).execute()
    except Exception:
        pass  # note is best-effort

    db.table("tasks").insert({
        # tasks_task_type_check only allows call/email/text/general
        "task_type": "general",
        "title": f"📄 GiaDienRe bill uploaded: {sub.get('full_name') or user['phone']}",
        "description": f"{summary}\nPhone: {user['phone']}\n[gdr:{sub['id']}]",
        "due_date": (_now() + timedelta(days=2)).isoformat(),
        "status": "pending", "priority": "medium",
        "lead_id": None, "customer_id": None, "deal_id": None,
    }).execute()

    audit(db, "giadienre_subscriptions", sub["id"], "portal_bill", None, bill,
          reason="Customer uploaded bill via portal (AI extraction)",
          actor=f"customer:{user['phone']}")

    days_left = None
    if end_date:
        days_left = (datetime.strptime(end_date, "%Y-%m-%d").date()
                     - _now().date()).days
    return {"ok": True, "subscription_id": sub["id"], "days_left": days_left}


@router.post("/portal/smt-interest")
def portal_smt_interest(user: dict = Depends(portal_user)):
    """Khách đăng ký nhận thông báo khi Smart Meter Texas được kích hoạt."""
    db = get_client()
    sub = _find_sub_by_phone(db, user["phone"])
    if not sub:
        raise HTTPException(status_code=404,
                            detail="No GiaDienRe membership found for this account.")
    extra = dict(sub.get("extra") or {})
    already = bool(extra.get("smt_interest"))
    if not already:
        extra["smt_interest"] = {"at": _now().isoformat()}
        db.table("giadienre_subscriptions").update(
            {"extra": extra, "updated_at": _now().isoformat()}).eq("id", sub["id"]).execute()
        db.table("tasks").insert({
            "task_type": "general",
            "title": f"📡 SMT interest: {sub.get('full_name') or user['phone']}",
            "description": (f"Customer wants Smart Meter Texas monitoring when it launches.\n"
                            f"Phone: {user['phone']}\n[gdr:{sub['id']}]"),
            "due_date": (_now() + timedelta(days=30)).isoformat(),
            "status": "pending", "priority": "low",
            "lead_id": None, "customer_id": None, "deal_id": None,
        }).execute()
        audit(db, "giadienre_subscriptions", sub["id"], "smt_interest", None,
              {"at": extra["smt_interest"]["at"]},
              reason="Customer registered Smart Meter Texas interest via portal",
              actor=f"customer:{user['phone']}")
    return {"ok": True, "already": already}


# — Daily AI contract monitoring (cron; giadienre.com Vercel Cron calls this) —

MONITOR_WINDOW_DAYS = 90


@router.post("/monitor/run")
def monitor_run(x_cron_key: str = Header(default="")):
    expected = os.environ.get("GDR_CRON_KEY") or os.environ.get("CRON_SECRET")
    if not expected:
        raise HTTPException(status_code=503, detail="Monitoring is not configured.")
    if x_cron_key != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")

    db = get_client()
    subs, off = [], 0
    while True:
        page = db.table("giadienre_subscriptions") \
            .select("id, full_name, phone, phone_digits, current_provider, "
                    "utility_provider, contract_end_date, status, extra") \
            .neq("status", "CANCELLED").range(off, off + 999).execute().data or []
        subs.extend(page)
        if len(page) < 1000:
            break
        off += 1000

    today = _now().date()
    counts = {"checked": len(subs), "with_end_date": 0, "expired": 0,
              "expiring_30": 0, "expiring_90": 0, "tasks_created": 0}

    for sub in subs:
        end = _valid_date(sub.get("contract_end_date"))
        if not end:
            continue
        counts["with_end_date"] += 1
        days_left = (datetime.strptime(end, "%Y-%m-%d").date() - today).days
        if days_left < 0:
            counts["expired"] += 1
        elif days_left <= 30:
            counts["expiring_30"] += 1
        elif days_left <= MONITOR_WINDOW_DAYS:
            counts["expiring_90"] += 1
        else:
            continue

        # one open renewal task per subscription — dedupe on the [gdr:id] marker
        marker = f"[gdr:{sub['id']}]"
        existing = db.table("tasks").select("id") \
            .eq("task_type", "general").eq("status", "pending") \
            .ilike("description", f"%{marker}%").limit(1).execute().data
        if existing:
            continue

        if days_left < 0:
            urgency, priority = f"contract EXPIRED {-days_left} days ago", "high"
        elif days_left <= 30:
            urgency, priority = f"contract expires in {days_left} days", "high"
        else:
            urgency, priority = f"contract expires in {days_left} days", "medium"
        db.table("tasks").insert({
            "task_type": "general",   # constraint: call/email/text/general only
            "title": f"⚡ GiaDienRe renewal: {sub.get('full_name') or sub.get('phone') or sub['id'][:8]}",
            "description": (f"Daily monitor: {urgency}.\n"
                            f"REP: {sub.get('current_provider') or '—'} · "
                            f"TDU: {sub.get('utility_provider') or '—'} · "
                            f"Ends: {end}\nPhone: {sub.get('phone') or '—'}\n{marker}"),
            "due_date": (_now() + timedelta(days=1)).isoformat(),
            "status": "pending", "priority": priority,
            "lead_id": None, "customer_id": None, "deal_id": None,
        }).execute()
        counts["tasks_created"] += 1

    audit(db, "giadienre_subscriptions", "daily-run", "monitor_run", None, counts,
          reason="Daily GiaDienRe contract monitoring", actor="cron:giadienre")

    if counts["tasks_created"]:
        try:
            from app.services.sms import send_sms
            send_sms(os.environ.get("ADMIN_ALERT_PHONE", "+18329379999"),
                     f"GiaDienRe monitor: {counts['tasks_created']} renewal "
                     f"opportunity(ies) found — check Tasks in the CRM.")
        except Exception:
            pass

    return {"ok": True, **counts}
