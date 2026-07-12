"""SaigonLLC website integrations — webhooks from the saigonllc.com platform.

POST /api/v1/saigonllc/power-enrollment
    Receiver for saigonllc's power-enrollment CRM sync (apps/api
    CrmSyncService). Signed with
        X-Saigon-Signature: HMAC-SHA256(rawBody, SAIGONLLC_POWER_WEBHOOK_SECRET)
    and pointed at by saigonllc's POWER_CRM_WEBHOOK_URL / _SECRET env pair.

    Idempotent by enrollment reference (SGE-XXXXXX): the first delivery
    creates an `enrollments` row plus the standard CRM lead + Future-deal
    mirror (same shape as /enrollments/public); later deliveries — retries,
    provider webhooks, staff status overrides on saigonllc — only update the
    enrollment status.
"""
import hashlib
import hmac
import json
import os
from datetime import datetime, timezone

from fastapi import APIRouter, Header, HTTPException, Request

from app.db.client import get_client
from app.services.audit import audit

router = APIRouter()

SOURCE_PREFIX = "saigonllc:"

# saigonllc PowerEnrollmentStatus → CRM enrollments.STATUSES
STATUS_MAP = {
    "SUBMITTED": "submitted",
    "PENDING_PROVIDER": "sent_to_provider",
    "CONFIRMED": "accepted",
    "ACTIVE": "active",
    "FAILED": "rejected",
    "CANCELED": "cancelled",
}
MOVE_TYPE_MAP = {"SWITCH": "switch", "MOVE_IN": "move_in"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@router.post("/power-enrollment")
async def receive_power_enrollment(request: Request,
                                   x_saigon_signature: str = Header(default="")):
    secret = os.environ.get("SAIGONLLC_POWER_WEBHOOK_SECRET", "").strip()
    if not secret:
        raise HTTPException(status_code=503,
                            detail="saigonllc webhook is not configured")
    raw = await request.body()
    expected = hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, (x_saigon_signature or "").strip()):
        raise HTTPException(status_code=401, detail="Invalid signature")

    try:
        payload = json.loads(raw)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    customer = payload.get("customer") or {}
    enrollment = payload.get("enrollment") or {}
    reference = str(enrollment.get("reference") or "").strip()[:20]
    first_name = str(customer.get("firstName") or "").strip()
    last_name = str(customer.get("lastName") or "").strip()
    if not reference or not first_name or not phone_of(customer):
        raise HTTPException(status_code=400,
                            detail="reference, customer name and phone are required")

    status = STATUS_MAP.get(str(enrollment.get("status") or ""), "submitted")
    source = f"{SOURCE_PREFIX}{reference}"
    db = get_client()

    existing = db.table("enrollments").select("id, status, lead_id") \
        .eq("source", source).limit(1).execute().data
    if existing:
        row = existing[0]
        if row.get("status") != status:
            db.table("enrollments").update(
                {"status": status}).eq("id", row["id"]).execute()
            audit(db, "enrollments", row["id"], "saigonllc_status_sync",
                  {"status": row.get("status")}, {"status": status},
                  reason=f"saigonllc.com status sync ({reference})",
                  actor="saigonllc-web")
        return {"ok": True, "id": row["id"], "updated": True}

    address = enrollment.get("serviceAddress") or {}
    plan = enrollment.get("plan") or {}
    line1 = str(address.get("line1") or "").strip()
    line2 = str(address.get("line2") or "").strip()
    record = {
        "status": status,
        "source": source,
        "first_name": first_name[:100],
        "last_name": last_name[:100] or "—",
        "email": (str(customer.get("email") or "").strip().lower() or None),
        "phone": phone_of(customer),
        "language": "en",
        "service_address": (f"{line1} {line2}".strip() or "—")[:300],
        "service_city": str(address.get("city") or "").strip()[:100] or "—",
        "service_state": (str(address.get("state") or "TX").strip()[:2].upper()),
        "service_zip": str(address.get("zip") or "").strip()[:10],
        "enrollment_type": MOVE_TYPE_MAP.get(
            str(enrollment.get("moveType") or ""), "switch"),
        "plan_name": str(plan.get("name") or "").strip() or None,
        "provider": str(plan.get("provider") or "").strip() or None,
        "term_months": plan.get("termMonths"),
        "terms_accepted_at": payload.get("submittedAt") or _now(),
    }
    enr = db.table("enrollments").insert(record).execute().data[0]

    # standard CRM pipeline mirror: lead + Future deal (best effort)
    try:
        lead = db.table("leads").insert({
            "first_name": record["first_name"], "last_name": record["last_name"],
            "address": record["service_address"], "city": record["service_city"],
            "state": record["service_state"], "zip": record["service_zip"],
            "phone": record["phone"], "email": record["email"],
            "status": "lead", "sales_agent": "Website",
            "source": "saigonllc.com",
        }).execute().data[0]
        deal = db.table("lead_deals").insert({
            "lead_id": lead["id"], "status": "Future",
            "supplier": record["provider"], "plan_name": record["plan_name"],
            "contract_term": (f"{record['term_months']} Months"
                              if record.get("term_months") else None),
            "service_address": record["service_address"],
            "service_city": record["service_city"],
            "service_state": record["service_state"],
            "service_zip": record["service_zip"],
            "sales_agent": "Website",
        }).execute().data[0]
        db.table("enrollments").update(
            {"lead_id": lead["id"], "deal_id": deal["id"]}
        ).eq("id", enr["id"]).execute()
    except Exception:
        pass  # the enrollment stands alone even if the mirror hiccups

    audit(db, "enrollments", enr["id"], "saigonllc_enrollment", None,
          {"reference": reference, "provider": record["provider"],
           "plan": record["plan_name"], "status": status},
          reason="Customer enrollment via saigonllc.com", actor="saigonllc-web")
    return {"ok": True, "id": enr["id"], "created": True}


def phone_of(customer: dict) -> str:
    return str(customer.get("phone") or "").strip()[:30]
