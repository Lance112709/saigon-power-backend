"""Lead → customer conversion.

A lead becomes a customer the moment one of its deals is Active. This module
owns that transition so every writer (deal create/edit, auto-promotion on
start date, enrollment activation, nightly self-heal) does exactly the same
thing and never fails silently.

Background: on 2026-09-04 an audit found 42 leads with an Active deal still
marked status='lead' (no lead_customers row, no SGP ID) because the
conversion step was wrapped in a bare `except: pass`.
"""
import logging
from datetime import datetime, timezone

from app.db.client import get_client

logger = logging.getLogger("saigon.lead_conversion")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def next_sgp_id(db) -> str:
    res = (
        db.table("leads").select("sgp_customer_id")
        .not_.is_("sgp_customer_id", "null")
        .order("sgp_customer_id", desc=True).limit(1).execute()
    )
    num = 1
    if res.data:
        try:
            num = int(res.data[0]["sgp_customer_id"].split("-")[1][4:]) + 1
        except Exception:
            logger.warning("Unparseable sgp_customer_id %r; restarting at 1", res.data[0])
    return f"SGP-2026{num:06d}"


def convert_lead(db, lead_id: str) -> dict:
    """Mark a lead as a converted customer if it has an Active deal.

    Idempotent. Raises on any DB failure — callers decide whether to surface
    or log, but nothing here is swallowed.
    Returns {"converted": bool, "sgp_customer_id": str|None, "reason": str}.
    """
    active = db.table("lead_deals").select("id").eq("lead_id", lead_id).eq("status", "Active").limit(1).execute()
    if not active.data:
        return {"converted": False, "sgp_customer_id": None, "reason": "no active deal"}

    existing = db.table("lead_customers").select("id").eq("lead_id", lead_id).limit(1).execute()
    if not existing.data:
        db.table("lead_customers").insert({"lead_id": lead_id}).execute()

    row = db.table("leads").select("status, sgp_customer_id").eq("id", lead_id).limit(1).execute()
    if not row.data:
        raise RuntimeError(f"lead {lead_id} not found")
    cur = row.data[0]
    patch = {"updated_at": _now()}
    if cur.get("status") != "converted":
        patch["status"] = "converted"
    sgp = cur.get("sgp_customer_id")
    if not sgp:
        sgp = next_sgp_id(db)
        patch["sgp_customer_id"] = sgp
    if len(patch) > 1:
        db.table("leads").update(patch).eq("id", lead_id).execute()
        logger.info("lead %s converted (sgp=%s)", lead_id, sgp)
    return {"converted": True, "sgp_customer_id": sgp, "reason": "ok"}


def try_convert_lead(db, lead_id: str) -> dict:
    """convert_lead that never raises: logs the traceback and reports the error
    so the caller can surface it (e.g. in an API response) instead of losing it."""
    try:
        return convert_lead(db, lead_id)
    except Exception as e:
        logger.exception("lead conversion failed for %s", lead_id)
        return {"converted": False, "sgp_customer_id": None, "reason": f"error: {e}"}


def find_stuck_leads(db) -> list[str]:
    """Lead IDs still status='lead' that have at least one Active deal."""
    active = db.table("lead_deals").select("lead_id").eq("status", "Active").execute().data or []
    active_ids = {d["lead_id"] for d in active if d.get("lead_id")}
    if not active_ids:
        return []
    stuck: list[str] = []
    off = 0
    while True:
        rows = db.table("leads").select("id").eq("status", "lead").range(off, off + 999).execute().data or []
        stuck.extend(r["id"] for r in rows if r["id"] in active_ids)
        if len(rows) < 1000:
            break
        off += 1000
    return stuck


def heal_stuck_leads(db=None) -> dict:
    """Convert every lead that has an Active deal but is still marked 'lead'.
    Safe to run any time (idempotent). Returns a summary for logs/API."""
    db = db or get_client()
    stuck = find_stuck_leads(db)
    healed, failed = [], []
    for lid in stuck:
        r = try_convert_lead(db, lid)
        (healed if r["converted"] else failed).append({"lead_id": lid, **r})
    if stuck:
        logger.warning("lead self-heal: %d stuck, %d healed, %d failed", len(stuck), len(healed), len(failed))
    return {"checked": len(stuck), "healed": healed, "failed": failed, "ran_at": _now()}
