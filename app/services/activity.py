"""Per-record activity trail (who changed what, when) on top of audit_log.

UI writes on customers, deals, leads, notes and attachments call record() /
record_update(); fetch() assembles the timeline for one customer, lead or
deal — including system entries the statement importers already write
(status_sync, end-date backfill, ...). Never raises: activity logging must
not break the operation it describes.
"""
from typing import Optional
from app.services.audit import audit

_SKIP = {"updated_at", "created_at", "id", "customer_id", "lead_id"}

LABELS = {
    "customer_updated": "Customer info updated", "customer_deleted": "Customer deleted",
    "deal_created": "Deal created", "deal_updated": "Deal updated", "deal_deleted": "Deal deleted",
    "deal_renewed": "Deal renewed", "deal_terminated": "Deal terminated",
    "deal_status_changed": "Deal status changed",
    "note_added": "Note added", "note_deleted": "Note deleted",
    "attachment_added": "File attached", "attachment_deleted": "File removed",
    "lead_created": "Lead created", "lead_updated": "Lead updated", "lead_deleted": "Lead deleted",
    "lead_converted": "Lead converted to customer",
    "status_deactivated": "Provider reported inactive (statement)",
    "status_reactivated": "Provider reported active again (statement)",
    "status_sync_blocked": "Provider status change blocked",
    "end_date_backfilled": "Contract end date filled from statement",
    "smartcare_badge_added": "SmartCare badge added", "smartcare_badge_removed": "SmartCare badge removed",
}


def actor_of(user) -> str:
    if isinstance(user, str):
        return user
    name = (getattr(user, "name", "") or "").strip()
    email = (getattr(user, "email", "") or "").strip()
    return f"{name} <{email}>" if name and email else (name or email or "system")


def _norm(v):
    if v is None or v == "":
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return float(v)
    return str(v)


def diff(before: Optional[dict], after: Optional[dict]):
    before, after = before or {}, after or {}
    old, new = {}, {}
    for k, v in after.items():
        if k in _SKIP:
            continue
        if _norm(before.get(k)) != _norm(v):
            old[k] = before.get(k)
            new[k] = v
    return old, new


def record(db, user, table: str, record_id: str, action: str, old=None, new=None, reason: str = "") -> None:
    try:
        audit(db, table, record_id, action, old, new, reason=reason or "", actor=actor_of(user))
    except Exception:
        pass


def record_update(db, user, table: str, record_id: str, before: Optional[dict], after: Optional[dict],
                  action: str = "updated", reason: str = "") -> None:
    """Store only the fields that actually changed; write nothing for a no-op."""
    old, new = diff(before, after)
    if not new:
        return
    record(db, user, table, record_id, action, old, new, reason)


def _event(r: dict) -> dict:
    old, new = r.get("old_value"), r.get("new_value")
    changes = []
    if isinstance(new, dict) and isinstance(old, dict):
        for k, v in new.items():
            changes.append({"field": k, "from": old.get(k), "to": v})
    actor = r.get("actor") or "system"
    name = actor.split("<")[0].strip() or actor
    return {
        "id": r["id"], "at": r["created_at"], "actor": actor, "actor_name": name,
        "system": "<" not in actor and "@" not in actor,
        "table": r.get("table_name"), "record_id": r.get("record_id"),
        "action": r.get("action"),
        "label": LABELS.get(r.get("action") or "", (r.get("action") or "").replace("_", " ").capitalize()),
        "changes": changes,
        "details": new if (isinstance(new, dict) and not isinstance(old, dict)) else None,
        "snapshot": old if (isinstance(old, dict) and not isinstance(new, dict)) else None,
        "reason": r.get("reason") or "",
    }


def fetch(db, record_ids: list, limit: int = 300) -> list:
    ids = [str(x) for x in record_ids if x]
    if not ids:
        return []
    def _q(id_list):
        return db.table("audit_log").select("*").in_("record_id", id_list) \
            .order("created_at", desc=True).limit(limit).execute().data or []
    rows = _q(ids)
    # Deleted deals were logged on the parent with a snapshot; pull their own history too.
    extra = [str((r.get("old_value") or {}).get("id")) for r in rows
             if r.get("action") == "deal_deleted" and isinstance(r.get("old_value"), dict) and (r.get("old_value") or {}).get("id")]
    extra = [e for e in extra if e not in ids]
    if extra:
        seen = {r["id"] for r in rows}
        rows += [r for r in _q(extra) if r["id"] not in seen]
        rows.sort(key=lambda r: r["created_at"], reverse=True)
    return [_event(r) for r in rows[:limit]]
