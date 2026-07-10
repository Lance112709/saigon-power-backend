"""Durable exception-case workflow for the audit engine.

Reconciliation runs are replaced on every re-import, so their items can't hold
workflow state. Each non-matched item is mirrored into `exception_cases`,
upserted on the natural key (supplier, month, esiid, issue_type):

  * a NEW discrepancy opens a case with a priority score, estimated dollars
    lost, and a recommended action;
  * a case seen again keeps its workflow status, notes, recovered dollars and
    dispute link while its numbers refresh;
  * a case whose discrepancy disappears from the latest run auto-resolves
    ("self-corrected on latest statement").

Every workflow change is audit-logged.
"""
from datetime import datetime, timezone
from typing import Optional

from app.services.audit import audit
from app.services.reconciliation_v2 import fetch_all

WORKFLOW_STATUSES = ("open", "investigating", "waiting_on_provider",
                     "resolved", "recovered", "ignored")
OPEN_STATUSES = ("open", "investigating", "waiting_on_provider")

_SEV_WEIGHT = {"critical": 4.0, "high": 3.0, "medium": 2.0, "low": 1.0}

_RECOMMENDED = {
    "missing": "Confirm the account's status with the provider; if active, request the missing payment. If churned, update the CRM and start win-back.",
    "short_paid": "Dispute the rate with the provider and request a true-up to the contracted adder.",
    "over_paid": "Verify the duplicate — expect a clawback next statement or notify the provider proactively.",
    "unexpected": "Identify the account: link the ESIID to a deal, or confirm the churn status and update the CRM.",
}


def _estimated_loss(item: dict) -> float:
    status = item.get("status")
    if status == "missing":
        return float(item.get("expected_amount") or 0)
    if status == "short_paid":
        return max(0.0, -float(item.get("discrepancy_amount") or 0))
    return 0.0


def _priority(severity: str, loss: float) -> float:
    return round(_SEV_WEIGHT.get(severity or "low", 1.0) * max(loss, 1.0), 2)


def upsert_cases_from_run(db, run_id: str, supplier_id: str, label: str,
                          deals: dict = None, actor: str = "system") -> dict:
    """Mirror a run's non-matched items into durable cases. Best-effort by
    design: called inside the import pipeline, it must never break an import
    (e.g. before migration 008 exists)."""
    try:
        return _upsert_cases(db, run_id, supplier_id, label, deals, actor)
    except Exception as e:  # pragma: no cover - defensive
        try:
            audit(db, "exception_cases", run_id, "case_sync_failed", None,
                  {"error": str(e)[:300]}, actor=actor)
        except Exception:
            pass
        return {"created": 0, "updated": 0, "auto_resolved": 0, "error": str(e)[:200]}


def _upsert_cases(db, run_id: str, supplier_id: str, label: str,
                  deals: Optional[dict], actor: str) -> dict:
    month = f"{label}-01"
    by_esiid = (deals or {}).get("by_esiid", {})
    items = fetch_all(db, "reconciliation_items",
                      "id,esiid,status,severity,expected_amount,actual_amount,"
                      "discrepancy_amount,resolution_notes",
                      filters=[("eq", ("reconciliation_run_id", run_id))])
    open_issues = [it for it in items if it.get("status") != "matched"]

    existing = {}
    for c in fetch_all(db, "exception_cases", "*",
                       filters=[("eq", ("supplier_id", supplier_id)),
                                ("eq", ("billing_month", month))]):
        existing[(c["esiid"], c["issue_type"])] = c

    now = datetime.now(timezone.utc).isoformat()
    created = updated = 0
    seen_keys = set()
    case_ids_by_item = {}

    for it in open_issues:
        key = (it["esiid"], it["status"])
        seen_keys.add(key)
        deal = by_esiid.get(it["esiid"]) or {}
        loss = _estimated_loss(it)
        fields = {
            "priority_score": _priority(it.get("severity"), loss),
            "estimated_loss": round(loss, 2),
            "explanation": (it.get("resolution_notes") or "").replace("ROOT CAUSE: ", ""),
            "recommended_action": _RECOMMENDED.get(it["status"], "Review this account."),
            "customer_name": deal.get("name") or None,
            "agent": deal.get("agent") or None,
            "last_seen_run_id": run_id,
            "last_seen_at": now,
            "updated_at": now,
        }
        cur = existing.get(key)
        if cur:
            db.table("exception_cases").update(fields).eq("id", cur["id"]).execute()
            case_ids_by_item[it["id"]] = cur["id"]
            updated += 1
        else:
            new = db.table("exception_cases").insert({
                **fields,
                "supplier_id": supplier_id, "billing_month": month,
                "esiid": it["esiid"], "issue_type": it["status"],
                "workflow_status": "open",
                "first_seen_run_id": run_id,
            }).execute().data[0]
            case_ids_by_item[it["id"]] = new["id"]
            created += 1

    for item_id, case_id in case_ids_by_item.items():
        db.table("reconciliation_items").update({"case_id": case_id}) \
            .eq("id", item_id).execute()

    # discrepancies that no longer reproduce self-resolve
    auto_resolved = 0
    for key, c in existing.items():
        if key in seen_keys or c.get("workflow_status") not in OPEN_STATUSES:
            continue
        db.table("exception_cases").update({
            "workflow_status": "resolved",
            "notes": ((c.get("notes") or "") +
                      f"\n[{now[:10]}] Self-corrected on the latest statement run.").strip(),
            "updated_at": now,
        }).eq("id", c["id"]).execute()
        audit(db, "exception_cases", c["id"], "case_auto_resolved",
              {"workflow_status": c.get("workflow_status")},
              {"workflow_status": "resolved"},
              reason="Discrepancy no longer present on latest reconciliation", actor=actor)
        auto_resolved += 1

    return {"created": created, "updated": updated, "auto_resolved": auto_resolved}


def list_cases(db, workflow_status: Optional[str] = None,
               supplier_id: Optional[str] = None,
               billing_month: Optional[str] = None,
               issue_type: Optional[str] = None,
               min_loss: Optional[float] = None,
               limit: int = 500) -> list:
    filters = []
    if workflow_status == "any_open":
        filters.append(("in_", ("workflow_status", list(OPEN_STATUSES))))
    elif workflow_status:
        filters.append(("eq", ("workflow_status", workflow_status)))
    if supplier_id:
        filters.append(("eq", ("supplier_id", supplier_id)))
    if billing_month:
        filters.append(("eq", ("billing_month", f"{billing_month[:7]}-01")))
    if issue_type:
        filters.append(("eq", ("issue_type", issue_type)))
    if min_loss is not None:
        filters.append(("gte", ("estimated_loss", min_loss)))
    rows = fetch_all(db, "exception_cases", "*", filters=filters)
    rows.sort(key=lambda c: -(c.get("priority_score") or 0))
    return rows[:limit]


_PATCHABLE = {"workflow_status", "notes", "recovered_amount", "dispute_id"}


def update_case(db, case_id: str, patch: dict, actor: str) -> dict:
    cur = db.table("exception_cases").select("*").eq("id", case_id) \
        .limit(1).execute().data
    if not cur:
        raise ValueError("Case not found")
    cur = cur[0]
    fields = {k: v for k, v in patch.items() if k in _PATCHABLE}
    if "workflow_status" in fields and fields["workflow_status"] not in WORKFLOW_STATUSES:
        raise ValueError(f"workflow_status must be one of {WORKFLOW_STATUSES}")
    fields["updated_at"] = datetime.now(timezone.utc).isoformat()
    db.table("exception_cases").update(fields).eq("id", case_id).execute()
    audit(db, "exception_cases", case_id, "case_updated",
          {k: cur.get(k) for k in fields if k != "updated_at"},
          {k: v for k, v in fields.items() if k != "updated_at"}, actor=actor)

    # keep the classic reconciliation view truthful
    if fields.get("workflow_status") in ("resolved", "recovered", "ignored"):
        note = f"Case {fields['workflow_status']} by {actor}"
        if patch.get("notes"):
            note += f": {patch['notes']}"
        db.table("reconciliation_items").update({
            "is_resolved": True, "resolved_at": fields["updated_at"],
            "resolution_notes": note,
        }).eq("case_id", case_id).execute()
    return {**cur, **fields}
