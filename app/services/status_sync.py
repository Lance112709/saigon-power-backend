"""Auto-update deal statuses from provider commission statements.

Providers report account status on their statements (Budget Power's
"Cust Status" column: Active / Inactive / Going Final). On every import:

  Active       -> confirms the deal; reactivates it if the CRM had it inactive
  Going Final  -> deal STAYS active (still billing until the final bill) but
                  gets a provider_status badge — the win-back signal
  Inactive     -> deal is deactivated in the CRM

Safety rails:
  * Only trusted sources apply: providers whose status column is a real
    account status (Budget Power "Cust Status", Tara Energy "Cust Status"
    A/I, NRG Commercial "LDC Status":
    Enrolled/New Account -> active, Drop Pending -> going final,
    Dropped/Cancelled -> inactive), or manual imports where the user mapped
    the status column themselves. Discount Power's TRANSACTION_TYPE is a
    billing code, not a status — never trusted.
  * If a statement marks >50% of its accounts churned, the column is
    considered unreliable that month (Budget flagged 94% Inactive in Apr
    2026 while still paying them) — nothing auto-applies; the changes are
    returned as "pending" for a human to force-apply.
  * Every change is audit-logged with the source statement.
"""
from typing import Optional

from app.services.audit import audit

TRUSTED_STATUS_GROUPS = {"Budget Power", "NRG Commercial", "Tara Energy", "Heritage Power"}

CHURN_RELIABILITY_THRESHOLD = 0.5


def map_status(raw) -> Optional[str]:
    """Provider status text -> 'active' | 'going_final' | 'inactive' | None."""
    s = str(raw or "").strip().lower()
    if not s:
        return None
    if "going final" in s or "pending final" in s or s == "final" or "drop pending" in s:
        return "going_final"
    if any(k in s for k in ("inactive", "cancel", "closed", "churn", "terminat",
                            "move out", "moved out", "drop", "disconnect")):
        return "inactive"
    # NRG Commercial LDC statuses: Enrolled / New Account / Enrollment Pending
    if "active" in s or "enroll" in s or "new account" in s:
        return "active"
    return None


DISPLAY = {"active": "Active", "going_final": "Going Final", "inactive": "Inactive"}


def sync_statuses(db, rows: list, deals: dict, source: str, actor: str,
                  force: bool = False) -> dict:
    """Apply provider-reported statuses from parsed statement rows to deals.

    rows: normalized statement rows (need esiid / provider_status / statement_label)
    deals: reconciliation_v2.load_deals() result for the provider group
    """
    # newest status per esiid (rows are in statement order; later rows win)
    per_esiid: dict = {}
    for r in rows:
        mapped = map_status(r.get("provider_status"))
        if mapped:
            per_esiid[r["esiid"]] = (mapped, str(r.get("provider_status")).strip(),
                                     r.get("statement_label") or "")

    if not per_esiid:
        return {"applied": 0, "pending": False, "with_status": 0}

    churned = sum(1 for m, _, _ in per_esiid.values() if m in ("inactive", "going_final"))
    ratio = churned / len(per_esiid)
    summary = {
        "with_status": len(per_esiid),
        "churn_ratio": round(ratio, 3),
        "confirmed_active": 0, "reactivated": 0, "deactivated": 0,
        "going_final": 0, "unmatched": 0, "applied": 0, "pending": False,
    }

    if ratio > CHURN_RELIABILITY_THRESHOLD and not force:
        # column unreliable this month — report what WOULD happen, apply nothing
        for es, (mapped, _, _) in per_esiid.items():
            deal = deals["by_esiid"].get(es)
            if deal is None:
                summary["unmatched"] += 1
            elif mapped == "inactive" and deal["active"]:
                summary["deactivated"] += 1
            elif mapped == "going_final":
                summary["going_final"] += 1
        summary["pending"] = True
        return summary

    for es, (mapped, raw, label) in per_esiid.items():
        deal = deals["by_esiid"].get(es)
        if deal is None:
            summary["unmatched"] += 1
            continue

        display = DISPLAY[mapped]
        updates: dict = {}
        change = None

        if deal.get("provider_status") != display:
            updates.update({
                "provider_status": display,
                "provider_status_date": f"{label}-01" if label else None,
                "provider_status_source": source[:200],
            })

        if mapped == "inactive" and deal["active"]:
            updates["status" if deal["source"] == "lead_deals" else "deal_status"] = \
                "Inactive" if deal["source"] == "lead_deals" else "INACTIVE"
            change = "deactivated"
        elif mapped == "active" and not deal["active"]:
            updates["status" if deal["source"] == "lead_deals" else "deal_status"] = \
                "Active" if deal["source"] == "lead_deals" else "ACTIVE"
            change = "reactivated"
        elif mapped == "going_final":
            change = "going_final"
        elif mapped == "active":
            change = "confirmed_active"

        if updates:
            db.table(deal["source"]).update(updates).eq("id", deal["id"]).execute()
            if change in ("deactivated", "reactivated"):
                audit(db, deal["source"], deal["id"], f"status_{change}",
                      {"active": deal["active"]},
                      {"provider_status": display, "status_change": change},
                      reason=f"Provider statement: {source}", actor=actor)
            deal["provider_status"] = display
            if change == "deactivated":
                deal["active"] = False
            elif change == "reactivated":
                deal["active"] = True
            summary["applied"] += 1

        if change:
            summary[change] = summary.get(change, 0) + 1

    return summary


# Providers whose statements carry NO status column: an active deal that has
# been absent from the last 3 monthly statements is deactivated instead.
ABSENCE_SYNC_GROUPS = {"Reliant Energy", "APG&E"}


def absence_sync(db, supplier_id: str, group: str, deals: dict, actor: str,
                 current_esiids: set = None) -> dict:
    """Deactivate active deals of this provider whose ESI IDs have not appeared
    on any of the supplier's last 3 statement months. Used for providers with
    no status column; every change is audit-logged and reversible."""
    months = db.table("actual_commissions").select("billing_month") \
        .eq("supplier_id", supplier_id).order("billing_month", desc=True).limit(1).execute().data
    if not months:
        return {"deactivated": 0}
    latest = months[0]["billing_month"]
    y, m = int(latest[:4]), int(latest[5:7]) - 2
    if m < 1:
        y, m = y - 1, m + 12
    floor = f"{y}-{m:02d}-01"
    seen, off = set(), 0
    while True:
        page = db.table("actual_commissions").select("raw_esiid,billing_month") \
            .eq("supplier_id", supplier_id).gte("billing_month", floor) \
            .order("id").range(off, off + 999).execute().data
        if not page:
            break
        seen.update((r.get("raw_esiid") or "").strip() for r in page)
        if len(page) < 1000:
            break
        off += 1000
    labels = {r for r in seen if r} | set(current_esiids or ())
    n = 0
    for es, deal in deals["by_esiid"].items():
        if not deal.get("active") or es in labels:
            continue
        col = "status" if deal["source"] == "lead_deals" else "deal_status"
        val = "Inactive" if deal["source"] == "lead_deals" else "INACTIVE"
        upd = {col: val, "provider_status": "Inactive",
               "provider_status_date": latest,
               "provider_status_source": f"{group} — absent from statements since {floor[:7]}"}
        db.table(deal["source"]).update(upd).eq("id", deal["id"]).execute()
        audit(db, deal["source"], deal["id"], "status_deactivated",
              {col: "ACTIVE"}, upd,
              reason=f"{group}: no payment on the last 3 monthly statements", actor=actor)
        deal["active"] = False
        deal["provider_status"] = "Inactive"
        n += 1
    return {"deactivated": n}
