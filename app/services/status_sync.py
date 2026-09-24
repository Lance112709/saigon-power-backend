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
    Dropped/Cancelled -> inactive, Hudson "Drop?" date -> inactive), or
    manual imports where the user mapped the status column themselves.
    Discount Power's TRANSACTION_TYPE is a billing code, not a status —
    never trusted; Iron Horse's "Account Type" is Residential/Commercial.
  * Every provider additionally runs the absence rule (see absence_sync
    below): an active deal missing from the last 3 statement months is
    deactivated, with grace for new contracts and a mass-churn hold.
  * If a statement marks >50% of its accounts churned, the column is
    considered unreliable that month (Budget flagged 94% Inactive in Apr
    2026 while still paying them) — nothing auto-applies; the changes are
    returned as "pending" for a human to force-apply.
  * Every change is audit-logged with the source statement.
"""
from typing import Optional

from app.services.audit import audit
from app.services.file_parser.provider_parsers import CRM_PROVIDER_GROUPS

TRUSTED_STATUS_GROUPS = {"Budget Power", "NRG Commercial", "Tara Energy", "Heritage Power", "Hudson Energy"}

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
        "stale_skipped": 0,
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

        # Back-filling history: never let an older statement overwrite a status
        # the CRM already holds from a newer one.
        have = str(deal.get("provider_status_date") or "")[:10]
        if label and have and f"{label}-01" < have:
            summary["stale_skipped"] += 1
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
            try:
                db.table(deal["source"]).update(updates).eq("id", deal["id"]).execute()
            except Exception as e:
                # The duplicate-ESIID guard refuses to reactivate a deal whose
                # ESIID is still ACTIVE under another provider (a switch the
                # CRM hasn't caught up with). Record it and keep syncing.
                summary["blocked"] = summary.get("blocked", 0) + 1
                audit(db, deal["source"], deal["id"], "status_sync_blocked",
                      {"active": deal["active"]},
                      {"provider_status": display, "attempted": change, "error": str(e)[:200]},
                      reason=f"Provider statement: {source}", actor=actor)
                continue
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


# ---------------------------------------------------------------------------
# Absence rule — every provider (Lance, 2026-09-23: "add the same rule to all
# REP statements"). An ACTIVE deal whose ESI ID has not been paid on any of
# the provider's last ABSENCE_WINDOW_MONTHS full statement months is
# deactivated. Providers with a real status column get both signals; the
# explicit status runs after this and wins for anything it names.
#
# Match by ESI, never by name. Before a deal is dropped the meter is checked
# across EVERY provider's statements in the window:
#   * paid by nobody                      -> churned: deactivate
#   * paid by another REP, and the CRM already holds an ACTIVE deal for the
#     meter under that switch (e.g. the Budget Power -> Direct Energy book
#     transfer left the old Budget deals Active)  -> stale twin: deactivate
#   * paid by another REP, no other deal  -> the customer switched but the CRM
#     never recorded it: left ACTIVE and reported as "switched" so the deal's
#     provider can be relabelled instead of losing the customer.
#
# Guards (all changes audit-logged and reversible):
#   * full months only — a statement month with fewer rows than
#     STUB_FRACTION of the provider's median month (last 6 months) is a stub
#     (Iron Horse's 10-row May 2026, Chariot's 9-row April) and does not
#     count toward the window, though the payments it carries still count
#     as "seen".
#   * thin history — nothing runs unless ABSENCE_WINDOW_MONTHS full months
#     are on file, so a newly imported provider cannot wipe its book.
#   * grace period — a contract that started within ABSENCE_GRACE_MONTHS of
#     the latest statement is skipped: first payments arrive 1-3 months in
#     arrears, so a new enrollment is expected to be missing.
#   * mass-churn hold — if more than CHURN_RELIABILITY_THRESHOLD of the
#     provider's active deals would drop at once, nothing applies; the count
#     is returned as "pending" for an admin to force-apply (Uploads page),
#     same as the status column.
# ---------------------------------------------------------------------------
ABSENCE_SYNC_GROUPS = {
    "Discount Power/Cirro", "NRG Commercial", "Tara Energy", "Reliant Energy", "APG&E",
    "Iron Horse", "Chariot", "Budget Power", "CleanSky", "Hudson Energy", "Heritage Power",
}
ABSENCE_WINDOW_MONTHS = 3
ABSENCE_GRACE_MONTHS = 4
STUB_FRACTION = 0.10   # real stubs run 3-7% of a normal month; a shrinking book (Budget after the DE transfer) is ~20%
_LOOKBACK_MONTHS = 9
_MEDIAN_OVER = 6       # median of the most recent full-or-stub months, so an old bigger book doesn't skew it


def _shift_month(ym: str, delta: int) -> str:
    """'2026-09-01' shifted by delta months -> first-of-month ISO date."""
    y, m = int(ym[:4]), int(ym[5:7]) + delta
    while m < 1:
        y, m = y - 1, m + 12
    while m > 12:
        y, m = y + 1, m - 12
    return f"{y}-{m:02d}-01"


def _full_statement_months(db, supplier_id: str) -> list:
    """Newest-first list of the supplier's statement months that are not stubs."""
    latest = db.table("actual_commissions").select("billing_month") \
        .eq("supplier_id", supplier_id).order("billing_month", desc=True).limit(1).execute().data
    if not latest:
        return []
    counts = {}
    ym = latest[0]["billing_month"][:10]
    for _ in range(_LOOKBACK_MONTHS):
        r = db.table("actual_commissions").select("id", count="exact", head=True) \
            .eq("supplier_id", supplier_id).eq("billing_month", ym).execute()
        if r.count:
            counts[ym] = r.count
        ym = _shift_month(ym, -1)
    if not counts:
        return []
    recent = [c for _, c in sorted(counts.items(), reverse=True)[:_MEDIAN_OVER]]
    vals = sorted(recent)
    median = vals[len(vals) // 2]
    return [m for m, c in sorted(counts.items(), reverse=True) if c >= STUB_FRACTION * median]


def _chunks(items, n=150):
    items = list(items)
    for i in range(0, len(items), n):
        yield items[i:i + n]


def absence_sync(db, supplier_id: str, group: str, deals: dict, actor: str,
                 current_esiids: set = None, force: bool = False,
                 dry_run: bool = False) -> dict:
    """Deactivate active deals of this provider whose ESI IDs have not appeared
    on any of the supplier's last ABSENCE_WINDOW_MONTHS full statement months
    and are not being paid by another provider either (see module notes).
    current_esiids: ESI IDs on the statement being imported right now (its
    rows are not in actual_commissions yet). dry_run returns the candidates
    without writing."""
    full = _full_statement_months(db, supplier_id)
    if len(full) < ABSENCE_WINDOW_MONTHS:
        return {"deactivated": 0, "pending": False,
                "skipped": f"only {len(full)} full statement month(s) on file"}
    latest, floor = full[0], full[ABSENCE_WINDOW_MONTHS - 1]
    grace = _shift_month(latest, -ABSENCE_GRACE_MONTHS)

    seen, off = set(), 0
    while True:
        page = db.table("actual_commissions").select("raw_esiid") \
            .eq("supplier_id", supplier_id).gte("billing_month", floor) \
            .order("id").range(off, off + 999).execute().data
        if not page:
            break
        seen.update((r.get("raw_esiid") or "").strip() for r in page)
        if len(page) < 1000:
            break
        off += 1000
    labels = {r for r in seen if r} | set(current_esiids or ())

    active_total, missing, in_grace, other_group = 0, [], 0, 0
    for es, deal in deals["by_esiid"].items():
        if not deal.get("active"):
            continue
        # load_deals() widens a group for reconciliation (Budget Power statements
        # also match the Direct Energy deals of the transferred book). Only deals
        # whose own provider belongs to this group are judged by its statements.
        prov = (deal.get("provider") or deal.get("supplier") or "").strip().lower()
        if prov and CRM_PROVIDER_GROUPS.get(prov, group) != group:
            other_group += 1
            continue
        active_total += 1
        if es in labels:
            continue
        start = str(deal.get("start") or "")[:10]
        if start and start >= grace:
            in_grace += 1
            continue
        missing.append((es, deal))

    # Cross-provider check on the missing meters: who else paid them, and does
    # the CRM already carry an ACTIVE deal for them under another provider?
    sup_names = {s["id"]: s["name"] for s in
                 (db.table("suppliers").select("id,name").execute().data or [])}
    paid_by, twins = {}, set()
    for batch in _chunks(es for es, _ in missing):
        for r in db.table("actual_commissions").select("raw_esiid,supplier_id") \
                .in_("raw_esiid", batch).gte("billing_month", floor) \
                .neq("supplier_id", supplier_id).execute().data or []:
            paid_by.setdefault(r["raw_esiid"], set()).add(sup_names.get(r["supplier_id"], "another provider"))
        ids = {d["id"] for _, d in missing}
        for r in db.table("crm_deals").select("id,esiid").in_("esiid", batch) \
                .eq("deal_status", "ACTIVE").execute().data or []:
            if r["id"] not in ids:
                twins.add(r["esiid"])
        for r in db.table("lead_deals").select("id,esiid").in_("esiid", batch) \
                .eq("status", "Active").execute().data or []:
            if r["id"] not in ids:
                twins.add(r["esiid"])

    candidates, switched = [], []
    for es, deal in missing:
        others = sorted(paid_by.get(es, ()))
        if others and es not in twins:
            switched.append({"source": deal["source"], "id": deal["id"], "esiid": es,
                             "name": deal.get("name") or "", "paid_by": ", ".join(others)})
            continue
        candidates.append((es, deal, others))

    summary = {"deactivated": 0, "pending": False, "window": floor[:7], "latest": latest[:7],
               "active": active_total, "in_grace": in_grace, "other_group": other_group,
               "candidates": len(candidates),
               "churned": sum(1 for _, _, o in candidates if not o),
               "stale_twins": sum(1 for _, _, o in candidates if o),
               "switched": switched}
    if dry_run:
        summary["would_deactivate"] = [
            {"source": d["source"], "id": d["id"], "esiid": es, "start": d.get("start"),
             "name": d.get("name") or "", "address": d.get("addr_n") or "",
             "now_paid_by": ", ".join(o)}
            for es, d, o in candidates]
        return summary
    if candidates and not force and active_total \
            and len(candidates) / active_total > CHURN_RELIABILITY_THRESHOLD:
        summary["pending"] = True
        summary["held"] = len(candidates)
        return summary

    for es, deal, others in candidates:
        col = "status" if deal["source"] == "lead_deals" else "deal_status"
        val = "Inactive" if deal["source"] == "lead_deals" else "INACTIVE"
        why = (f"now paid by {', '.join(others)} (CRM has an active deal there)" if others
               else f"no payment from any provider on the last {ABSENCE_WINDOW_MONTHS} statements")
        upd = {col: val, "provider_status": "Inactive",
               "provider_status_date": latest,
               "provider_status_source": f"{group} — absent from statements since {floor[:7]}; {why}"[:200]}
        db.table(deal["source"]).update(upd).eq("id", deal["id"]).execute()
        audit(db, deal["source"], deal["id"], "status_deactivated",
              {col: "ACTIVE"}, upd,
              reason=f"{group}: {why}" + (" (force-applied)" if force else ""),
              actor=actor)
        deal["active"] = False
        deal["provider_status"] = "Inactive"
        summary["deactivated"] += 1
    return summary
