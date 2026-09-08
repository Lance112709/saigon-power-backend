"""Bank-deposit check for imported commission statements.

Each confirmed upload batch is one statement. Once the REP's deposit lands in
the bank the admin records it (amount + date + note) and the CRM compares it
with the statement:

  paid_in_full           deposit == statement total
  explained_withholding  deposit == statement total - the statement's own
                         "Total Withheld" (NRG reports this on its Summary)
  short_paid / over_paid anything else — needs a look
  awaiting               no deposit recorded yet, still inside the pay window
  overdue                no deposit recorded and the pay window has passed

The pay window is the statement's Pay Date + 7 days when the statement carries
one (NRG), else 14 days after the statement was imported (statements are
emailed on or around the day the REP pays).
"""
from datetime import date, datetime, timedelta
from typing import Optional

from app.services.file_parser.provider_parsers import CUMULATIVE_GROUPS

TOLERANCE = 0.02
GRACE_AFTER_PAY_DATE = 7
GRACE_AFTER_IMPORT = 14

NEEDS_ATTENTION = {"short_paid", "over_paid", "overdue"}
# Deposit tracking started Sep 2026; older statements with no deposit recorded
# are "not_tracked" rather than flooding the page with years of "overdue".
TRACKING_FROM_MONTH = "2026-07"


def statement_figures(parsed: dict) -> dict:
    """Columns to store on the batch from a detect_and_parse() result."""
    return {
        "total_withheld": parsed.get("total_withheld"),
        "expected_pay_date": parsed.get("expected_pay_date") or None,
    }


def _to_date(v) -> Optional[date]:
    if not v:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00")).date()
    except Exception:
        try:
            return date.fromisoformat(str(v)[:10])
        except Exception:
            return None


def provider_group(batch: dict) -> Optional[str]:
    meta = batch.get("ai_column_mapping") or {}
    return meta.get("provider_group") if isinstance(meta, dict) else None


def batch_month_total(db, batch_id: str, month: str) -> Optional[float]:
    """Sum of one batch's rows for one statement month (paginated)."""
    total, off, seen = 0.0, 0, False
    while True:
        page = db.table("actual_commissions").select("raw_amount").eq("upload_batch_id", batch_id) \
            .eq("billing_month", f"{month}-01").order("id").range(off, off + 999).execute().data or []
        seen = seen or bool(page)
        total += sum(float(r.get("raw_amount") or 0) for r in page)
        if len(page) < 1000:
            break
        off += 1000
    return round(total, 2) if seen else None


def deposit_status(batch: dict, today: Optional[date] = None, month: Optional[str] = None, db=None) -> dict:
    today = today or date.today()
    month = month or statement_month(batch)
    total = batch.get("total_affinity_amount")
    total = float(total) if total is not None else None
    cumulative = provider_group(batch) in CUMULATIVE_GROUPS
    if cumulative and db is not None and month:
        # Cumulative account summaries (CleanSky) restate the whole history;
        # the REP's deposit is only the newest month's commissions.
        mt = batch_month_total(db, batch["id"], month)
        if mt is not None:
            total = mt
    withheld = float(batch.get("total_withheld") or 0)
    received = batch.get("amount_received")
    received = float(received) if received is not None else None
    expected = round(total - withheld, 2) if total is not None else None
    pay_date = _to_date(batch.get("expected_pay_date"))
    imported = _to_date(batch.get("confirmed_at") or batch.get("created_at"))
    due = (pay_date + timedelta(days=GRACE_AFTER_PAY_DATE)) if pay_date else \
          ((imported + timedelta(days=GRACE_AFTER_IMPORT)) if imported else None)

    out = {
        "statement_total": total,
        "total_withheld": withheld if withheld else None,
        "expected_deposit": expected,
        "expected_pay_date": pay_date.isoformat() if pay_date else None,
        "due_date": due.isoformat() if due else None,
        "amount_received": received,
        "received_at": batch.get("received_at"),
        "received_notes": batch.get("received_notes"),
        "received_by": batch.get("received_by"),
        "cumulative": cumulative,
        "difference": None,            # received - statement total
        "difference_vs_expected": None,  # received - (total - withheld)
        "status": "awaiting",
        "explanation": "",
    }
    if total is None:
        out["status"] = "unknown"
        out["explanation"] = "Statement total not available."
        return out
    note = (" This provider sends a cumulative account summary, so the expected deposit is "
            f"the {month} rows only." if cumulative else "")

    if received is None:
        if month and month < TRACKING_FROM_MONTH:
            out["status"] = "not_tracked"
            out["explanation"] = "Statement predates deposit tracking; record the deposit if you want it checked."
            return out
        if due and today > due:
            out["status"] = "overdue"
            out["explanation"] = f"No deposit recorded and the pay window closed {due.isoformat()}."
        else:
            out["status"] = "awaiting"
            out["explanation"] = f"Deposit expected by {due.isoformat()}." if due else "Deposit not recorded yet."
        out["explanation"] += note
        return out

    diff = round(received - total, 2)
    diff_exp = round(received - expected, 2)
    out["difference"], out["difference_vs_expected"] = diff, diff_exp
    if abs(diff) < TOLERANCE:
        out["status"] = "paid_in_full"
        out["explanation"] = "Deposit matches the statement total."
    elif withheld and abs(diff_exp) < TOLERANCE:
        out["status"] = "explained_withholding"
        out["explanation"] = (f"Deposit is ${withheld:,.2f} under the statement total — exactly the "
                              f"amount the statement reports as withheld.")
    elif diff < 0:
        out["status"] = "short_paid"
        hint = f" (statement reports ${withheld:,.2f} withheld, which does not account for it)" if withheld else ""
        out["explanation"] = f"Deposit is ${-diff:,.2f} short of the statement total{hint}."
    else:
        out["status"] = "over_paid"
        out["explanation"] = f"Deposit is ${diff:,.2f} more than the statement total."
    out["needs_attention"] = out["status"] in NEEDS_ATTENTION
    out["explanation"] += note
    return out


def statement_month(batch: dict) -> Optional[str]:
    # The rows' latest month is the statement month. The top-level
    # statement_label can come from the filename, which for NRG/Tara names
    # the month AFTER the commissions it covers.
    meta = batch.get("ai_column_mapping") or {}
    if isinstance(meta, dict):
        labels = meta.get("labels") or []
        if labels:
            return max(labels)
        if meta.get("statement_label"):
            return meta["statement_label"]
    created = _to_date(batch.get("created_at"))
    return created.strftime("%Y-%m") if created else None


def list_deposits(db, month_from: Optional[str] = None, month_to: Optional[str] = None,
                  only: Optional[str] = None) -> dict:
    """Every confirmed statement with its deposit status, newest first."""
    rows, off = [], 0
    while True:
        page = db.table("upload_batches").select("*, suppliers(name, code)") \
            .eq("status", "confirmed").order("created_at", desc=True).range(off, off + 999).execute().data or []
        rows.extend(page)
        if len(page) < 1000:
            break
        off += 1000

    today = date.today()
    items = []
    for b in rows:
        month = statement_month(b)
        if month_from and month and month < month_from:
            continue
        if month_to and month and month > month_to:
            continue
        dep = deposit_status(b, today, month, db=db)
        if only == "needs_attention" and dep["status"] not in NEEDS_ATTENTION:
            continue
        if only == "open" and dep["status"] not in (NEEDS_ATTENTION | {"awaiting"}):
            continue
        items.append({
            "id": b["id"],
            "statement_month": month,
            "provider": (b.get("suppliers") or {}).get("name") or "",
            "provider_group": ((b.get("ai_column_mapping") or {}).get("provider_group")
                               if isinstance(b.get("ai_column_mapping"), dict) else None),
            "original_filename": b.get("original_filename"),
            "imported_at": b.get("confirmed_at") or b.get("created_at"),
            "rows_imported": b.get("rows_imported"),
            **dep,
        })
    items.sort(key=lambda x: ((x["statement_month"] or ""), x["imported_at"] or ""), reverse=True)

    counts = {}
    for it in items:
        counts[it["status"]] = counts.get(it["status"], 0) + 1
    return {
        "items": items,
        "counts": counts,
        "totals": {
            "statement_total": round(sum(i["statement_total"] or 0 for i in items), 2),
            "received": round(sum(i["amount_received"] or 0 for i in items), 2),
            "not_yet_received": round(sum((i["expected_deposit"] or 0) for i in items
                                          if i["amount_received"] is None and i["status"] != "not_tracked"), 2),
            "unexplained_difference": round(sum((i["difference"] or 0) for i in items
                                                if i["status"] in ("short_paid", "over_paid")), 2),
        },
        "range": {"from": month_from, "to": month_to},
    }
