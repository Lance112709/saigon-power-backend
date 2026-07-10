"""Permanent expected-vs-paid calculation history.

Every reconciliation writes one snapshot row per account-month with the exact
math used (rate expected/paid, kwh, method). Rows are append-only: re-running
a month adds new rows and never touches old ones, so the calculation history
survives run replacement and rule changes. Latest snapshot per (esiid, month)
= most recent created_at.
"""
from typing import Optional


def persist_snapshots(db, snaps: list) -> int:
    """Insert snapshot rows in chunks. Best-effort: snapshot failures must
    never break a reconciliation run (e.g. before migration 008 is applied)."""
    if not snaps:
        return 0
    written = 0
    try:
        for i in range(0, len(snaps), 200):
            db.table("expected_commission_snapshots").insert(snaps[i:i + 200]).execute()
            written += len(snaps[i:i + 200])
    except Exception:
        return written
    return written


def snapshot_history(db, esiid: str, supplier_id: Optional[str] = None,
                     months: int = 24) -> list:
    """Latest snapshot per billing month for one account, newest first."""
    q = db.table("expected_commission_snapshots").select("*") \
        .eq("esiid", esiid).order("billing_month", desc=True) \
        .order("created_at", desc=True)
    if supplier_id:
        q = q.eq("supplier_id", supplier_id)
    rows = q.limit(months * 6).execute().data or []
    latest = {}
    for r in rows:  # newest first — first hit per month wins
        latest.setdefault(str(r["billing_month"])[:7], r)
    return [latest[k] for k in sorted(latest, reverse=True)][:months]
