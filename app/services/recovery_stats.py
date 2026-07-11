"""Learning from dispute outcomes — deterministic, no LLM.

Every resolved dispute teaches the system how a provider behaves: how often
they pay, how much of a claim they honor, and which issue types recover.
Those stats feed back into the audit workflow:

  * new exception cases for a provider with a strong recovery record get a
    priority boost (chasing them demonstrably pays);
  * findings and cases carry a plain-English hint ("past disputes with this
    provider recovered 82% of claimed dollars") so the user knows whether a
    dispute is worth the effort before building one.
"""
from typing import Optional

from app.services.reconciliation_v2 import fetch_all

_CLOSED = ("recovered", "rejected")


def provider_recovery_stats(db) -> dict:
    """supplier_id -> outcome stats from every dispute that got a response."""
    disputes = fetch_all(db, "disputes",
                         "id,supplier_id,status,total_claimed,total_recovered")
    stats: dict = {}
    for d in disputes:
        s = stats.setdefault(d["supplier_id"], {
            "disputes_sent": 0, "disputes_closed": 0, "disputes_recovered": 0,
            "claimed_closed": 0.0, "recovered": 0.0, "recovery_rate": None,
        })
        if d["status"] != "draft":
            s["disputes_sent"] += 1
        if d["status"] in _CLOSED:
            s["disputes_closed"] += 1
            s["claimed_closed"] += float(d.get("total_claimed") or 0)
            s["recovered"] += float(d.get("total_recovered") or 0)
            if d["status"] == "recovered":
                s["disputes_recovered"] += 1
    for s in stats.values():
        if s["claimed_closed"] > 0:
            s["recovery_rate"] = round(s["recovered"] / s["claimed_closed"], 3)
        s["claimed_closed"] = round(s["claimed_closed"], 2)
        s["recovered"] = round(s["recovered"], 2)
    return stats


def recovery_hint(stats: dict, supplier_id: str) -> Optional[str]:
    """Plain-English history line for a provider (None until outcomes exist)."""
    s = stats.get(supplier_id)
    if not s or not s["disputes_closed"]:
        return None
    rate = s["recovery_rate"] or 0.0
    if rate >= 0.75:
        verdict = "this provider almost always pays — dispute it"
    elif rate >= 0.4:
        verdict = "this provider often pays — worth disputing"
    elif rate > 0:
        verdict = "this provider pays reluctantly — dispute the big ones"
    else:
        verdict = "this provider has rejected past disputes — escalate beyond email"
    return (f"History: {s['disputes_closed']} closed dispute(s), "
            f"{rate * 100:.0f}% of claimed dollars recovered — {verdict}.")


def priority_multiplier(stats: dict, supplier_id: str) -> float:
    """Boost case priority for providers whose disputes historically pay.
    1.0 with no history; up to 1.5 for providers that fully pay claims."""
    s = stats.get(supplier_id)
    if not s or not s["disputes_closed"] or s["recovery_rate"] is None:
        return 1.0
    return round(1.0 + min(s["recovery_rate"], 1.0) * 0.5, 3)
