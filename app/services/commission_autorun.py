"""Scheduled agent-commission calculation.

Runs the same engine the Calculate button uses for the current month and the
two before it, so 'calculated' rows are always fresh when Lance opens the
page. Approved / closed-out / paid rows are locked and never touched.
"""
import logging
from datetime import date

from app.db.client import get_client
from app.services.agent_commission_engine import calculate_month, save_month_results

log = logging.getLogger("saigon.commission_autorun")
ACTOR = "scheduler (auto-calculate)"


def months_to_run(today: date = None, back: int = 2) -> list:
    today = today or date.today()
    out, y, m = [], today.year, today.month
    for _ in range(back + 1):
        out.append((y, m))
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return out


def auto_calculate(back: int = 2, today: date = None) -> dict:
    db = get_client()
    summary = {}
    for y, m in months_to_run(today, back):
        try:
            result = calculate_month(db, y, m)
            if not result["agents"]:
                summary[f"{y}-{m:02d}"] = {"skipped": "nothing to pay"}
                continue
            saved, locked = save_month_results(db, y, m, result, performed_by=ACTOR)
            summary[f"{y}-{m:02d}"] = {"calculated": len(saved), "locked": len(locked),
                                       "total": round(sum(s["total_commission"] for s in saved), 2)}
        except Exception as e:  # one bad month must not stop the others
            log.exception("auto-calculate %s-%02d failed", y, m)
            summary[f"{y}-{m:02d}"] = {"error": str(e)[:200]}
    return summary
