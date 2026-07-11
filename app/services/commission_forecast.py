"""Deterministic commission forecast — no ML, just the book's arithmetic.

Projects the next 12 months of provider commissions from three verifiable
inputs:

  1. the trailing average of VERIFIED received dollars (reconciliation runs),
  2. contract roll-offs — active deals whose contracts end reduce the
     projection by their estimated monthly contribution unless renewed,
  3. clawback exposure — dollars likely to be taken back (canceled accounts
     still being paid, duplicate payments).

The renewal pipeline is the flip side of (2): every rolled-off dollar is
recoverable by renewing the customer, which is what the call list drives.
"""
from datetime import date
from typing import Optional

TRAILING_MONTHS = 3
HORIZON = 12


def _month_label(y: int, m: int) -> str:
    return f"{y}-{m:02d}"


def _add_months(label: str, n: int) -> str:
    y, m = int(label[:4]), int(label[5:7])
    m += n
    y += (m - 1) // 12
    m = ((m - 1) % 12) + 1
    return _month_label(y, m)


def build_forecast(monthly_received: dict, deals: list, clawback_exposure: float,
                   today: Optional[date] = None) -> dict:
    """Pure projection.

    monthly_received: {"YYYY-MM": verified $ received}
    deals: [{active, adder, est_kwh, end_date, supplier}]
    """
    today = today or date.today()
    current = _month_label(today.year, today.month)

    complete = sorted(m for m in monthly_received if m < current)
    trailing = complete[-TRAILING_MONTHS:]
    base = round(sum(monthly_received[m] for m in trailing) / len(trailing), 2) if trailing else 0.0

    # contribution of each active deal, bucketed by the month its contract ends
    rolloff_by_month: dict = {}
    renewal_accounts_by_month: dict = {}
    for d in deals:
        if not d.get("active") or not d.get("end_date"):
            continue
        end = str(d["end_date"])[:7]
        if end < current or end > _add_months(current, HORIZON):
            continue
        contribution = float(d.get("adder") or 0) * float(d.get("est_kwh") or 0)
        if contribution <= 0:
            continue
        rolloff_by_month[end] = rolloff_by_month.get(end, 0.0) + contribution
        renewal_accounts_by_month[end] = renewal_accounts_by_month.get(end, 0) + 1

    months = []
    cumulative = 0.0
    for i in range(1, HORIZON + 1):
        m = _add_months(current, i)
        ending_prev = _add_months(m, -1)
        cumulative += rolloff_by_month.get(ending_prev, 0.0)
        months.append({
            "month": m,
            "projected": round(max(0.0, base - cumulative), 2),
            "rolling_off_this_month": round(rolloff_by_month.get(m, 0.0), 2),
            "accounts_ending": renewal_accounts_by_month.get(m, 0),
            "cumulative_rolloff": round(cumulative, 2),
        })

    at_stake = round(sum(rolloff_by_month.values()), 2)
    return {
        "base_monthly": base,
        "trailing_months": trailing,
        "months": months,
        "renewals_at_stake_12mo": at_stake,
        "renewal_accounts_12mo": sum(renewal_accounts_by_month.values()),
        "projected_12mo_no_renewals": round(sum(m["projected"] for m in months), 2),
        "projected_12mo_all_renewed": round(base * HORIZON, 2),
        "clawback_exposure": round(clawback_exposure, 2),
    }


def commission_forecast(db) -> dict:
    """Fetch inputs and build the forecast (plus per-provider trailing table)."""
    from app.services.ai_agent import _full_deal_book

    runs = db.table("reconciliation_runs").select(
        "billing_month,total_actual,suppliers(name)") \
        .like("notes", '%"engine": "v2"%').order("billing_month", desc=True) \
        .limit(1000).execute().data or []

    monthly: dict = {}
    per_provider: dict = {}
    for r in runs:
        m = str(r.get("billing_month"))[:7]
        amt = float(r.get("total_actual") or 0)
        monthly[m] = monthly.get(m, 0.0) + amt
        name = (r.get("suppliers") or {}).get("name") or "Unknown"
        per_provider.setdefault(name, {})[m] = per_provider.setdefault(name, {}).get(m, 0.0) + amt

    deals = _full_deal_book(db)

    # clawback exposure: canceled-but-paid findings + duplicate payments
    clawback = 0.0
    try:
        findings = db.table("audit_findings").select("finding_type,details,status") \
            .eq("finding_type", "churned_still_paid") \
            .in_("status", ["open", "investigating"]).limit(200).execute().data or []
        for f in findings:
            for a in (f.get("details") or {}).get("accounts", []):
                clawback += float(a.get("amount") or 0)
    except Exception:
        pass

    out = build_forecast(monthly, deals, clawback)

    today = date.today()
    current = _month_label(today.year, today.month)
    providers = []
    for name, series in per_provider.items():
        complete = sorted(m for m in series if m < current)[-TRAILING_MONTHS:]
        if not complete:
            continue
        providers.append({
            "provider": name,
            "trailing_avg": round(sum(series[m] for m in complete) / len(complete), 2),
            "last_month": complete[-1],
        })
    providers.sort(key=lambda p: -p["trailing_avg"])
    out["per_provider"] = providers
    return out
