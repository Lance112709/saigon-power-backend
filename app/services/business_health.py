"""Business Health — the numbers a growing brokerage runs on.

Everything here is computed from VERIFIED data: actual provider payments
(actual_commissions), reconciliation-v2 runs, provider-reported statuses,
and the audit log. Results are cached in-process for 10 minutes.

Panels:
  1. growth      — accounts gained vs lost per month (statement membership)
  2. book_value  — ARPA, churn-derived lifetime, LTV, total book valuation
  3. providers   — revenue share, effective rate, pay accuracy, reliability
  4. winback     — Going Final / provider-inactive accounts worth saving
  5. chasing     — open reconciliation issues in dollars
  6. agents      — per-agent adds, book, churn-risk, last payout
"""
import json
import time
from datetime import datetime, timezone

from app.services.reconciliation_v2 import fetch_all
from app.services.agent_commission_engine import norm_name

_cache: dict = {"at": 0.0, "data": None}
CACHE_TTL_SECONDS = 600


# ── Pure math (unit-tested) ───────────────────────────────────────────────────

def month_over_month(prev_esiids: set, cur_esiids: set) -> dict:
    """Accounts gained/lost between two consecutive statement months."""
    return {"gained": len(cur_esiids - prev_esiids),
            "lost": len(prev_esiids - cur_esiids),
            "net": len(cur_esiids) - len(prev_esiids)}


def confirmed_flow(prev2: set, prev: set, cur: set, nxt: set) -> dict:
    """Growth for month `cur`, robust to billing-cycle bounce.

    An account skipping ONE statement month and returning is not churn —
    bills straddle month boundaries. So:
      lost   = was paid in prev, absent in BOTH cur and nxt (gone for good)
      gained = paid in cur, absent in both prev and prev2 (genuinely new)
    """
    lost = prev - cur - nxt
    gained = cur - prev - prev2
    return {"gained": len(gained), "lost": len(lost), "net": len(gained) - len(lost)}


def ltv_math(total_received: float, account_months: int,
             avg_monthly_churn: float) -> dict:
    """ARPA, expected lifetime and LTV from verified payment history.

    avg_monthly_churn is a fraction (0.03 = 3%/month). Guard rails keep the
    lifetime estimate between 6 and 72 months so one noisy month can't
    produce a silly valuation."""
    arpa = (total_received / account_months) if account_months else 0.0
    churn = max(avg_monthly_churn, 1e-6)
    lifetime = min(max(1.0 / churn, 6.0), 72.0)
    return {"arpa": round(arpa, 2),
            "monthly_churn_pct": round(avg_monthly_churn * 100, 2),
            "expected_lifetime_months": round(lifetime, 1),
            "ltv_per_account": round(arpa * lifetime, 2)}


# ── Data assembly ─────────────────────────────────────────────────────────────

def _v2_runs(db) -> list:
    return db.table("reconciliation_runs").select(
        "id,billing_month,supplier_id,total_actual,total_expected,matched_count,"
        "short_paid_count,over_paid_count,missing_count,unexpected_count,suppliers(name,code)"
    ).like("notes", '%"engine": "v2"%').limit(1000).execute().data or []


def _month_rows(db, label: str) -> list:
    return fetch_all(db, "actual_commissions", "raw_esiid,supplier_id,raw_amount,raw_kwh",
                     filters=[("eq", ("billing_month", f"{label}-01"))])


def build_business_health(db, months_back: int = 7) -> dict:
    now = time.time()
    if _cache["data"] is not None and now - _cache["at"] < CACHE_TTL_SECONDS:
        return _cache["data"]

    runs = _v2_runs(db)
    sup_names = {r["supplier_id"]: (r.get("suppliers") or {}).get("name", "?") for r in runs}
    all_months = sorted({r["billing_month"][:7] for r in runs})
    months = all_months[-months_back:]

    # month -> supplier -> esiid set (+ amounts/kwh for scorecards)
    membership: dict = {}
    amounts: dict = {}
    kwhs: dict = {}
    for m in months:
        membership[m] = {}
        for row in _month_rows(db, m):
            sid = row["supplier_id"]
            membership[m].setdefault(sid, set()).add(row["raw_esiid"])
            amounts[(m, sid)] = amounts.get((m, sid), 0.0) + float(row.get("raw_amount") or 0)
            k = float(row.get("raw_kwh") or 0)
            kwhs[(m, sid)] = kwhs.get((m, sid), 0.0) + k

    # 1) growth: confirmed gains/losses per provider. A month is evaluated
    # only with its neighbors present (billing-cycle bounce is not churn) and
    # only for providers that reported in all the months involved.
    growth = []
    churn_samples = []
    for i in range(1, len(months) - 1):
        prev2 = months[i - 2] if i >= 2 else None
        prev, cur, nxt = months[i - 1], months[i], months[i + 1]
        g = {"month": cur, "gained": 0, "lost": 0, "net": 0, "by_provider": {}, "not_reporting": []}
        for sid in set(membership[prev]) | set(membership[cur]):
            name = sup_names.get(sid, "?")
            if sid not in membership[cur]:
                g["not_reporting"].append(name)
                continue
            if sid not in membership[prev] or sid not in membership.get(nxt, {}):
                continue  # can't confirm without neighbors
            cf = confirmed_flow(
                membership.get(prev2, {}).get(sid, set()) if prev2 else set(),
                membership[prev][sid], membership[cur][sid], membership[nxt][sid])
            g["gained"] += cf["gained"]
            g["lost"] += cf["lost"]
            g["net"] += cf["net"]
            g["by_provider"][name] = cf
            base = len(membership[prev][sid])
            if base >= 20:
                churn_samples.append(cf["lost"] / base)
        growth.append(g)

    # 2) book value from verified payments
    total_received = sum(r["total_actual"] or 0 for r in runs)
    account_months = sum((r["matched_count"] or 0) + (r["short_paid_count"] or 0)
                         + (r["over_paid_count"] or 0) + (r["unexpected_count"] or 0)
                         for r in runs)
    avg_churn = (sum(churn_samples) / len(churn_samples)) if churn_samples else 0.03
    book = ltv_math(total_received, account_months, avg_churn)
    latest = months[-1] if months else None
    prev_month = months[-2] if len(months) > 1 else None
    paying_now = 0
    for sid in sup_names:
        cur_set = membership.get(latest, {}).get(sid)
        if cur_set is None and prev_month:
            cur_set = membership.get(prev_month, {}).get(sid)  # statement not in yet
        paying_now += len(cur_set or set())
    book.update({
        "paying_accounts": paying_now,
        "book_value": round(paying_now * book["ltv_per_account"], 0),
        "total_received_alltime": round(total_received, 2),
    })

    # 3) provider scorecards (last 3 reported months per provider)
    latest_run_by_sup: dict = {}
    for r in sorted(runs, key=lambda x: x["billing_month"], reverse=True):
        latest_run_by_sup.setdefault(r["supplier_id"], r)
    recent3 = months[-3:]
    total_recent = sum(amounts.get((m, sid), 0) for m in recent3 for sid in sup_names) or 1
    providers = []
    for sid, name in sup_names.items():
        amt = sum(amounts.get((m, sid), 0) for m in recent3)
        kwh = sum(kwhs.get((m, sid), 0) for m in recent3)
        lr = latest_run_by_sup.get(sid)
        paid_items = ((lr["matched_count"] or 0) + (lr["short_paid_count"] or 0)
                      + (lr["over_paid_count"] or 0)) if lr else 0
        missing_months = sum(1 for m in months if sid not in membership.get(m, {}))
        providers.append({
            "name": name,
            "share_pct": round(amt / total_recent * 100, 1),
            "received_3mo": round(amt, 2),
            "effective_mills": round(amt / kwh * 1000, 2) if kwh else None,
            "pay_accuracy_pct": round((lr["matched_count"] or 0) / paid_items * 100, 1) if paid_items else None,
            "latest_month": lr["billing_month"][:7] if lr else None,
            "months_not_reporting": missing_months,
            "accounts_latest": len(membership.get(latest, {}).get(sid, set())) if latest else 0,
        })
    providers.sort(key=lambda p: -p["received_3mo"])

    # 4) win-back queue: Going Final, or provider says Inactive while CRM active
    winback = []
    for table, active_field, active_val, cols in (
        ("lead_deals", "status", "Active",
         "id,esiid,adder,est_kwh,sales_agent,provider_status,provider_status_date,status,supplier,leads(first_name,last_name,phone)"),
        ("crm_deals", "deal_status", "ACTIVE",
         "id,esiid,adder,meter_type,sales_agent,provider_status,provider_status_date,deal_status,provider,crm_customers(full_name,phone)"),
    ):
        rows = db.table(table).select(cols) \
            .in_("provider_status", ["Going Final", "Inactive"]).eq(active_field, active_val) \
            .limit(500).execute().data or []
        for d in rows:
            if table == "lead_deals":
                who = d.get("leads") or {}
                nm = f"{who.get('first_name','')} {who.get('last_name','')}".strip()
                prov = d.get("supplier")
                kwh = float(d.get("est_kwh") or 1100)
            else:
                who = d.get("crm_customers") or {}
                nm = who.get("full_name") or ""
                prov = d.get("provider")
                kwh = 2500.0 if d.get("meter_type") == "Commercial" else 1100.0
            est = round(float(d.get("adder") or 0) * kwh, 2)
            winback.append({
                "source": table, "deal_id": d["id"], "customer": nm,
                "phone": who.get("phone"), "provider": prov,
                "agent": d.get("sales_agent") or "", "esiid": d.get("esiid"),
                "provider_status": d.get("provider_status"),
                "since": d.get("provider_status_date"),
                "monthly_value": est,
            })
    winback.sort(key=lambda w: -w["monthly_value"])
    recovered = db.table("audit_log").select("id", count="exact") \
        .eq("action", "status_reactivated") \
        .gte("created_at", datetime.now(timezone.utc).replace(day=1).isoformat()) \
        .limit(1).execute()
    winback_summary = {
        "count": len(winback),
        "monthly_value_at_risk": round(sum(w["monthly_value"] for w in winback), 2),
        "recovered_this_month": recovered.count or 0,
        "queue": winback[:20],
    }

    # 5) dollars being chased (unresolved financial items on each latest run)
    missing_d = underpaid_d = 0.0
    open_items = 0
    for r in latest_run_by_sup.values():
        items = fetch_all(db, "reconciliation_items", "status,discrepancy_amount",
                          filters=[("eq", ("reconciliation_run_id", r["id"])),
                                   ("eq", ("is_resolved", False)),
                                   ("in_", ("status", ["missing", "short_paid"]))])
        for it in items:
            open_items += 1
            d = float(it.get("discrepancy_amount") or 0)
            if it["status"] == "missing":
                missing_d += -d
            else:
                underpaid_d += -d
    chasing = {"missing_dollars": round(missing_d, 2),
               "underpaid_dollars": round(underpaid_d, 2),
               "open_items": open_items,
               "total": round(missing_d + underpaid_d, 2)}

    # 6) agent scoreboard
    month_start = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0).isoformat()
    added: dict = {}
    for d in fetch_all(db, "lead_deals", "sales_agent,created_at,status"):
        if (d.get("created_at") or "") >= month_start and d.get("sales_agent"):
            k = norm_name(d["sales_agent"])
            added[k] = added.get(k, 0) + 1
    book_by_agent: dict = {}
    risk_by_agent: dict = {}
    display: dict = {}
    for table, active_field, active_val in (("lead_deals", "status", "Active"),
                                            ("crm_deals", "deal_status", "ACTIVE")):
        for d in fetch_all(db, table, f"sales_agent,{active_field},provider_status"):
            if d.get(active_field) != active_val or not d.get("sales_agent"):
                continue
            k = norm_name(d["sales_agent"])
            display.setdefault(k, d["sales_agent"].strip())
            book_by_agent[k] = book_by_agent.get(k, 0) + 1
            if d.get("provider_status") in ("Going Final", "Inactive"):
                risk_by_agent[k] = risk_by_agent.get(k, 0) + 1
    payouts = db.table("agent_commissions").select("agent_name,total_commission,month,year") \
        .order("year", desc=True).order("month", desc=True).limit(100).execute().data or []
    last_payout: dict = {}
    for p in payouts:
        k = norm_name(p["agent_name"])
        last_payout.setdefault(k, p["total_commission"])
    agents = [{
        "agent": display[k], "book": v,
        "added_this_month": added.get(k, 0),
        "at_risk": risk_by_agent.get(k, 0),
        "last_payout": last_payout.get(k),
    } for k, v in book_by_agent.items()]
    agents.sort(key=lambda a: -a["book"])

    data = {
        "computed_at": datetime.now(timezone.utc).isoformat(),
        "months": months,
        "growth": growth,
        "book": book,
        "providers": providers,
        "winback": winback_summary,
        "chasing": chasing,
        "agents": agents[:10],
    }
    _cache.update({"at": now, "data": data})
    return data
