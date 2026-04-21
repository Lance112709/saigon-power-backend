from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Optional
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from app.auth.deps import require_admin, UserContext
from app.db.client import get_client
from app.services.ai_agent import (
    get_dashboard, run_full_scan, generate_daily_report,
    generate_monthly_report, _resolve_alert
)

router = APIRouter()


@router.get("/dashboard")
def ai_dashboard(user: UserContext = Depends(require_admin)):
    return get_dashboard()


@router.post("/scan")
def manual_scan(user: UserContext = Depends(require_admin)):
    return run_full_scan()


@router.get("/alerts")
def list_alerts(user: UserContext = Depends(require_admin)):
    db = get_client()
    res = db.table("ai_alerts").select("*").eq("status", "open").order("created_at", desc=True).limit(200).execute()
    return res.data or []


@router.patch("/alerts/{alert_id}/resolve")
def resolve_alert(alert_id: str, user: UserContext = Depends(require_admin)):
    db = get_client()
    res = db.table("ai_alerts").select("type, entity_id").eq("id", alert_id).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Alert not found")
    row = res.data[0]
    _resolve_alert(db, row["type"], row["entity_id"])
    return {"ok": True}


@router.post("/reports/daily")
def trigger_daily_report(user: UserContext = Depends(require_admin)):
    return generate_daily_report()


@router.post("/reports/monthly")
def trigger_monthly_report(user: UserContext = Depends(require_admin)):
    return generate_monthly_report()


@router.get("/reports")
def list_reports(user: UserContext = Depends(require_admin)):
    db = get_client()
    res = db.table("ai_reports").select("*").order("report_date", desc=True).limit(30).execute()
    return res.data or []


@router.get("/leaderboard")
def agent_leaderboard(user: UserContext = Depends(require_admin)):
    db = get_client()
    now = datetime.now(timezone.utc)
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()

    all_deals = db.table("lead_deals").select("id, lead_id, sales_agent, status, created_at, est_kwh, adder").execute().data or []
    proposals  = db.table("proposals").select("rep_name, status, created_at").execute().data or []

    agents: dict = {}

    def _agent(name: str):
        if name not in agents:
            agents[name] = {
                "name": name,
                "total_deals": 0, "active_deals": 0, "future_deals": 0,
                "deals_this_month": 0,
                "unique_leads": set(), "converted_leads": set(),
                "proposals_sent": 0, "proposals_accepted": 0,
                "est_monthly_commission": 0.0,
            }
        return agents[name]

    for d in all_deals:
        name = d.get("sales_agent") or "Unassigned"
        a = _agent(name)
        a["total_deals"] += 1
        if d.get("lead_id"):
            a["unique_leads"].add(d["lead_id"])
        status = d.get("status", "")
        if status == "Active":
            a["active_deals"] += 1
            if d.get("lead_id"):
                a["converted_leads"].add(d["lead_id"])
            a["est_monthly_commission"] += float(d.get("est_kwh") or 0) * float(d.get("adder") or 0)
        elif status == "Future":
            a["future_deals"] += 1
        if (d.get("created_at") or "") >= month_start:
            a["deals_this_month"] += 1

    for p in proposals:
        name = p.get("rep_name") or "Unassigned"
        a = _agent(name)
        if p.get("status") in ("sent", "viewed", "accepted", "rejected"):
            a["proposals_sent"] += 1
        if p.get("status") == "accepted":
            a["proposals_accepted"] += 1

    results = []
    for name, a in agents.items():
        total_leads   = len(a["unique_leads"])
        converted     = len(a["converted_leads"])
        conv_rate     = round(converted / total_leads * 100, 1) if total_leads > 0 else 0.0
        prop_rate     = round(a["proposals_accepted"] / a["proposals_sent"] * 100, 1) if a["proposals_sent"] > 0 else 0.0
        results.append({
            "name":                   name,
            "deals_this_month":       a["deals_this_month"],
            "active_deals":           a["active_deals"],
            "future_deals":           a["future_deals"],
            "total_deals":            a["total_deals"],
            "unique_leads_touched":   total_leads,
            "conversion_rate":        conv_rate,
            "proposals_sent":         a["proposals_sent"],
            "proposals_accepted":     a["proposals_accepted"],
            "proposal_close_rate":    prop_rate,
            "est_monthly_commission": round(a["est_monthly_commission"], 2),
        })

    results.sort(key=lambda x: (x["deals_this_month"], x["active_deals"]), reverse=True)
    return results


@router.get("/pipeline")
def pipeline_value(user: UserContext = Depends(require_admin)):
    db = get_client()
    now = datetime.now(timezone.utc)

    active = db.table("lead_deals").select("id, est_kwh, adder, end_date, start_date, supplier, sales_agent, status").eq("status", "Active").execute().data or []
    future = db.table("lead_deals").select("id, est_kwh, adder, start_date, supplier, sales_agent").eq("status", "Future").execute().data or []

    def _commission(d):
        return float(d.get("est_kwh") or 0) * float(d.get("adder") or 0)

    monthly_commission = sum(_commission(d) for d in active)
    pipeline_value_amt = sum(_commission(d) for d in future)

    # Revenue at risk — active deals expiring within 90 days
    at_risk = []
    for d in active:
        days = None
        if d.get("end_date"):
            try:
                end = datetime.strptime(d["end_date"][:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
                days = (end - now).days
            except Exception:
                pass
        if days is not None and days <= 90:
            at_risk.append({
                "supplier":              d.get("supplier") or "—",
                "sales_agent":           d.get("sales_agent") or "Unassigned",
                "days_until_expiry":     days,
                "end_date":              d.get("end_date", "")[:10],
                "est_monthly_commission": round(_commission(d), 2),
            })
    at_risk.sort(key=lambda x: x["days_until_expiry"])

    # Expiry schedule — commission expiring by month
    expiry_by_month: dict = defaultdict(float)
    for d in active:
        if d.get("end_date"):
            try:
                month = d["end_date"][:7]
                expiry_by_month[month] += _commission(d)
            except Exception:
                pass

    # Commission added by start month (trend)
    start_by_month: dict = defaultdict(lambda: {"deals": 0, "commission": 0.0})
    for d in active:
        key = (d.get("start_date") or "")[:7] or (d.get("created_at") or "")[:7]
        if key:
            start_by_month[key]["deals"] += 1
            start_by_month[key]["commission"] = round(start_by_month[key]["commission"] + _commission(d), 2)

    # Commission by agent
    by_agent: dict = defaultdict(lambda: {"active": 0, "future": 0, "commission": 0.0})
    for d in active:
        name = d.get("sales_agent") or "Unassigned"
        by_agent[name]["active"] += 1
        by_agent[name]["commission"] = round(by_agent[name]["commission"] + _commission(d), 2)
    for d in future:
        name = d.get("sales_agent") or "Unassigned"
        by_agent[name]["future"] += 1

    return {
        "summary": {
            "active_monthly_commission": round(monthly_commission, 2),
            "pipeline_value":            round(pipeline_value_amt, 2),
            "active_deals":              len(active),
            "future_deals":              len(future),
            "at_risk_count":             len(at_risk),
            "at_risk_commission":        round(sum(x["est_monthly_commission"] for x in at_risk), 2),
        },
        "at_risk":             at_risk[:30],
        "expiry_by_month":     dict(sorted(expiry_by_month.items())),
        "commission_by_start": {k: v for k, v in sorted(start_by_month.items())},
        "by_agent":            {k: v for k, v in sorted(by_agent.items(), key=lambda x: x[1]["commission"], reverse=True)},
    }


@router.get("/deals-by-agent")
def deals_by_agent(
    mode: str = Query("month", regex="^(day|month)$"),
    months_back: int = Query(6, ge=1, le=24),
    user: UserContext = Depends(require_admin),
):
    db = get_client()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=months_back * 30)).isoformat()

    deals = (
        db.table("lead_deals")
        .select("sales_agent, status, created_at")
        .gte("created_at", cutoff)
        .execute()
        .data or []
    )

    # Group by agent → period → count
    # "closed" = Active or Inactive (anything that was signed)
    closed_statuses = {"Active", "Inactive"}
    counts: dict = defaultdict(lambda: defaultdict(int))
    agents: set = set()

    for d in deals:
        if d.get("status") not in closed_statuses:
            continue
        agent = d.get("sales_agent") or "Unassigned"
        agents.add(agent)
        try:
            dt = datetime.fromisoformat(d["created_at"].replace("Z", "+00:00"))
            period = dt.strftime("%Y-%m-%d") if mode == "day" else dt.strftime("%Y-%m")
        except Exception:
            continue
        counts[period][agent] += 1

    # Build sorted period list
    periods = sorted(counts.keys())
    agents_sorted = sorted(agents)

    rows = []
    for period in periods:
        row = {"period": period}
        for agent in agents_sorted:
            row[agent] = counts[period].get(agent, 0)
        row["total"] = sum(counts[period].values())
        rows.append(row)

    # Per-agent totals
    agent_totals = {agent: sum(counts[p].get(agent, 0) for p in periods) for agent in agents_sorted}

    return {
        "mode": mode,
        "periods": periods,
        "agents": agents_sorted,
        "rows": rows,
        "agent_totals": agent_totals,
    }
