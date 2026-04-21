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
