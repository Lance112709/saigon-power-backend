from fastapi import APIRouter, HTTPException, Depends, Body
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
