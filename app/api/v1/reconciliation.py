from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional
from app.db.client import get_client
from app.services.reconciliation_engine import run_reconciliation
from app.auth.deps import require_admin, UserContext

router = APIRouter()

@router.post("/run")
def trigger_reconciliation(billing_month: str, supplier_id: Optional[str] = None, user: UserContext = Depends(require_admin)):
    return run_reconciliation(billing_month, supplier_id)

@router.get("/runs")
def list_runs(billing_month: Optional[str] = Query(None), user: UserContext = Depends(require_admin)):
    db = get_client()
    q = db.table("reconciliation_runs").select("*, suppliers(name, code)").order("run_at", desc=True)
    if billing_month:
        q = q.eq("billing_month", billing_month)
    return q.execute().data

@router.get("/runs/{id}")
def get_run(id: str, user: UserContext = Depends(require_admin)):
    db = get_client()
    run = db.table("reconciliation_runs").select("*, suppliers(name, code)").eq("id", id).single().execute()
    if not run.data:
        raise HTTPException(status_code=404, detail="Run not found")
    return run.data

@router.get("/runs/{id}/items")
def get_run_items(
    id: str,
    status: Optional[str] = Query(None),
    severity: Optional[str] = Query(None),
    is_resolved: Optional[bool] = Query(None),
    user: UserContext = Depends(require_admin)
):
    db = get_client()
    q = db.table("reconciliation_items").select(
        "*, suppliers(name, code), service_points(esiid, customers(business_name))"
    ).eq("reconciliation_run_id", id).order("severity")

    if status:
        q = q.eq("status", status)
    if severity:
        q = q.eq("severity", severity)
    if is_resolved is not None:
        q = q.eq("is_resolved", is_resolved)

    return q.execute().data

@router.patch("/items/{id}")
def resolve_item(id: str, resolution_notes: str = "", is_resolved: bool = True, user: UserContext = Depends(require_admin)):
    db = get_client()
    from datetime import datetime
    res = db.table("reconciliation_items").update({
        "is_resolved": is_resolved,
        "resolution_notes": resolution_notes,
        "resolved_at": datetime.utcnow().isoformat() if is_resolved else None
    }).eq("id", id).execute()
    return res.data[0]
