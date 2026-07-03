from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional, List
from pydantic import BaseModel
from app.db.client import get_client
from app.services.reconciliation_v2 import (
    rows_from_db, replace_prior_runs, run_reconciliation_v2, load_deals,
)
from app.services.file_parser.provider_parsers import PROVIDER_SUPPLIERS
from app.auth.deps import require_admin, UserContext

router = APIRouter()

@router.post("/run")
def trigger_reconciliation(billing_month: str, supplier_id: Optional[str] = None, user: UserContext = Depends(require_admin)):
    """Re-reconcile one month against imported statement rows, per provider.
    Replaces each provider's existing run for the month (resolutions carry over)."""
    db = get_client()
    label = billing_month[:7]
    results = []
    for group, sdef in PROVIDER_SUPPLIERS.items():
        sup = db.table("suppliers").select("id").eq("code", sdef["code"]).limit(1).execute().data
        if not sup:
            continue
        sup_id = sup[0]["id"]
        if supplier_id and sup_id != supplier_id:
            continue
        rows = rows_from_db(db, sup_id, label)
        if not rows:
            continue
        carry = replace_prior_runs(db, sup_id, label)
        deals = load_deals(db, group)
        results.append(run_reconciliation_v2(
            db, sup_id, group, label, rows,
            deals=deals, actor=user.email or "admin", carry_resolved=carry))
    if not results:
        raise HTTPException(status_code=404,
                            detail=f"No imported statement rows for {label}. Upload the statements first.")
    return {"billing_month": label, "runs": results}

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

class BulkResolveBody(BaseModel):
    item_ids: List[str]
    resolution_notes: str = ""

@router.post("/items/bulk-resolve")
def bulk_resolve_items(body: BulkResolveBody, user: UserContext = Depends(require_admin)):
    db = get_client()
    from datetime import datetime
    now = datetime.utcnow().isoformat()
    updated = 0
    for item_id in body.item_ids:
        db.table("reconciliation_items").update({
            "is_resolved": True,
            "resolution_notes": body.resolution_notes,
            "resolved_at": now,
        }).eq("id", item_id).execute()
        updated += 1
    return {"resolved": updated}

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
