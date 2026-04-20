from fastapi import APIRouter, Query, Depends
from typing import Optional
from app.db.client import get_client
from app.services.commission_engine import bulk_generate_expected
from app.auth.deps import require_admin, UserContext

router = APIRouter()

@router.get("/expected")
def list_expected(
    billing_month: Optional[str] = Query(None),
    supplier_id: Optional[str] = Query(None),
    user: UserContext = Depends(require_admin)
):
    db = get_client()
    q = db.table("expected_commissions").select(
        "*, contracts(contract_number), service_points(esiid), suppliers(name, code)"
    ).order("billing_month", desc=True)
    if billing_month:
        q = q.eq("billing_month", billing_month)
    if supplier_id:
        q = q.eq("supplier_id", supplier_id)
    return q.execute().data

@router.post("/expected/generate")
def generate_expected_bulk(billing_month: str, supplier_id: Optional[str] = None, user: UserContext = Depends(require_admin)):
    return bulk_generate_expected(billing_month, supplier_id)

@router.get("/actual")
def list_actual(
    billing_month: Optional[str] = Query(None),
    supplier_id: Optional[str] = Query(None),
    is_matched: Optional[bool] = Query(None),
    user: UserContext = Depends(require_admin)
):
    db = get_client()
    q = db.table("actual_commissions").select(
        "*, suppliers(name, code), service_points(esiid)"
    ).order("billing_month", desc=True)
    if billing_month:
        q = q.eq("billing_month", billing_month)
    if supplier_id:
        q = q.eq("supplier_id", supplier_id)
    if is_matched is not None:
        q = q.eq("is_matched", is_matched)
    return q.execute().data
