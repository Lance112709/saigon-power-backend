from fastapi import APIRouter, HTTPException, Query, Depends
from app.auth.deps import get_current_user, require_admin, UserContext
from pydantic import BaseModel
from typing import Optional
from datetime import date
from app.db.client import get_client
from app.services.commission_engine import generate_expected_for_month

router = APIRouter()

class ContractCreate(BaseModel):
    contract_number: str
    customer_id: str
    supplier_id: str
    service_point_id: str
    commission_model: str  # "per_kwh" or "percentage_of_bill"
    commission_rate: float
    start_date: date
    end_date: Optional[date] = None
    notes: Optional[str] = None

class ContractUpdate(BaseModel):
    commission_rate: Optional[float] = None
    end_date: Optional[date] = None
    status: Optional[str] = None
    notes: Optional[str] = None

@router.get("")
def list_contracts(
    supplier_id: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    customer_id: Optional[str] = Query(None),
    user: UserContext = Depends(get_current_user),
):
    db = get_client()
    q = db.table("contracts").select("*, customers(business_name), suppliers(name, code), service_points(esiid)").order("created_at", desc=True)
    if supplier_id:
        q = q.eq("supplier_id", supplier_id)
    if status:
        q = q.eq("status", status)
    if customer_id:
        q = q.eq("customer_id", customer_id)
    return q.execute().data

@router.post("")
def create_contract(body: ContractCreate, user: UserContext = Depends(require_admin)):
    db = get_client()
    data = body.model_dump()
    data["start_date"] = str(data["start_date"])
    if data["end_date"]:
        data["end_date"] = str(data["end_date"])
    res = db.table("contracts").insert(data).execute()
    return res.data[0]

@router.get("/{id}")
def get_contract(id: str, user: UserContext = Depends(get_current_user)):
    db = get_client()
    res = db.table("contracts").select("*, customers(business_name), suppliers(name, code), service_points(esiid, service_address)").eq("id", id).single().execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Contract not found")
    return res.data

@router.patch("/{id}")
def update_contract(id: str, body: ContractUpdate, user: UserContext = Depends(require_admin)):
    db = get_client()
    updates = {k: v for k, v in body.model_dump().items() if v is not None}
    if "end_date" in updates:
        updates["end_date"] = str(updates["end_date"])
    res = db.table("contracts").update(updates).eq("id", id).execute()
    return res.data[0]

@router.post("/{id}/generate-expected")
def generate_expected(id: str, billing_month: str, user: UserContext = Depends(require_admin)):
    """Generate expected commission for a contract for a given month (YYYY-MM-DD)"""
    db = get_client()
    contract = db.table("contracts").select("*").eq("id", id).single().execute()
    if not contract.data:
        raise HTTPException(status_code=404, detail="Contract not found")
    result = generate_expected_for_month(contract.data, billing_month)
    return result
