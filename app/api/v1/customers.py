from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional
from app.db.client import get_client

router = APIRouter()

class CustomerCreate(BaseModel):
    account_number: str
    business_name: str
    contact_name: Optional[str] = None
    contact_email: Optional[str] = None
    contact_phone: Optional[str] = None
    service_address: str
    city: str
    state: str = "TX"
    zip_code: str

class CustomerUpdate(BaseModel):
    business_name: Optional[str] = None
    contact_name: Optional[str] = None
    contact_email: Optional[str] = None
    contact_phone: Optional[str] = None
    service_address: Optional[str] = None
    is_active: Optional[bool] = None

@router.get("")
def list_customers(
    search: Optional[str] = Query(None),
    is_active: Optional[bool] = Query(None)
):
    db = get_client()
    q = db.table("customers").select("*").order("business_name")
    if is_active is not None:
        q = q.eq("is_active", is_active)
    res = q.execute()
    data = res.data
    if search:
        search_lower = search.lower()
        data = [c for c in data if search_lower in c["business_name"].lower()]
    return data

@router.post("")
def create_customer(body: CustomerCreate):
    db = get_client()
    res = db.table("customers").insert(body.model_dump()).execute()
    return res.data[0]

@router.get("/{id}")
def get_customer(id: str):
    db = get_client()
    customer = db.table("customers").select("*").eq("id", id).single().execute()
    if not customer.data:
        raise HTTPException(status_code=404, detail="Customer not found")
    service_points = db.table("service_points").select("*").eq("customer_id", id).execute()
    contracts = db.table("contracts").select("*, suppliers(name, code)").eq("customer_id", id).execute()
    return {
        **customer.data,
        "service_points": service_points.data,
        "contracts": contracts.data
    }

@router.patch("/{id}")
def update_customer(id: str, body: CustomerUpdate):
    db = get_client()
    updates = {k: v for k, v in body.model_dump().items() if v is not None}
    res = db.table("customers").update(updates).eq("id", id).execute()
    return res.data[0]
