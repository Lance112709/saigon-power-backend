from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from app.db.client import get_client

router = APIRouter()

class ServicePointCreate(BaseModel):
    customer_id: str
    esiid: str
    meter_number: Optional[str] = None
    service_address: str
    city: str
    state: str = "TX"
    zip_code: str

class ServicePointUpdate(BaseModel):
    meter_number: Optional[str] = None
    service_address: Optional[str] = None
    is_active: Optional[bool] = None

@router.post("")
def create_service_point(body: ServicePointCreate):
    db = get_client()
    res = db.table("service_points").insert(body.model_dump()).execute()
    return res.data[0]

@router.get("/lookup/{esiid}")
def lookup_by_esiid(esiid: str):
    db = get_client()
    res = db.table("service_points").select("*, customers(business_name)").eq("esiid", esiid).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="ESIID not found")
    return res.data[0]

@router.get("/{id}")
def get_service_point(id: str):
    db = get_client()
    res = db.table("service_points").select("*, customers(business_name)").eq("id", id).single().execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Service point not found")
    return res.data

@router.patch("/{id}")
def update_service_point(id: str, body: ServicePointUpdate):
    db = get_client()
    updates = {k: v for k, v in body.model_dump().items() if v is not None}
    res = db.table("service_points").update(updates).eq("id", id).execute()
    return res.data[0]
