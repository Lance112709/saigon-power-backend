from fastapi import APIRouter, HTTPException, Depends
from app.auth.deps import get_current_user, require_admin, UserContext
from pydantic import BaseModel
from typing import Optional
from app.db.client import get_client

router = APIRouter()

class SupplierCreate(BaseModel):
    name: str
    code: str
    contact_email: Optional[str] = None
    notes: Optional[str] = None

class SupplierUpdate(BaseModel):
    name: Optional[str] = None
    contact_email: Optional[str] = None
    is_active: Optional[bool] = None
    notes: Optional[str] = None
    default_adder: Optional[float] = None

@router.get("")
def list_suppliers(user: UserContext = Depends(get_current_user)):
    db = get_client()
    res = db.table("suppliers").select("*").order("name").execute()
    return res.data

@router.post("")
def create_supplier(body: SupplierCreate, user: UserContext = Depends(require_admin)):
    db = get_client()
    res = db.table("suppliers").insert(body.model_dump()).execute()
    return res.data[0]

@router.get("/{id}")
def get_supplier(id: str, user: UserContext = Depends(get_current_user)):
    db = get_client()
    res = db.table("suppliers").select("*").eq("id", id).single().execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Supplier not found")
    return res.data

@router.patch("/{id}")
def update_supplier(id: str, body: SupplierUpdate, user: UserContext = Depends(require_admin)):
    db = get_client()
    updates = {k: v for k, v in body.model_dump().items() if v is not None or k == "default_adder"}
    res = db.table("suppliers").update(updates).eq("id", id).execute()
    return res.data[0]
