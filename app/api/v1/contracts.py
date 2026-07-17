from fastapi import APIRouter, HTTPException, Query, Depends, UploadFile, File, Request
from app.auth.deps import get_current_user, require_admin, UserContext
from app.core.security import rate_limit
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

class ContractTemplateUpdate(BaseModel):
    html_content: str

# ── Proposal contract template endpoints (defined before /{id} to avoid conflicts) ──

@router.get("/template")
def get_contract_template():
    db = get_client()
    res = db.table("contract_templates").select("*").order("id", desc=True).limit(1).execute()
    if not res.data:
        return {"id": None, "html_content": ""}
    return res.data[0]

@router.patch("/template")
def update_contract_template(body: ContractTemplateUpdate, user: UserContext = Depends(get_current_user)):
    if user.role not in ("admin", "manager"):
        raise HTTPException(status_code=403, detail="Admin/Manager only")
    db = get_client()
    existing = db.table("contract_templates").select("id").order("id", desc=True).limit(1).execute()
    if existing.data:
        res = db.table("contract_templates").update({"html_content": body.html_content}).eq("id", existing.data[0]["id"]).execute()
    else:
        res = db.table("contract_templates").insert({"html_content": body.html_content}).execute()
    return res.data[0]

MAX_CONTRACT_PDF_BYTES = 10 * 1024 * 1024  # 10 MB

@router.post("/store/{token}")
async def store_signed_contract(token: str, file: UploadFile = File(...), request: Request = None):
    # Public by design: the signer's browser posts the executed PDF using the
    # unguessable proposal token as its capability. Throttle + validate so the
    # endpoint can't be abused to dump arbitrary/oversized blobs into storage.
    if request is not None:
        rate_limit(request, "store_contract", limit=20, window_seconds=600)
    db = get_client()
    proposal = db.table("proposals").select("id, status").eq("token", token).limit(1).execute()
    if not proposal.data:
        raise HTTPException(status_code=404, detail="Proposal not found")

    pdf_bytes = await file.read()
    if not pdf_bytes:
        raise HTTPException(status_code=400, detail="Empty file.")
    if len(pdf_bytes) > MAX_CONTRACT_PDF_BYTES:
        raise HTTPException(status_code=400, detail="File too large (max 10 MB).")
    if not pdf_bytes.startswith(b"%PDF-"):
        raise HTTPException(status_code=400, detail="Only PDF files are accepted.")
    path = f"{token}.pdf"

    try:
        db.storage.from_("contracts").upload(path, pdf_bytes, {"content-type": "application/pdf", "upsert": "true"})
    except Exception:
        try:
            db.storage.from_("contracts").remove([path])
            db.storage.from_("contracts").upload(path, pdf_bytes, {"content-type": "application/pdf"})
        except Exception as e2:
            raise HTTPException(status_code=500, detail=f"Upload failed: {str(e2)}")

    db.table("proposals").update({"signed_contract_url": path}).eq("token", token).execute()

    # Also file the signed contract under the customer's Attachments so it shows
    # on their account (best-effort — never blocks the signing itself).
    try:
        pf = db.table("proposals").select("lead_id").eq("token", token).limit(1).execute().data
        lead_id = pf[0].get("lead_id") if pf else None
        if lead_id:
            att_path = f"leads/{lead_id}/signed_contract_{token}.pdf"
            db.storage.from_("crm-attachments").upload(
                att_path, pdf_bytes, {"content-type": "application/pdf", "upsert": "true"})
            exists = db.table("lead_attachments").select("id").eq("lead_id", lead_id) \
                .eq("storage_path", att_path).limit(1).execute().data
            if not exists:
                db.table("lead_attachments").insert({
                    "lead_id": lead_id,
                    "file_name": "Signed Contract.pdf",
                    "storage_path": att_path,
                    "file_type": "application/pdf",
                    "file_size": len(pdf_bytes),
                    "uploaded_by": "E-Signature",
                }).execute()
    except Exception:
        pass

    return {"path": path}

@router.get("/signed-url/{token}")
def get_signed_url(token: str, user: UserContext = Depends(get_current_user)):
    db = get_client()
    proposal = db.table("proposals").select("signed_contract_url").eq("token", token).limit(1).execute()
    if not proposal.data or not proposal.data[0].get("signed_contract_url"):
        raise HTTPException(status_code=404, detail="No signed contract found")

    path = proposal.data[0]["signed_contract_url"]
    try:
        result = db.storage.from_("contracts").create_signed_url(path, 3600)
        # Handle different supabase-py response shapes
        if hasattr(result, "data"):
            url = (result.data or {}).get("signedUrl") or (result.data or {}).get("signedURL")
        else:
            url = result.get("signedUrl") or result.get("signedURL")
        if not url:
            raise ValueError("Empty signed URL")
        return {"url": url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate signed URL: {str(e)}")

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
