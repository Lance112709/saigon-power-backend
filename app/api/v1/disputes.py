import io
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.auth.deps import require_admin, UserContext
from app.db.client import get_client
from app.services.disputes import (
    build_dispute_package, get_attachment, record_outcome, send_dispute,
)

router = APIRouter()


@router.get("")
def list_disputes(status: Optional[str] = Query(None),
                  supplier_id: Optional[str] = Query(None),
                  user: UserContext = Depends(require_admin)):
    db = get_client()
    q = db.table("disputes").select("*, suppliers(name, code)") \
        .order("created_at", desc=True).limit(300)
    if status:
        q = q.eq("status", status)
    if supplier_id:
        q = q.eq("supplier_id", supplier_id)
    return q.execute().data or []


@router.get("/{id}")
def get_dispute(id: str, user: UserContext = Depends(require_admin)):
    db = get_client()
    rows = db.table("disputes").select("*, suppliers(name, code)").eq("id", id) \
        .limit(1).execute().data
    if not rows:
        raise HTTPException(status_code=404, detail="Dispute not found")
    items = db.table("dispute_items").select("*").eq("dispute_id", id) \
        .order("claimed_amount", desc=True).limit(2000).execute().data or []
    return {**rows[0], "items": items}


class CreateDisputeBody(BaseModel):
    supplier_id: str
    case_ids: Optional[List[str]] = None
    finding_id: Optional[str] = None
    title: Optional[str] = None


@router.post("")
def create_dispute(body: CreateDisputeBody, user: UserContext = Depends(require_admin)):
    db = get_client()
    try:
        return build_dispute_package(db, body.supplier_id, user.email or "admin",
                                     case_ids=body.case_ids, finding_id=body.finding_id,
                                     title=body.title)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))


class EditDisputeBody(BaseModel):
    title: Optional[str] = None
    email_to: Optional[str] = None
    email_subject: Optional[str] = None
    email_body: Optional[str] = None
    notes: Optional[str] = None


@router.patch("/{id}")
def edit_dispute(id: str, body: EditDisputeBody, user: UserContext = Depends(require_admin)):
    db = get_client()
    rows = db.table("disputes").select("status").eq("id", id).limit(1).execute().data
    if not rows:
        raise HTTPException(status_code=404, detail="Dispute not found")
    if rows[0]["status"] != "draft":
        raise HTTPException(status_code=422, detail="Only drafts can be edited")
    fields = {k: v for k, v in body.model_dump().items() if v is not None}
    if not fields:
        raise HTTPException(status_code=422, detail="Nothing to update")
    return db.table("disputes").update(fields).eq("id", id).execute().data[0]


@router.post("/{id}/send")
def send(id: str, user: UserContext = Depends(require_admin)):
    db = get_client()
    try:
        return send_dispute(db, id, user.email or "admin")
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))


class OutcomeBody(BaseModel):
    status: str  # provider_responded | recovered | rejected
    recovered_amount: float = 0.0
    notes: str = ""


@router.post("/{id}/outcome")
def outcome(id: str, body: OutcomeBody, user: UserContext = Depends(require_admin)):
    db = get_client()
    try:
        return record_outcome(db, id, body.status, body.recovered_amount,
                              body.notes, user.email or "admin")
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))


@router.get("/{id}/attachment")
def download_attachment(id: str, user: UserContext = Depends(require_admin)):
    db = get_client()
    rows = db.table("disputes").select("*").eq("id", id).limit(1).execute().data
    if not rows:
        raise HTTPException(status_code=404, detail="Dispute not found")
    blob = get_attachment(db, rows[0])
    if not blob:
        raise HTTPException(status_code=404, detail="No attachment stored for this dispute")
    return StreamingResponse(
        io.BytesIO(blob),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="dispute_{id[:8]}.xlsx"'})
