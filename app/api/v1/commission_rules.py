from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.auth.deps import require_admin, UserContext
from app.db.client import get_client
from app.services.commission_rules import (
    RULE_TYPES, create_rule_version, evaluate_rule, rule_history,
)

router = APIRouter()


@router.get("")
def list_current_rules(supplier_id: Optional[str] = Query(None),
                       user: UserContext = Depends(require_admin)):
    """Current (open-ended) rule per supplier, with supplier name."""
    db = get_client()
    q = db.table("commission_rules").select("*, suppliers(name, code)") \
        .is_("effective_to", "null").order("created_at", desc=True)
    if supplier_id:
        q = q.eq("supplier_id", supplier_id)
    return q.limit(200).execute().data or []


@router.get("/history")
def get_history(supplier_id: str = Query(...), user: UserContext = Depends(require_admin)):
    db = get_client()
    return rule_history(db, supplier_id)


class RuleBody(BaseModel):
    supplier_id: str
    name: Optional[str] = None
    rule_type: str
    config: dict = {}
    effective_from: str  # YYYY-MM-DD
    notes: Optional[str] = None


@router.post("")
def create_rule(body: RuleBody, user: UserContext = Depends(require_admin)):
    if body.rule_type not in RULE_TYPES:
        raise HTTPException(status_code=422, detail=f"rule_type must be one of {RULE_TYPES}")
    db = get_client()
    return create_rule_version(db, body.supplier_id, body.model_dump(),
                               actor=user.email or "admin")


class PreviewSample(BaseModel):
    kwh: Optional[float] = None
    adder: Optional[float] = None


class PreviewBody(BaseModel):
    rule_type: str
    config: dict = {}
    samples: List[PreviewSample] = []


@router.post("/preview")
def preview_rule(body: PreviewBody, user: UserContext = Depends(require_admin)):
    """Evaluate a draft rule against sample accounts (admin-page calculator)."""
    rule = {"rule_type": body.rule_type, "config": body.config}
    out = []
    for s in body.samples:
        ev = evaluate_rule(rule, s.kwh, s.adder)
        out.append({"kwh": s.kwh, "adder": s.adder,
                    "expected_amount": ev[0] if ev else None,
                    "expected_rate": ev[1] if ev else None,
                    "computable": ev is not None})
    return {"results": out}
