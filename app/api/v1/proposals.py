from fastapi import APIRouter, HTTPException, Query, Body, Depends
from typing import Optional
from datetime import datetime, timezone
from app.db.client import get_client
from app.auth.deps import get_current_user, UserContext

router = APIRouter()

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

# ── Declared BEFORE /{id} routes to avoid path conflicts ──────────────────────

@router.get("/view/{token}")
def view_proposal(token: str):
    db = get_client()
    res = db.table("proposals").select("*").eq("token", token).limit(1).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Proposal not found")
    p = res.data[0]
    if p["status"] == "draft":
        db.table("proposals").update({"status": "sent", "updated_at": _now()}).eq("token", token).execute()
        p["status"] = "sent"
    elif p["status"] == "sent":
        db.table("proposals").update({"status": "viewed", "updated_at": _now()}).eq("token", token).execute()
        p["status"] = "viewed"
    return p

@router.post("/accept/{token}")
def accept_proposal(token: str, data: dict = Body(...)):
    db = get_client()
    res = db.table("proposals").select("*").eq("token", token).limit(1).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Proposal not found")
    p = res.data[0]
    if p["status"] == "accepted":
        raise HTTPException(status_code=400, detail="Proposal already accepted")
    if p["status"] == "rejected":
        raise HTTPException(status_code=400, detail="Proposal has been rejected")

    signature = str(data.get("signature") or "").strip()
    if not signature:
        raise HTTPException(status_code=400, detail="Signature is required")

    deal_id = None
    if p.get("lead_id"):
        try:
            deal_payload = {
                "lead_id":         p["lead_id"],
                "status":          "Future",
                "supplier":        p.get("rep_name"),
                "plan_name":       p.get("plan_name"),
                "rate":            float(p["rate"]) if p.get("rate") else None,
                "contract_term":   f"{p['term_months']} Months" if p.get("term_months") else None,
                "sales_agent":     None,
                "flag_tos":        False,
                "flag_toao":       False,
                "flag_deposit":    False,
                "flag_special_deal": False,
                "flag_promo_10":   False,
                "notes":           f"Created from accepted proposal. Est. monthly bill: ${p.get('est_monthly_bill') or 'N/A'}",
            }
            deal_res = db.table("lead_deals").insert(deal_payload).execute()
            if deal_res.data:
                deal_id = deal_res.data[0]["id"]
        except Exception as e:
            pass

    db.table("proposals").update({
        "status":       "accepted",
        "signature":    signature,
        "accepted_at":  _now(),
        "deal_id":      deal_id,
        "updated_at":   _now(),
    }).eq("token", token).execute()

    return {"ok": True, "deal_id": deal_id}

# ── List + Create ─────────────────────────────────────────────────────────────

@router.get("")
def list_proposals(
    status:   Optional[str] = Query(None),
    lead_id:  Optional[str] = Query(None),
    limit:    int           = Query(50),
    offset:   int           = Query(0),
    user: UserContext = Depends(get_current_user),
):
    db = get_client()
    q = db.table("proposals").select("*")
    if status:
        q = q.eq("status", status)
    if lead_id:
        q = q.eq("lead_id", lead_id)
    res = q.order("created_at", desc=True).range(offset, offset + limit - 1).execute()
    return res.data or []

@router.post("")
def create_proposal(data: dict = Body(...), user: UserContext = Depends(get_current_user)):
    db = get_client()
    name = str(data.get("customer_name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="customer_name is required")

    def _f(key):
        v = data.get(key)
        try:
            return float(v) if v not in (None, "", "null") else None
        except (ValueError, TypeError):
            return None

    def _i(key):
        v = data.get(key)
        try:
            return int(v) if v not in (None, "", "null") else None
        except (ValueError, TypeError):
            return None

    payload = {
        "lead_id":              data.get("lead_id") or None,
        "customer_id":          data.get("customer_id") or None,
        "customer_name":        name,
        "customer_phone":       str(data.get("customer_phone") or "").strip() or None,
        "customer_email":       str(data.get("customer_email") or "").strip() or None,
        "customer_address":     str(data.get("customer_address") or "").strip() or None,
        "rep_name":             str(data.get("rep_name") or "").strip() or None,
        "plan_name":            str(data.get("plan_name") or "").strip() or None,
        "rate":                 _f("rate"),
        "term_months":          _i("term_months"),
        "est_monthly_bill":     _f("est_monthly_bill"),
        "early_termination_fee": _f("early_termination_fee"),
        "notes":                str(data.get("notes") or "").strip() or None,
        "status":               "draft",
    }
    res = db.table("proposals").insert(payload).execute()
    if not res.data:
        raise HTTPException(status_code=500, detail="Failed to create proposal")
    created = res.data[0]

    if created.get("customer_phone"):
        try:
            from app.services.sms import send_automated
            send_automated(
                "proposal_sent",
                created["customer_phone"],
                {
                    "first_name": (created["customer_name"] or "").split()[0],
                    "plan_name":  created.get("plan_name") or "our energy plan",
                    "rep_name":   created.get("rep_name") or "Your Saigon Power rep",
                },
                lead_id=created.get("lead_id"),
                customer_id=created.get("customer_id"),
            )
        except Exception:
            pass

    return created

# ── Single proposal ───────────────────────────────────────────────────────────

@router.get("/{proposal_id}")
def get_proposal(proposal_id: str, user: UserContext = Depends(get_current_user)):
    db = get_client()
    res = db.table("proposals").select("*").eq("id", proposal_id).limit(1).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Proposal not found")
    return res.data[0]

@router.patch("/{proposal_id}")
def update_proposal(proposal_id: str, data: dict = Body(...), user: UserContext = Depends(get_current_user)):
    db = get_client()
    allowed = {"status", "rep_name", "plan_name", "rate", "term_months",
               "est_monthly_bill", "early_termination_fee", "notes"}
    payload = {k: v for k, v in data.items() if k in allowed}
    if not payload:
        raise HTTPException(status_code=400, detail="No valid fields to update")
    payload["updated_at"] = _now()
    res = db.table("proposals").update(payload).eq("id", proposal_id).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Proposal not found")
    return res.data[0]
