from fastapi import APIRouter, HTTPException, Query, Body, Depends
from typing import Optional
from datetime import datetime, timezone
from app.db.client import get_client
from app.auth.deps import get_current_user, require_admin, UserContext
from app.services.sms import send_sms, render_template

router = APIRouter()

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Send SMS (manual) ─────────────────────────────────────────────────────────

@router.post("/send")
def send_manual_sms(data: dict = Body(...), user: UserContext = Depends(get_current_user)):
    to = str(data.get("to") or "").strip()
    body = str(data.get("body") or "").strip()
    if not to or not body:
        raise HTTPException(status_code=400, detail="'to' and 'body' are required")

    result = send_sms(
        to=to,
        body=body,
        user_id=user.id,
        lead_id=data.get("lead_id") or None,
        customer_id=data.get("customer_id") or None,
        deal_id=data.get("deal_id") or None,
    )
    if not result["ok"] and result.get("error") == "opted_out":
        raise HTTPException(status_code=422, detail="This contact has opted out of SMS")
    return result


# ── SMS Logs ──────────────────────────────────────────────────────────────────

@router.get("/logs")
def get_sms_logs(
    lead_id:     Optional[str] = Query(None),
    customer_id: Optional[str] = Query(None),
    deal_id:     Optional[str] = Query(None),
    limit:       int           = Query(50),
    offset:      int           = Query(0),
    user: UserContext = Depends(get_current_user),
):
    db = get_client()
    q = db.table("sms_messages").select("*")
    if lead_id:
        q = q.eq("lead_id", lead_id)
    if customer_id:
        q = q.eq("customer_id", customer_id)
    if deal_id:
        q = q.eq("deal_id", deal_id)
    res = q.order("created_at", desc=True).range(offset, offset + limit - 1).execute()
    return res.data or []


# ── Templates ─────────────────────────────────────────────────────────────────

@router.get("/templates")
def list_templates(user: UserContext = Depends(get_current_user)):
    db = get_client()
    res = db.table("sms_templates").select("*").order("trigger_type").execute()
    return res.data or []


@router.post("/templates")
def create_template(data: dict = Body(...), user: UserContext = Depends(require_admin)):
    db = get_client()
    trigger = str(data.get("trigger_type") or "").strip()
    body = str(data.get("message_body") or "").strip()
    if not trigger or not body:
        raise HTTPException(status_code=400, detail="'trigger_type' and 'message_body' are required")

    existing = db.table("sms_templates").select("id").eq("trigger_type", trigger).limit(1).execute()
    if existing.data:
        raise HTTPException(status_code=409, detail=f"Template for '{trigger}' already exists")

    res = db.table("sms_templates").insert({
        "trigger_type":  trigger,
        "message_body":  body,
        "description":   str(data.get("description") or "").strip() or None,
        "is_active":     bool(data.get("is_active", True)),
    }).execute()
    if not res.data:
        raise HTTPException(status_code=500, detail="Failed to create template")
    return res.data[0]


@router.patch("/templates/{template_id}")
def update_template(
    template_id: str,
    data: dict = Body(...),
    user: UserContext = Depends(require_admin),
):
    db = get_client()
    allowed = {"message_body", "description", "is_active"}
    payload = {k: v for k, v in data.items() if k in allowed}
    if not payload:
        raise HTTPException(status_code=400, detail="No valid fields to update")
    payload["updated_at"] = _now()
    res = db.table("sms_templates").update(payload).eq("id", template_id).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Template not found")
    return res.data[0]


@router.delete("/templates/{template_id}")
def delete_template(template_id: str, user: UserContext = Depends(require_admin)):
    db = get_client()
    db.table("sms_templates").delete().eq("id", template_id).execute()
    return {"ok": True}


# ── Opt-out toggle ────────────────────────────────────────────────────────────

@router.post("/opt-out/lead/{lead_id}")
def toggle_lead_opt_out(lead_id: str, data: dict = Body(default={}), user: UserContext = Depends(get_current_user)):
    db = get_client()
    res = db.table("leads").select("sms_opt_out").eq("id", lead_id).limit(1).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Lead not found")
    current = bool(res.data[0].get("sms_opt_out"))
    new_val = data.get("opt_out") if "opt_out" in data else (not current)
    db.table("leads").update({"sms_opt_out": new_val}).eq("id", lead_id).execute()
    return {"sms_opt_out": new_val}


@router.post("/opt-out/customer/{customer_id}")
def toggle_customer_opt_out(customer_id: str, data: dict = Body(default={}), user: UserContext = Depends(get_current_user)):
    db = get_client()
    res = db.table("crm_customers").select("sms_opt_out").eq("id", customer_id).limit(1).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Customer not found")
    current = bool(res.data[0].get("sms_opt_out"))
    new_val = data.get("opt_out") if "opt_out" in data else (not current)
    db.table("crm_customers").update({"sms_opt_out": new_val}).eq("id", customer_id).execute()
    return {"sms_opt_out": new_val}


# ── Preview template ──────────────────────────────────────────────────────────

@router.post("/preview")
def preview_template(data: dict = Body(...), user: UserContext = Depends(get_current_user)):
    body = str(data.get("message_body") or "").strip()
    variables = data.get("variables") or {}
    return {"preview": render_template(body, variables)}
