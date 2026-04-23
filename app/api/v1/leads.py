from fastapi import APIRouter, HTTPException, Query, Body, Depends
from typing import Optional
from datetime import datetime, timezone
import re
from app.db.client import get_client
from app.auth.deps import get_current_user, require_admin, require_manager, UserContext
from app.api.v1.tasks import create_lead_tasks

router = APIRouter()

# ── Helpers ───────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

def _validate_phone(phone: str) -> bool:
    cleaned = re.sub(r"[\s\-\(\)\+\.]", "", phone)
    return len(cleaned) in (10, 11) and cleaned.isdigit()

def _full_name(lead: dict) -> str:
    return f"{lead.get('first_name', '')} {lead.get('last_name', '')}".strip()

def _next_sgp_id(db) -> str:
    res = db.table("leads").select("sgp_customer_id").not_.is_("sgp_customer_id", "null").order("sgp_customer_id", desc=True).limit(1).execute()
    num = 1
    if res.data:
        try:
            num = int(res.data[0]["sgp_customer_id"].split("-")[1][4:]) + 1
        except Exception:
            pass
    return f"SGP-2026{num:06d}"

def _try_convert(db, lead_id: str) -> None:
    active = db.table("lead_deals").select("id").eq("lead_id", lead_id).eq("status", "Active").execute()
    if not active.data:
        return
    existing = db.table("lead_customers").select("id").eq("lead_id", lead_id).execute()
    if not existing.data:
        db.table("lead_customers").insert({"lead_id": lead_id}).execute()
    # Assign SGP customer ID if not already set
    lead_row = db.table("leads").select("sgp_customer_id").eq("id", lead_id).execute()
    if lead_row.data and not lead_row.data[0].get("sgp_customer_id"):
        db.table("leads").update({"sgp_customer_id": _next_sgp_id(db), "status": "converted", "updated_at": _now()}).eq("id", lead_id).execute()
    else:
        db.table("leads").update({"status": "converted", "updated_at": _now()}).eq("id", lead_id).execute()

def _shape_lead(lead: dict) -> dict:
    deals = lead.pop("lead_deals", []) or []
    return {
        **lead,
        "full_name": _full_name(lead),
        "deal_count": len(deals),
        "active_deal_count": sum(1 for d in deals if d.get("status") == "Active"),
    }

def _deal_payload(data: dict) -> dict:
    def _f(key):
        v = data.get(key)
        try: return float(v) if v not in (None, "", "null") else None
        except (ValueError, TypeError): return None

    return {
        # Flags
        "flag_tos":          bool(data.get("flag_tos")),
        "flag_toao":         bool(data.get("flag_toao")),
        "flag_deposit":      bool(data.get("flag_deposit")),
        "flag_special_deal": bool(data.get("flag_special_deal")),
        "flag_promo_10":     bool(data.get("flag_promo_10")),
        # Contract
        "status":              data.get("status", "Future"),
        "supplier":            str(data.get("supplier") or "").strip() or None,
        "plan_name":           str(data.get("plan_name") or "").strip() or None,
        "product_type":        str(data.get("product_type") or "").strip() or None,
        "rate_type":           str(data.get("rate_type") or "").strip() or None,
        "contract_term":       str(data.get("contract_term") or "").strip() or None,
        "rate":                _f("rate"),
        "adder":               _f("adder"),
        "est_kwh":             _f("est_kwh"),
        "expected_close_date": data.get("expected_close_date") or None,
        "start_date":          data.get("start_date") or None,
        "end_date":            data.get("end_date") or None,
        # Property
        "service_address": str(data.get("service_address") or "").strip() or None,
        "service_city":    str(data.get("service_city") or "").strip() or None,
        "service_state":   str(data.get("service_state") or "TX").strip() or "TX",
        "service_zip":     str(data.get("service_zip") or "").strip() or None,
        "esiid":           str(data.get("esiid") or "").strip() or None,
        # Assignment
        "sales_agent": str(data.get("sales_agent") or "").strip() or None,
        # Deal meta
        "deal_type":          str(data.get("deal_type") or "").strip() or None,
        "service_order_type": str(data.get("service_order_type") or "").strip() or None,
        # Notes
        "notes": str(data.get("notes") or "").strip() or None,
    }

# ── Sales Agents (declared BEFORE /{id}) ──────────────────────────────────────

@router.get("/agents")
def list_agents(user: UserContext = Depends(get_current_user)):
    db = get_client()
    res = db.table("sales_agents").select("*").order("name").execute()
    return res.data

@router.post("/agents")
def create_agent(data: dict = Body(...), user: UserContext = Depends(require_manager)):
    db = get_client()
    name = str(data.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="'name' is required")
    agent_type = str(data.get("agent_type") or "").strip() or None
    phone = str(data.get("phone") or "").strip() or None

    # Auto-generate agent code: FirstName + last 4 digits of phone
    first_name = name.split()[0].upper()
    digits = "".join(c for c in (phone or "") if c.isdigit())
    last4 = digits[-4:] if len(digits) >= 4 else digits.zfill(4)
    agent_code = f"{first_name}{last4}" if last4 else first_name

    payload = {
        "name":       name,
        "agent_type": agent_type,
        "email":      str(data.get("email") or "").strip() or None,
        "phone":      phone,
        "agent_code": agent_code,
    }
    res = db.table("sales_agents").insert(payload).execute()
    return res.data[0]

@router.patch("/agents/{agent_id}")
def update_agent(agent_id: str, data: dict = Body(...), user: UserContext = Depends(get_current_user)):
    if user.role not in ("admin", "manager"):
        raise HTTPException(status_code=403, detail="Admin or Manager only")
    db = get_client()
    allowed = {"name", "agent_type", "email", "phone"}
    payload = {k: v for k, v in data.items() if k in allowed}
    if not payload:
        raise HTTPException(status_code=400, detail="No valid fields")
    # If phone changed, regenerate agent_code
    if "phone" in payload or "name" in payload:
        existing = db.table("sales_agents").select("name, phone").eq("id", agent_id).limit(1).execute().data
        current = existing[0] if existing else {}
        name  = payload.get("name",  current.get("name",  ""))
        phone = payload.get("phone", current.get("phone", ""))
        first_name = (name or "").split()[0].upper() if name else ""
        digits = "".join(c for c in (phone or "") if c.isdigit())
        last4  = digits[-4:] if len(digits) >= 4 else digits.zfill(4)
        payload["agent_code"] = f"{first_name}{last4}" if last4 else first_name
    res = db.table("sales_agents").update(payload).eq("id", agent_id).execute()
    return res.data[0] if res.data else {}

@router.delete("/agents/{agent_id}")
def delete_agent(agent_id: str, user: UserContext = Depends(require_manager)):
    db = get_client()
    db.table("sales_agents").delete().eq("id", agent_id).execute()
    return {"ok": True}

# ── Converted Customers (declared BEFORE /{id} to avoid route conflict) ────────

@router.get("/customers")
def list_lead_customers(
    search: Optional[str] = Query(None),
    limit: int = Query(50),
    offset: int = Query(0),
    user: UserContext = Depends(get_current_user),
):
    db = get_client()
    res = (
        db.table("lead_customers")
        .select("id, lead_id, created_at, leads(*, lead_deals(*))")
        .order("created_at", desc=True)
        .range(offset, offset + limit - 1)
        .execute()
    )
    results = []
    for c in res.data:
        lead = c.get("leads") or {}
        deals = lead.pop("lead_deals", []) or []
        name = _full_name(lead)
        if search and search.lower() not in name.lower() and search.lower() not in (lead.get("phone") or "").lower():
            continue
        start_dates = [d.get("start_date") for d in deals if d.get("start_date")]
        contract_start = min(start_dates) if start_dates else None
        results.append({
            "id": c["id"],
            "lead_id": c["lead_id"],
            "customer_since": contract_start or c["created_at"],
            "full_name": name,
            "sgp_customer_id": lead.get("sgp_customer_id"),
            "business_name": lead.get("business_name"),
            "phone": lead.get("phone"),
            "email": lead.get("email"),
            "address": lead.get("address"),
            "city": lead.get("city"),
            "state": lead.get("state"),
            "zip": lead.get("zip"),
            "deals": deals,
            "active_deal_count": sum(1 for d in deals if d.get("status") == "Active"),
        })
    return results

# ── Backfill SGP IDs ──────────────────────────────────────────────────────────

@router.post("/backfill-sgp-ids")
def backfill_sgp_ids(user: UserContext = Depends(get_current_user)):
    db = get_client()
    converted = db.table("leads").select("id, sgp_customer_id").eq("status", "converted").order("created_at").execute().data or []
    assigned = 0
    for lead in converted:
        if lead.get("sgp_customer_id"):
            continue
        new_id = _next_sgp_id(db)
        db.table("leads").update({"sgp_customer_id": new_id}).eq("id", lead["id"]).execute()
        assigned += 1
    return {"assigned": assigned}

# ── Dropped Deals ─────────────────────────────────────────────────────────────

@router.get("/dropped-deals")
def list_dropped_deals(
    search: Optional[str] = Query(None),
    supplier: Optional[str] = Query(None),
    sales_agent: Optional[str] = Query(None),
    limit: int = Query(100),
    offset: int = Query(0),
    user: UserContext = Depends(get_current_user),
):
    db = get_client()
    q = db.table("lead_deals").select("*, leads(first_name, last_name, phone, address, city, state)").eq("status", "Inactive")
    if supplier:
        q = q.eq("supplier", supplier)
    if sales_agent:
        q = q.eq("sales_agent", sales_agent)
    res = q.order("updated_at", desc=True).range(offset, offset + limit - 1).execute()
    results = []
    for d in res.data:
        lead = d.pop("leads", None) or {}
        name = f"{lead.get('first_name','')} {lead.get('last_name','')}".strip()
        if search:
            s = search.lower()
            if not (s in name.lower() or s in (d.get("supplier") or "").lower()
                    or s in (d.get("sales_agent") or "").lower()
                    or s in (d.get("esiid") or "").lower()):
                continue
        results.append({**d, "lead_name": name, "lead_phone": lead.get("phone"), "lead_address": f"{lead.get('address','')} {lead.get('city','')} {lead.get('state','')}".strip()})
    return results

# ── Leads List + Create ────────────────────────────────────────────────────────

@router.get("")
def list_leads(
    search: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    limit: int = Query(50),
    offset: int = Query(0),
    user: UserContext = Depends(get_current_user),
):
    db = get_client()
    q = db.table("leads").select("*, lead_deals(id, status)")
    if search:
        q = q.or_(
            f"first_name.ilike.%{search}%,"
            f"last_name.ilike.%{search}%,"
            f"phone.ilike.%{search}%,"
            f"address.ilike.%{search}%"
        )
    if status:
        q = q.eq("status", status)
    res = q.order("created_at", desc=True).range(offset, offset + limit - 1).execute()
    return [_shape_lead(lead) for lead in res.data]

@router.post("")
def create_lead(data: dict = Body(...)):
    db = get_client()
    required = ["first_name", "last_name", "address", "city", "state", "zip", "phone"]
    for field in required:
        if not str(data.get(field) or "").strip():
            raise HTTPException(status_code=400, detail=f"'{field}' is required")

    phone = str(data["phone"]).strip()
    if not _validate_phone(phone):
        raise HTTPException(status_code=400, detail="Invalid phone number. Use format: (555) 555-5555")

    fn = data["first_name"].strip().lower()
    ln = data["last_name"].strip().lower()
    addr = data["address"].strip().lower()
    dups = db.table("leads").select("id, first_name, last_name").ilike("address", f"%{addr[:20]}%").execute()
    for d in dups.data:
        if d["first_name"].lower() == fn and d["last_name"].lower() == ln:
            raise HTTPException(status_code=409, detail="A lead with this name and address already exists")

    payload = {
        "first_name":     data["first_name"].strip(),
        "last_name":      data["last_name"].strip(),
        "address":        data["address"].strip(),
        "city":           data["city"].strip(),
        "state":          data["state"].strip().upper(),
        "zip":            data["zip"].strip(),
        "phone":          phone,
        "email":          str(data.get("email") or "").strip().lower() or None,
        "business_name":  str(data.get("business_name") or "").strip() or None,
        "phone2":         str(data.get("phone2") or "").strip() or None,
        "email2":         str(data.get("email2") or "").strip().lower() or None,
        "referral_by":    str(data.get("referral_by") or "").strip() or None,
        "sales_agent":    str(data.get("sales_agent") or "").strip() or None,
        "status":         "lead",
        "source":         str(data.get("source") or "manual").strip(),
    }
    res = db.table("leads").insert(payload).execute()
    new_lead = res.data[0]
    lead_name = f"{payload['first_name']} {payload['last_name']}"
    try:
        from app.services.sms import send_automated
        send_automated(
            "new_lead",
            payload["phone"],
            {"first_name": payload["first_name"]},
            lead_id=new_lead["id"],
        )
    except Exception:
        pass
    return new_lead

# ── Lead Detail + Update ───────────────────────────────────────────────────────

@router.get("/{id}")
def get_lead(id: str, user: UserContext = Depends(get_current_user)):
    db = get_client()
    lead = db.table("leads").select("*").eq("id", id).limit(1).execute()
    if not lead.data:
        raise HTTPException(status_code=404, detail="Lead not found")
    deals = db.table("lead_deals").select("*").eq("lead_id", id).order("created_at", desc=True).execute()
    return {**lead.data[0], "full_name": _full_name(lead.data[0]), "deals": deals.data}

@router.patch("/{id}")
def update_lead(id: str, data: dict = Body(...), user: UserContext = Depends(get_current_user)):
    db = get_client()
    allowed = {"first_name", "last_name", "address", "city", "state", "zip", "phone", "email", "business_name", "phone2", "email2", "referral_by", "sales_agent", "anxh", "dob", "dl_number", "account_flag"}
    payload = {k: str(v).strip() for k, v in data.items() if k in allowed and v is not None}
    if "phone" in payload and not _validate_phone(payload["phone"]):
        raise HTTPException(status_code=400, detail="Invalid phone number format")
    if not payload:
        raise HTTPException(status_code=400, detail="No valid fields to update")
    payload["updated_at"] = _now()
    res = db.table("leads").update(payload).eq("id", id).execute()
    return res.data[0] if res.data else {}

# ── Delete Lead ───────────────────────────────────────────────────────────────

@router.delete("/{id}")
def delete_lead(id: str, user: UserContext = Depends(require_manager)):
    db = get_client()
    db.table("lead_notes").delete().eq("lead_id", id).execute()
    db.table("lead_deals").delete().eq("lead_id", id).execute()
    db.table("lead_customers").delete().eq("lead_id", id).execute()
    db.table("leads").delete().eq("id", id).execute()
    return {"ok": True}

# ── Deals for a Lead ──────────────────────────────────────────────────────────

@router.post("/{id}/deals")
def create_lead_deal(id: str, data: dict = Body(...), user: UserContext = Depends(get_current_user)):
    db = get_client()
    lead = db.table("leads").select("id").eq("id", id).limit(1).execute()
    if not lead.data:
        raise HTTPException(status_code=404, detail="Lead not found")

    if not str(data.get("status") or "").strip():
        raise HTTPException(status_code=400, detail="'status' is required")

    payload = {"lead_id": id, **_deal_payload(data)}
    try:
        res = db.table("lead_deals").insert(payload).execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    if not res.data:
        raise HTTPException(status_code=500, detail="Insert returned no data — check DB columns exist")
    deal = res.data[0]

    if deal["status"] == "Active":
        _try_convert(db, id)

    return deal

@router.delete("/{id}/deals/{deal_id}")
def delete_lead_deal(id: str, deal_id: str, user: UserContext = Depends(get_current_user)):
    db = get_client()
    db.table("lead_deals").delete().eq("id", deal_id).eq("lead_id", id).execute()
    return {"ok": True}

@router.get("/{id}/notes")
def get_lead_notes(id: str, user: UserContext = Depends(get_current_user)):
    db = get_client()
    res = db.table("lead_notes").select("*").eq("lead_id", id).order("created_at", desc=True).execute()
    return res.data

@router.post("/{id}/notes")
def create_lead_note(id: str, data: dict = Body(...), user: UserContext = Depends(get_current_user)):
    db = get_client()
    content = str(data.get("content") or "").strip()
    author  = str(data.get("author_name") or "").strip()
    if not content:
        raise HTTPException(status_code=400, detail="Content is required")
    if not author:
        raise HTTPException(status_code=400, detail="Author name is required")
    res = db.table("lead_notes").insert({"lead_id": id, "content": content, "author_name": author}).execute()
    return res.data[0]

@router.delete("/{id}/notes/{note_id}")
def delete_lead_note(id: str, note_id: str, user: UserContext = Depends(require_admin)):
    db = get_client()
    db.table("lead_notes").delete().eq("id", note_id).eq("lead_id", id).execute()
    return {"ok": True}

@router.patch("/{id}/deals/{deal_id}")
def update_lead_deal(id: str, deal_id: str, data: dict = Body(...), user: UserContext = Depends(get_current_user)):
    db = get_client()
    allowed = {
        "flag_tos", "flag_toao", "flag_deposit", "flag_special_deal", "flag_promo_10",
        "status", "supplier", "plan_name", "product_type", "rate_type", "contract_term",
        "rate", "adder", "est_kwh", "expected_close_date", "start_date", "end_date",
        "service_address", "service_city", "service_state", "service_zip", "esiid",
        "sales_agent", "deal_type", "service_order_type", "notes",
    }
    payload = {k: v for k, v in data.items() if k in allowed}
    if not payload:
        raise HTTPException(status_code=400, detail="No valid fields to update")
    payload["updated_at"] = _now()
    res = db.table("lead_deals").update(payload).eq("id", deal_id).eq("lead_id", id).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Deal not found")

    if payload.get("status") == "Active":
        _try_convert(db, id)

    return res.data[0]
