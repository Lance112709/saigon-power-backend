from fastapi import APIRouter, HTTPException, Query, Body, Depends
from typing import Optional
from datetime import datetime, timezone
import re
from app.db.client import get_client
from app.auth.deps import get_current_user, require_admin, require_manager, UserContext
from app.api.v1.tasks import create_lead_tasks, create_deal_renewal_tasks

router = APIRouter()

# ── Helpers ───────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

def _validate_phone(phone: str) -> bool:
    cleaned = re.sub(r"[\s\-\(\)\+\.]", "", phone)
    return len(cleaned) in (10, 11) and cleaned.isdigit()

def _full_name(lead: dict) -> str:
    return f"{lead.get('first_name', '')} {lead.get('last_name', '')}".strip()

def _try_convert(db, lead_id: str) -> None:
    active = db.table("lead_deals").select("id").eq("lead_id", lead_id).eq("status", "Active").execute()
    if not active.data:
        return
    existing = db.table("lead_customers").select("id").eq("lead_id", lead_id).execute()
    if not existing.data:
        db.table("lead_customers").insert({"lead_id": lead_id}).execute()
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
    payload = {
        "name":       name,
        "agent_type": agent_type,
        "email":      str(data.get("email") or "").strip() or None,
        "phone":      str(data.get("phone") or "").strip() or None,
    }
    res = db.table("sales_agents").insert(payload).execute()
    return res.data[0]

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
        results.append({
            "id": c["id"],
            "lead_id": c["lead_id"],
            "customer_since": c["created_at"],
            "full_name": name,
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
        "status":         "lead",
        "source":         str(data.get("source") or "manual").strip(),
    }
    res = db.table("leads").insert(payload).execute()
    new_lead = res.data[0]
    lead_name = f"{payload['first_name']} {payload['last_name']}"
    try:
        create_lead_tasks(db, new_lead["id"], lead_name)
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
    allowed = {"first_name", "last_name", "address", "city", "state", "zip", "phone", "email", "business_name", "phone2", "email2"}
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

    if deal.get("end_date"):
        lead_row = db.table("leads").select("first_name, last_name").eq("id", id).limit(1).execute()
        lead_name = ""
        if lead_row.data:
            lead_name = f"{lead_row.data[0].get('first_name','')} {lead_row.data[0].get('last_name','')}".strip()
        try:
            create_deal_renewal_tasks(db, id, deal["id"], lead_name, deal["end_date"])
        except Exception:
            pass

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
        "status", "supplier", "plan_name", "product_type", "contract_term",
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
