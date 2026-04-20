from fastapi import APIRouter, HTTPException, Query, Body, Depends
from typing import Optional
import re
from app.db.client import get_client
from app.auth.deps import get_current_user, require_admin, require_manager, UserContext

router = APIRouter()

# Provider name → supplier code
PROVIDER_TO_CODE = {
    "BUDGET POWER": "BUDGET",
    "IRON HORSE": "IRONHORSE",
    "HERITAGE POWER": "HERITAGE",
    "NRG ENERGY": "NRG_COMM",
    "DISCOUNT POWER": "NRG",
    "CHARIOT ENERGY": "CHARIOT",
    "CLEANSKY ENERGY": "CLEANSKY",
    "HUDSON ENERGY": "HUDSON",
}

def _extract_email(contact_str: str) -> Optional[str]:
    if not contact_str:
        return None
    m = re.search(r"\(([^)@]+@[^)]+)\)", str(contact_str))
    return m.group(1).strip().lower() if m else None

def _to_date_str(val) -> Optional[str]:
    if not val or str(val).strip() in ("", "nan", "None", "NaT"):
        return None
    try:
        from datetime import datetime as dt
        if hasattr(val, "date"):
            return val.date().isoformat()
        s = str(val).strip()
        for fmt in ("%m/%d/%Y %H:%M", "%m/%d/%Y", "%Y-%m-%d"):
            try:
                return dt.strptime(s[:16] if ":" in s else s[:10], fmt).date().isoformat()
            except Exception:
                continue
    except Exception:
        pass
    return None

def _to_float(val) -> Optional[float]:
    try:
        return float(str(val).replace(",", "").strip()) if val not in (None, "", "nan") else None
    except Exception:
        return None

def _enrich_deals(db, deals: list) -> None:
    """Backfill energy_rate and sales_agent from actual_commissions by ESIID."""
    missing_rate  = [d["esiid"] for d in deals if not d.get("energy_rate") and d.get("esiid")]
    missing_agent = [d["esiid"] for d in deals if not d.get("sales_agent") and d.get("esiid")]
    esiids_needed = list(set(missing_rate + missing_agent))
    if not esiids_needed:
        return
    comm_res = db.table("actual_commissions").select(
        "raw_esiid, raw_rate, raw_row_data, billing_month"
    ).in_("raw_esiid", esiids_needed).order("billing_month", desc=True).limit(len(esiids_needed) * 3).execute()
    latest: dict = {}
    for row in (comm_res.data or []):
        esiid = row.get("raw_esiid")
        if esiid and esiid not in latest:
            latest[esiid] = row
    for deal in deals:
        esiid = deal.get("esiid")
        if esiid and esiid in latest:
            comm = latest[esiid]
            if not deal.get("energy_rate") and comm.get("raw_rate"):
                deal["energy_rate"] = comm["raw_rate"]
            if not deal.get("sales_agent"):
                raw = comm.get("raw_row_data") or {}
                for key in ("AE Name", "Agent", "Sales Rep", "Rep Name", "Salesperson", "sales_agent"):
                    val = raw.get(key)
                    if val and str(val).strip():
                        deal["sales_agent"] = str(val).strip()
                        break

# ── Stats ─────────────────────────────────────────────────────────────────────

@router.get("/stats")
def get_crm_stats():
    db = get_client()
    customers = db.table("crm_customers").select("id", count="exact").execute()
    active = db.table("crm_deals").select("id", count="exact").eq("deal_status", "ACTIVE").execute()
    inactive = db.table("crm_deals").select("id", count="exact").eq("deal_status", "INACTIVE").execute()
    return {
        "total_customers": customers.count or 0,
        "active_deals": active.count or 0,
        "inactive_deals": inactive.count or 0,
    }

# ── Customers ─────────────────────────────────────────────────────────────────

@router.get("/customers")
def list_customers(
    search: Optional[str] = Query(None),
    provider: Optional[str] = Query(None),
    deal_status: Optional[str] = Query(None),
    limit: int = Query(50),
    offset: int = Query(0),
    user: UserContext = Depends(get_current_user),
):
    db = get_client()
    q = db.table("crm_customers").select(
        "id, full_name, first_name, last_name, email, phone, city, state, created_at, "
        "crm_deals(id, deal_status, provider, sales_agent)"
    )
    if search:
        q = q.or_(f"full_name.ilike.%{search}%,email.ilike.%{search}%,phone.ilike.%{search}%")
    res = q.order("full_name").range(offset, offset + limit - 1).execute()

    agent_name = user.sales_agent_name.lower() if user.is_sales_agent and user.sales_agent_name else None

    results = []
    for c in res.data:
        deals = c.get("crm_deals", [])
        # Sales agents only see customers where they have at least one deal
        if agent_name:
            deals = [d for d in deals if (d.get("sales_agent") or "").lower() == agent_name]
            if not deals:
                continue
        # Apply deal filters in Python since we can't filter nested
        if provider:
            deals = [d for d in deals if d.get("provider", "").upper() == provider.upper()]
        if deal_status:
            deals = [d for d in deals if d.get("deal_status", "").upper() == deal_status.upper()]
            if not deals:
                continue
        active_count = sum(1 for d in deals if d.get("deal_status") == "ACTIVE")
        results.append({
            "id": c["id"],
            "full_name": c["full_name"],
            "email": c.get("email"),
            "phone": c.get("phone"),
            "city": c.get("city"),
            "state": c.get("state"),
            "deal_count": len(deals),
            "active_deal_count": active_count,
            "created_at": c.get("created_at"),
        })
    return results

@router.get("/customers/{id}")
def get_customer(id: str, user: UserContext = Depends(get_current_user)):
    db = get_client()
    c = db.table("crm_customers").select("*").eq("id", id).limit(1).execute()
    if not c.data:
        raise HTTPException(status_code=404, detail="Customer not found")
    deals_res = db.table("crm_deals").select("*").eq("customer_id", id).order("deal_status").execute()
    deals = deals_res.data or []
    _enrich_deals(db, deals)
    # Sales agents only see their own deals for this customer
    if user.is_sales_agent and user.sales_agent_name:
        agent_name = user.sales_agent_name.lower()
        deals = [d for d in deals if (d.get("sales_agent") or "").lower() == agent_name]
        if not deals:
            raise HTTPException(status_code=403, detail="Access denied")
    return {**c.data[0], "deals": deals}

@router.get("/customers/{id}/notes")
def get_customer_notes(id: str, user: UserContext = Depends(get_current_user)):
    db = get_client()
    res = db.table("crm_customer_notes").select("*").eq("crm_customer_id", id).order("created_at", desc=True).execute()
    return res.data or []

@router.post("/customers/{id}/notes")
def create_customer_note(id: str, data: dict = Body(...), user: UserContext = Depends(get_current_user)):
    content = str(data.get("content") or "").strip()
    if not content:
        raise HTTPException(status_code=400, detail="Content required")
    author = str(data.get("author_name") or "").strip() or None
    db = get_client()
    res = db.table("crm_customer_notes").insert({"crm_customer_id": id, "content": content, "author_name": author}).execute()
    return res.data[0]

@router.delete("/customers/{id}/notes/{note_id}")
def delete_customer_note(id: str, note_id: str, user: UserContext = Depends(require_admin)):
    db = get_client()
    db.table("crm_customer_notes").delete().eq("id", note_id).eq("crm_customer_id", id).execute()
    return {"ok": True}

@router.patch("/customers/{id}")
def update_customer(id: str, data: dict = Body(...), user: UserContext = Depends(get_current_user)):
    db = get_client()
    allowed = {"full_name", "first_name", "last_name", "email", "phone", "dob",
               "mailing_address", "city", "state", "postal_code", "notes"}
    payload = {k: v for k, v in data.items() if k in allowed}
    if not payload:
        raise HTTPException(status_code=400, detail="No valid fields to update")
    from datetime import datetime, timezone
    payload["updated_at"] = datetime.now(timezone.utc).isoformat()
    res = db.table("crm_customers").update(payload).eq("id", id).execute()
    return res.data[0] if res.data else {}

# ── Deals ─────────────────────────────────────────────────────────────────────

@router.post("/customers/{id}/deals")
def create_customer_deal(id: str, data: dict = Body(...), user: UserContext = Depends(get_current_user)):
    db = get_client()
    c = db.table("crm_customers").select("id").eq("id", id).limit(1).execute()
    if not c.data:
        raise HTTPException(status_code=404, detail="Customer not found")

    def _f(key):
        v = data.get(key)
        try: return float(v) if v not in (None, "", "null") else None
        except (ValueError, TypeError): return None

    payload = {
        "customer_id":           id,
        "deal_name":             str(data.get("deal_name") or "").strip() or None,
        "business_name":         str(data.get("business_name") or "").strip() or None,
        "provider":              str(data.get("provider") or "").strip().upper() or None,
        "esiid":                 str(data.get("esiid") or "").strip() or None,
        "meter_type":            str(data.get("meter_type") or "").strip() or None,
        "deal_type":             str(data.get("deal_type") or "").strip() or None,
        "deal_status":           str(data.get("deal_status") or "ACTIVE").strip().upper(),
        "energy_rate":           _f("energy_rate"),
        "adder":                 _f("adder"),
        "contract_term":         str(data.get("contract_term") or "").strip() or None,
        "contract_start_date":   data.get("contract_start_date") or None,
        "contract_end_date":     data.get("contract_end_date") or None,
        "contract_signed_date":  data.get("contract_signed_date") or None,
        "service_address":       str(data.get("service_address") or "").strip() or None,
        "sales_agent":           str(data.get("sales_agent") or "").strip() or None,
        "deal_owner":            str(data.get("deal_owner") or "").strip() or None,
        "product_type":          str(data.get("product_type") or "").strip() or None,
    }
    res = db.table("crm_deals").insert(payload).execute()
    if not res.data:
        raise HTTPException(status_code=500, detail="Failed to create deal")
    return res.data[0]

@router.get("/deals")
def list_deals(
    search: Optional[str] = Query(None),
    provider: Optional[str] = Query(None),
    deal_status: Optional[str] = Query(None),
    meter_type: Optional[str] = Query(None),
    deal_type: Optional[str] = Query(None),
    sales_agent: Optional[str] = Query(None),
    limit: int = Query(50),
    offset: int = Query(0),
    user: UserContext = Depends(get_current_user),
):
    db = get_client()
    q = db.table("crm_deals").select(
        "*, crm_customers(id, full_name, email)"
    )
    if provider:
        q = q.ilike("provider", f"%{provider}%")
    if deal_status:
        q = q.eq("deal_status", deal_status.upper())
    if meter_type:
        q = q.ilike("meter_type", f"%{meter_type}%")
    if deal_type:
        q = q.ilike("deal_type", f"%{deal_type}%")
    if sales_agent:
        q = q.ilike("sales_agent", f"%{sales_agent}%")
    if search:
        q = q.or_(f"deal_name.ilike.%{search}%,esiid.ilike.%{search}%,service_address.ilike.%{search}%")
    res = q.order("created_at", desc=True).range(offset, offset + limit - 1).execute()
    deals = res.data or []
    _enrich_deals(db, deals)
    # Sales agents only see their own deals
    if user.is_sales_agent and user.sales_agent_name:
        deals = [d for d in deals if (d.get("sales_agent") or "").lower() == user.sales_agent_name.lower()]
    return deals

@router.get("/deals/{id}")
def get_deal(id: str, user: UserContext = Depends(get_current_user)):
    db = get_client()
    res = db.table("crm_deals").select("*, crm_customers(id, full_name, email, phone)").eq("id", id).limit(1).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Deal not found")
    deal = res.data[0]
    _enrich_deals(db, [deal])
    if user.is_sales_agent and user.sales_agent_name:
        if (deal.get("sales_agent") or "").lower() != user.sales_agent_name.lower():
            raise HTTPException(status_code=403, detail="Access denied")
    return deal

@router.get("/deals/{id}/notes")
def get_deal_notes(id: str, user: UserContext = Depends(get_current_user)):
    db = get_client()
    res = db.table("crm_deal_notes").select("*").eq("crm_deal_id", id).order("created_at", desc=True).execute()
    return res.data or []

@router.post("/deals/{id}/notes")
def create_deal_note(id: str, data: dict = Body(...), user: UserContext = Depends(get_current_user)):
    content = str(data.get("content") or "").strip()
    if not content:
        raise HTTPException(status_code=400, detail="Content required")
    author = str(data.get("author_name") or "").strip() or None
    db = get_client()
    res = db.table("crm_deal_notes").insert({"crm_deal_id": id, "content": content, "author_name": author}).execute()
    return res.data[0]

@router.delete("/deals/{id}/notes/{note_id}")
def delete_deal_note(id: str, note_id: str, user: UserContext = Depends(require_admin)):
    db = get_client()
    db.table("crm_deal_notes").delete().eq("id", note_id).eq("crm_deal_id", id).execute()
    return {"ok": True}

@router.patch("/deals/{id}")
def update_deal(id: str, data: dict = Body(...), user: UserContext = Depends(get_current_user)):
    db = get_client()
    allowed = {"deal_status", "deal_name", "provider", "adder", "energy_rate", "deal_owner",
               "sales_agent", "contract_start_date", "contract_end_date", "notes"}
    payload = {k: v for k, v in data.items() if k in allowed}
    if not payload:
        raise HTTPException(status_code=400, detail="No valid fields to update")
    from datetime import datetime, timezone
    payload["updated_at"] = datetime.now(timezone.utc).isoformat()
    res = db.table("crm_deals").update(payload).eq("id", id).execute()
    return res.data[0] if res.data else {}

# ── Import ─────────────────────────────────────────────────────────────────────

@router.post("/import")
def import_deals(file_path: str = Body(..., embed=True), user: UserContext = Depends(require_admin)):
    """
    One-time idempotent import from MERGED_DEALS FINAL.xlsx.
    Deduplicates customers by email (from Associated Contact column).
    """
    import openpyxl

    db = get_client()

    # Load supplier code → id map
    sup_res = db.table("suppliers").select("id, code").execute()
    supplier_map = {s["code"]: s["id"] for s in sup_res.data}

    # Load existing customers by email (idempotency)
    existing_res = db.table("crm_customers").select("id, email").execute()
    customer_by_email: dict = {c["email"]: c["id"] for c in existing_res.data if c.get("email")}

    wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
    ws = wb.active

    rows_iter = ws.iter_rows(values_only=True)
    header = next(rows_iter)  # skip header

    customers_created = 0
    deals_created = 0
    deals_skipped = 0

    for row in rows_iter:
        if len(row) < 20:
            continue

        adder        = _to_float(row[0])
        signed_date  = _to_date_str(row[1])
        end_date     = _to_date_str(row[2])
        rate         = _to_float(row[3])
        start_date   = _to_date_str(row[4])
        term         = str(row[5] or "").strip() or None
        deal_name    = str(row[6] or "").strip() or None
        biz_name     = str(row[7] or "").strip() or None
        deal_owner   = str(row[8] or "").strip() or None
        deal_status  = str(row[9] or "ACTIVE").strip().upper() or "ACTIVE"
        meter_type   = str(row[10] or "").strip() or None
        deal_type    = str(row[11] or "").strip() or None
        esiid        = str(row[12] or "").strip() or None
        product_type = str(row[13] or "").strip() or None
        sales_agent  = str(row[14] or "").strip() or None
        svc_address  = str(row[15] or "").strip() or None
        provider     = str(row[16] or "").strip().upper() or None
        contact      = str(row[17] or "").strip()
        first_name   = str(row[18] or "").strip() or None
        last_name    = str(row[19] or "").strip() or None
        anxh         = str(row[20] or "").strip() or None
        dob          = str(row[21] or "").strip() or None
        email        = str(row[22] or "").strip().lower() or None
        mail_addr    = str(row[23] or "").strip() or None
        city         = str(row[24] or "").strip() or None
        postal       = str(row[25] or "").strip() or None
        state        = str(row[26] or "TX").strip() or "TX"
        phone        = str(row[28] or "").strip() if len(row) > 28 else None

        # Try email from col 22 first, fallback to contact column
        if not email:
            email = _extract_email(contact)

        # Build customer key
        if email:
            cust_key = email
        elif first_name or last_name:
            cust_key = f"{first_name or ''} {last_name or ''}".strip().lower()
        else:
            deals_skipped += 1
            continue

        # Get or create customer
        if cust_key not in customer_by_email:
            full_name = f"{first_name or ''} {last_name or ''}".strip()
            if not full_name:
                # Try extracting name from contact string
                m = re.match(r"^([^(]+)\s*\(", contact)
                full_name = m.group(1).strip() if m else cust_key

            new_cust = {
                "full_name": full_name,
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "phone": phone,
                "dob": dob,
                "mailing_address": mail_addr,
                "city": city,
                "state": state[:2] if state else "TX",
                "postal_code": postal,
            }
            cres = db.table("crm_customers").insert(new_cust).execute()
            customer_id = cres.data[0]["id"]
            customer_by_email[cust_key] = customer_id
            customers_created += 1
        else:
            customer_id = customer_by_email[cust_key]

        # Map provider to supplier_id
        supplier_code = PROVIDER_TO_CODE.get(provider or "")
        supplier_id = supplier_map.get(supplier_code) if supplier_code else None

        # Flag truncated ESIIDs (scientific notation)
        esiid_notes = "ESIID truncated (scientific notation in source file)" if esiid and ("E+" in esiid or "e+" in esiid) else None

        deal = {
            "customer_id": customer_id,
            "deal_name": deal_name,
            "business_name": biz_name,
            "esiid": esiid,
            "provider": provider,
            "supplier_id": supplier_id,
            "meter_type": meter_type,
            "deal_type": deal_type,
            "deal_status": deal_status if deal_status in ("ACTIVE", "INACTIVE") else "ACTIVE",
            "adder": adder,
            "energy_rate": rate,
            "product_type": product_type,
            "contract_term": term,
            "contract_signed_date": signed_date,
            "contract_start_date": start_date,
            "contract_end_date": end_date,
            "service_address": svc_address,
            "deal_owner": deal_owner,
            "sales_agent": sales_agent,
            "anxh": anxh,
        }
        db.table("crm_deals").insert(deal).execute()
        deals_created += 1

    wb.close()
    return {
        "customers_created": customers_created,
        "deals_created": deals_created,
        "deals_skipped": deals_skipped,
    }

# ── Providers list (for filter dropdowns) ─────────────────────────────────────

@router.get("/providers")
def list_providers(user: UserContext = Depends(get_current_user)):
    db = get_client()
    res = db.table("crm_deals").select("provider").execute()
    providers = sorted({r["provider"] for r in res.data if r.get("provider")})
    return providers

@router.get("/agents")
def list_agents(user: UserContext = Depends(get_current_user)):
    db = get_client()
    res = db.table("crm_deals").select("sales_agent").execute()
    agents = sorted({r["sales_agent"] for r in res.data if r.get("sales_agent")})
    return agents
