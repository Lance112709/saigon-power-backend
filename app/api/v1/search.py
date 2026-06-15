from fastapi import APIRouter, Query, Depends
from app.auth.deps import get_current_user, UserContext
from app.db.client import get_client

router = APIRouter()


@router.get("")
def global_search(
    q: str = Query(..., min_length=2, max_length=100),
    user: UserContext = Depends(get_current_user),
):
    db = get_client()
    q = q.strip()
    results = []
    seen_lead_ids: set = set()
    seen_cust_ids: set = set()

    # ── 1. Pipeline leads (name, email, phone, address) ────────────────────────
    leads = db.table("leads").select(
        "id, first_name, last_name, email, phone, address, city, state, status"
    ).or_(
        f"first_name.ilike.%{q}%,"
        f"last_name.ilike.%{q}%,"
        f"email.ilike.%{q}%,"
        f"phone.ilike.%{q}%,"
        f"address.ilike.%{q}%,"
        f"city.ilike.%{q}%"
    ).limit(8).execute().data or []

    for l in leads:
        seen_lead_ids.add(l["id"])
        results.append({
            "type": "lead",
            "id": l["id"],
            "name": f"{l.get('first_name','') or ''} {l.get('last_name','') or ''}".strip() or "Unknown",
            "sub": l.get("phone") or l.get("email") or "",
            "detail": ", ".join(x for x in [l.get("address"), l.get("city"), l.get("state")] if x),
            "status": l.get("status") or "",
            "url": f"/crm/leads/{l['id']}",
        })

    # ── 2. Pipeline lead_deals (esiid, service_address → parent lead) ──────────
    ld_rows = db.table("lead_deals").select(
        "id, esiid, service_address, supplier, leads(id, first_name, last_name, phone, status)"
    ).or_(
        f"esiid.ilike.%{q}%,"
        f"service_address.ilike.%{q}%"
    ).limit(8).execute().data or []

    for d in ld_rows:
        lead = d.get("leads") or {}
        lid = lead.get("id")
        if not lid or lid in seen_lead_ids:
            continue
        seen_lead_ids.add(lid)
        results.append({
            "type": "lead",
            "id": lid,
            "name": f"{lead.get('first_name','') or ''} {lead.get('last_name','') or ''}".strip() or "Unknown",
            "sub": d.get("esiid") or lead.get("phone") or "",
            "detail": d.get("service_address") or "",
            "status": lead.get("status") or "",
            "url": f"/crm/leads/{lid}",
        })

    # ── 3. Imported customers (name, email, phone, address) ────────────────────
    customers = db.table("crm_customers").select(
        "id, full_name, first_name, last_name, email, phone, mailing_address, city, state"
    ).or_(
        f"full_name.ilike.%{q}%,"
        f"first_name.ilike.%{q}%,"
        f"last_name.ilike.%{q}%,"
        f"email.ilike.%{q}%,"
        f"phone.ilike.%{q}%,"
        f"mailing_address.ilike.%{q}%,"
        f"city.ilike.%{q}%"
    ).limit(8).execute().data or []

    for c in customers:
        seen_cust_ids.add(c["id"])
        name = c.get("full_name") or f"{c.get('first_name','') or ''} {c.get('last_name','') or ''}".strip() or "Unknown"
        results.append({
            "type": "customer",
            "id": c["id"],
            "name": name,
            "sub": c.get("phone") or c.get("email") or "",
            "detail": ", ".join(x for x in [c.get("mailing_address"), c.get("city"), c.get("state")] if x),
            "status": "Imported",
            "url": f"/crm/customers/{c['id']}",
        })

    # ── 4. Imported crm_deals (esiid, service_address, business_name → customer) ─
    crm_deals = db.table("crm_deals").select(
        "id, esiid, service_address, business_name, deal_status, crm_customers(id, full_name, phone)"
    ).or_(
        f"esiid.ilike.%{q}%,"
        f"service_address.ilike.%{q}%,"
        f"business_name.ilike.%{q}%"
    ).limit(8).execute().data or []

    for d in crm_deals:
        cust = d.get("crm_customers") or {}
        cid = cust.get("id")
        if not cid or cid in seen_cust_ids:
            continue
        seen_cust_ids.add(cid)
        results.append({
            "type": "customer",
            "id": cid,
            "name": cust.get("full_name") or d.get("business_name") or "Unknown",
            "sub": d.get("esiid") or cust.get("phone") or "",
            "detail": d.get("service_address") or "",
            "status": "Imported",
            "url": f"/crm/customers/{cid}",
        })

    return results[:20]
