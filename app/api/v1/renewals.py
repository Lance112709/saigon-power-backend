from fastapi import APIRouter, Body, Depends, HTTPException, Query
from typing import Optional
from datetime import date, datetime, timezone
from app.db.client import get_client
from app.api.v1.auth import get_current_user, UserContext
from app.utils.deals import is_month_to_month

router = APIRouter()

@router.get("/filters")
def get_renewal_filters(user: UserContext = Depends(get_current_user)):
    db = get_client()
    providers, agents = set(), set()

    for r in db.table("lead_deals").select("supplier, sales_agent").eq("status", "Active").execute().data:
        if r.get("supplier"): providers.add(r["supplier"].strip())
        if r.get("sales_agent"): agents.add(r["sales_agent"].strip())

    for r in db.table("crm_deals").select("provider, sales_agent").eq("deal_status", "ACTIVE").execute().data:
        if r.get("provider"): providers.add(r["provider"].strip())
        if r.get("sales_agent"): agents.add(r["sales_agent"].strip())

    return {
        "providers": sorted(providers, key=str.upper),
        "agents": sorted(agents, key=str.upper),
    }

@router.get("")
def get_renewals(
    start_date: Optional[str] = Query(None),
    end_date:   Optional[str] = Query(None),
    provider:   Optional[str] = Query(None),
    sales_agent: Optional[str] = Query(None),
    user: UserContext = Depends(get_current_user),
):
    db = get_client()
    today = datetime.now(timezone.utc).date().isoformat()

    agent_filter = user.sales_agent_name.lower() if user.is_sales_agent and user.sales_agent_name else None
    sa = (sales_agent or "").strip().lower() or None

    results = []

    # ── CRM Leads deals ──────────────────────────────────────────────────────────
    q = db.table("lead_deals").select(
        "id, end_date, supplier, plan_name, contract_term, rate, rate_type, "
        "lead_id, sales_agent, status, "
        "leads(first_name, last_name, phone, email, sgp_customer_id)"
    ).eq("status", "Active")

    if start_date: q = q.gte("end_date", start_date)
    if end_date:   q = q.lte("end_date", end_date)
    if provider:   q = q.ilike("supplier", f"%{provider}%")
    if sa:         q = q.ilike("sales_agent", f"%{sa}%")
    if agent_filter and not sa:
        q = q.ilike("sales_agent", f"%{agent_filter}%")

    for d in q.order("end_date").execute().data:
        if is_month_to_month(d.get("rate_type"), d.get("plan_name"), d.get("contract_term")):
            continue  # month-to-month has no contract end to renew
        lead = d.pop("leads", None) or {}
        end = d.get("end_date") or ""
        try:
            days_left = (date.fromisoformat(end) - date.fromisoformat(today)).days if end else None
        except: days_left = None
        results.append({
            "deal_id":      d["id"],
            "lead_id":      d.get("lead_id"),
            "customer_id":  None,
            "source":       "crm",
            "full_name":    f"{lead.get('first_name','')} {lead.get('last_name','')}".strip(),
            "phone":        lead.get("phone"),
            "email":        lead.get("email"),
            "sgp_id":       lead.get("sgp_customer_id"),
            "provider":     d.get("supplier"),
            "plan_name":    d.get("plan_name"),
            "rate":         d.get("rate"),
            "rate_type":    d.get("rate_type"),
            "contract_term": d.get("contract_term"),
            "sales_agent":  d.get("sales_agent"),
            "end_date":     end,
            "days_left":    days_left,
        })

    # ── Imported Customers deals ─────────────────────────────────────────────────
    q2 = db.table("crm_deals").select(
        "id, contract_end_date, provider, product_type, contract_term, energy_rate, "
        "customer_id, sales_agent, deal_status, "
        "crm_customers(full_name, phone, email)"
    ).eq("deal_status", "ACTIVE")

    if start_date: q2 = q2.gte("contract_end_date", start_date)
    if end_date:   q2 = q2.lte("contract_end_date", end_date)
    if provider:   q2 = q2.ilike("provider", f"%{provider}%")
    if sa:         q2 = q2.ilike("sales_agent", f"%{sa}%")
    if agent_filter and not sa:
        q2 = q2.ilike("sales_agent", f"%{agent_filter}%")

    for d in q2.order("contract_end_date").execute().data:
        if is_month_to_month(d.get("product_type"), d.get("contract_term")):
            continue  # month-to-month has no contract end to renew
        cust = d.pop("crm_customers", None) or {}
        end = (d.get("contract_end_date") or "")[:10]
        try:
            days_left = (date.fromisoformat(end) - date.fromisoformat(today)).days if end else None
        except: days_left = None
        results.append({
            "deal_id":      d["id"],
            "lead_id":      None,
            "customer_id":  d.get("customer_id"),
            "source":       "imported",
            "full_name":    cust.get("full_name", ""),
            "phone":        cust.get("phone"),
            "email":        cust.get("email"),
            "sgp_id":       None,
            "provider":     d.get("provider"),
            "plan_name":    d.get("product_type"),
            "rate":         d.get("energy_rate"),
            "rate_type":    None,
            "contract_term": d.get("contract_term"),
            "sales_agent":  d.get("sales_agent"),
            "end_date":     end,
            "days_left":    days_left,
        })

    results.sort(key=lambda x: x["end_date"] or "9999")
    return results


@router.post("/email")
def email_renewal(data: dict = Body(...), user: UserContext = Depends(get_current_user)):
    """Email a personalized renewal offer for one expiring deal."""
    from app.services.customer_email import send_email, renewal_email_html
    from app.services.audit import audit
    db = get_client()
    source, deal_id = data.get("source"), data.get("deal_id")

    if source == "crm":
        d = db.table("lead_deals").select("*, leads(first_name, last_name, email)").eq("id", deal_id).limit(1).execute().data
        if not d:
            raise HTTPException(status_code=404, detail="Deal not found")
        d = d[0]
        who = d.get("leads") or {}
        name = f"{who.get('first_name','')} {who.get('last_name','')}".strip()
        email = (data.get("email") or who.get("email") or "").strip()
        provider, plan, end = d.get("supplier"), d.get("plan_name") or d.get("rate_type"), d.get("end_date")
    else:
        d = db.table("crm_deals").select("*, crm_customers(full_name, email)").eq("id", deal_id).limit(1).execute().data
        if not d:
            raise HTTPException(status_code=404, detail="Deal not found")
        d = d[0]
        who = d.get("crm_customers") or {}
        name = who.get("full_name") or ""
        email = (data.get("email") or who.get("email") or "").strip()
        provider, plan, end = d.get("provider"), d.get("product_type"), d.get("contract_end_date")

    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="No email on file for this customer — add one first.")

    end_str = str(end)[:10] if end else None
    days_left = None
    try:
        days_left = (date.fromisoformat(end_str) - datetime.now(timezone.utc).date()).days
    except Exception:
        pass

    plans = db.table("landing_plans").select("plan_name, rate, term_months").order("sort_order").limit(3).execute().data or []
    result = send_email(
        email,
        f"{(name or 'Your').split(' ')[0]}, your electricity contract ends "
        f"{'in ' + str(days_left) + ' days' if days_left is not None else 'soon'} — let's beat your rate",
        renewal_email_html(name, provider, plan, end_str, days_left, plans),
    )
    if not result.get("ok"):
        raise HTTPException(status_code=503, detail=result.get("error", "Email failed"))
    audit(db, source or "deal", str(deal_id), "emailed_renewal", None,
          {"to": email, "days_left": days_left},
          reason="Renewal offer emailed from CRM", actor=user.email or "staff")
    return {"ok": True, "to": email}
