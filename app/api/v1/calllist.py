from fastapi import APIRouter, Query, Depends
from typing import Optional
from datetime import datetime, timezone, timedelta, date
from app.db.client import get_client
from app.auth.deps import get_current_user, UserContext

router = APIRouter()

def _days_until(date_str: Optional[str]) -> Optional[int]:
    if not date_str:
        return None
    try:
        d = date.fromisoformat(date_str[:10])
        return (d - datetime.now(timezone.utc).date()).days
    except Exception:
        return None


def _is_month_to_month(deal: dict) -> bool:
    """Month-to-month plans have no renewal date to chase — keep them off the
    call list. The marker varies by import: rate_type 'Month-Month',
    contract_term 'Month to Month', or a plan name containing it."""
    for field in ("rate_type", "contract_term", "plan_name", "product_type"):
        v = str(deal.get(field) or "").lower().replace("-", " ").replace("_", " ")
        if "month to month" in v or "month month" in v:
            return True
    return False


def _fetch_all(db, table: str, cols: str, filters: list) -> list:
    out, off = [], 0
    while True:
        q = db.table(table).select(cols)
        for fn, args in filters:
            q = getattr(q, fn)(*args)
        page = q.range(off, off + 999).execute().data or []
        out.extend(page)
        if len(page) < 1000:
            break
        off += 1000
    return out


def _score_customer(lead: dict, deals: list) -> tuple[int, list[str], str]:
    score = 0
    reasons = []
    action = "Check in with customer"

    active_deals = [d for d in deals if d.get("status") == "Active"]
    if not active_deals:
        return 0, [], action

    for deal in active_deals:
        days = _days_until(deal.get("end_date"))

        if days is not None:
            if days < 0:
                score += 100
                reasons.append(f"Contract EXPIRED {-days} day{'s' if days != -1 else ''} ago — "
                               f"customer is on holdover pricing")
                action = "Call now — contract already expired"
            elif days <= 7:
                score += 100
                reasons.append(f"Contract expires in {days} day{'s' if days != 1 else ''} — URGENT")
                action = "Renew NOW — contract expiring"
            elif days <= 30:
                score += 80
                reasons.append(f"Contract expires in {days} days")
                action = "Call for renewal — URGENT"
            elif days <= 60:
                score += 50
                reasons.append(f"Contract expires in {days} days")
                action = "Call for renewal"
            elif days <= 90:
                score += 25
                reasons.append(f"Renewal window opens soon ({days} days)")
                action = "Start renewal conversation"

        # Boost commercial accounts
        if str(deal.get("product_type") or "").lower() == "commercial":
            score += 15

    return min(score, 100), reasons, action


@router.get("")
def get_call_list(
    priority_filter: Optional[str] = Query(None),
    limit:           int           = Query(50),
    user: UserContext = Depends(get_current_user),
):
    db = get_client()
    results = []

    # Fetch all converted customers via lead_customers
    customers = db.table("lead_customers").select(
        "id, lead_id, leads(first_name, last_name, phone, sgp_customer_id, sales_agent)"
    ).range(0, 499).execute().data or []

    if not customers:
        return []

    lead_ids = [c["lead_id"] for c in customers if c.get("lead_id")]

    # Batch fetch all active deals; month-to-month plans are excluded — there
    # is no contract end date to call about, and customers whose only plan is
    # month-to-month drop off the list entirely (they score 0 below).
    all_deals = db.table("lead_deals").select(
        "id, lead_id, status, end_date, supplier, plan_name, product_type, est_kwh, adder, "
        "rate_type, contract_term"
    ).in_("lead_id", lead_ids).eq("status", "Active").execute().data or []
    all_deals = [d for d in all_deals if not _is_month_to_month(d)]

    deals_by_lead: dict = {}
    for d in all_deals:
        deals_by_lead.setdefault(d["lead_id"], []).append(d)

    # Sales agents only see their own customers — if no agent name mapped, return nothing
    if user.is_sales_agent and not user.sales_agent_name:
        return []
    agent_name = user.sales_agent_name if user.is_sales_agent else None

    for c in customers:
        lead_id = c.get("lead_id")
        lead = c.get("leads") or {}
        deals = deals_by_lead.get(lead_id, [])

        # Sales agents only see their own customers
        if agent_name and (lead.get("sales_agent") or "").lower() != agent_name.lower():
            continue

        score, reasons, action = _score_customer(lead, deals)
        if score == 0:
            continue

        # Pick the most urgent deal
        active = [d for d in deals if d.get("status") == "Active"]
        active.sort(key=lambda d: (_days_until(d.get("end_date")) or 9999))
        top_deal = active[0] if active else None

        results.append({
            "name":            f"{lead.get('first_name', '')} {lead.get('last_name', '')}".strip(),
            "type":            "Customer",
            "phone":           lead.get("phone") or "—",
            "sgp_customer_id": lead.get("sgp_customer_id"),
            "sales_agent":     lead.get("sales_agent"),
            "supplier":        top_deal.get("supplier") if top_deal else None,
            "plan_name":       top_deal.get("plan_name") if top_deal else None,
            "end_date":        top_deal.get("end_date") if top_deal else None,
            "days_left":       _days_until(top_deal.get("end_date")) if top_deal else None,
            "priority_score":  score,
            "reason":          " · ".join(reasons),
            "action":          action,
            "lead_id":         lead_id,
            "entity_url":      f"/crm/leads/{lead_id}",
        })

    # ── CRM book (crm_deals — most of the business, ~5k active deals) ──
    crm_deals = _fetch_all(
        db, "crm_deals",
        "id, customer_id, deal_status, provider, deal_name, product_type, contract_term, "
        "meter_type, contract_end_date, sales_agent, business_name, "
        "crm_customers(full_name, phone)",
        filters=[("eq", ("deal_status", "ACTIVE"))])

    by_customer: dict = {}
    for d in crm_deals:
        if _is_month_to_month(d):
            continue
        if agent_name and (d.get("sales_agent") or "").lower() != agent_name.lower():
            continue
        # normalize to the shape _score_customer expects
        norm = {
            "status": "Active",
            "end_date": d.get("contract_end_date"),
            # meter_type carries Commercial/Residential for the scoring boost
            "product_type": d.get("meter_type"),
            "supplier": d.get("provider"),
            "plan_name": d.get("deal_name") or d.get("product_type"),
            "sales_agent": d.get("sales_agent"),
            "customer": d.get("crm_customers") or {},
            "business_name": d.get("business_name"),
            "customer_id": d.get("customer_id"),
        }
        key = d.get("customer_id") or f"deal:{d['id']}"
        by_customer.setdefault(key, []).append(norm)

    for key, deals in by_customer.items():
        score, reasons, action = _score_customer({}, deals)
        if score == 0:
            continue
        deals.sort(key=lambda d: (_days_until(d.get("end_date")) or 9999))
        top = deals[0]
        cust = top.get("customer") or {}
        name = (cust.get("full_name") or top.get("business_name")
                or top.get("plan_name") or "Unknown")
        customer_id = top.get("customer_id")
        results.append({
            "name":            name,
            "type":            "Customer",
            "phone":           cust.get("phone") or "—",
            "sgp_customer_id": None,
            "sales_agent":     top.get("sales_agent"),
            "supplier":        top.get("supplier"),
            "plan_name":       top.get("plan_name"),
            "end_date":        top.get("end_date"),
            "days_left":       _days_until(top.get("end_date")),
            "priority_score":  score,
            "reason":          " · ".join(reasons),
            "action":          action,
            "lead_id":         None,
            "entity_url":      f"/crm/customers/{customer_id}" if customer_id else "/crm/deals",
        })

    results.sort(key=lambda x: x["priority_score"], reverse=True)

    if priority_filter == "high":
        results = [r for r in results if r["priority_score"] >= 75]

    return results[:limit]
