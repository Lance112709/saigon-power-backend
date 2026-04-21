from fastapi import APIRouter, Query, Depends
from typing import Optional
from datetime import datetime, timezone, timedelta
from app.db.client import get_client
from app.auth.deps import get_current_user, UserContext

router = APIRouter()

def _now() -> datetime:
    return datetime.now(timezone.utc)

def _days_until(date_str: Optional[str]) -> Optional[int]:
    if not date_str:
        return None
    try:
        d = datetime.strptime(date_str[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return (d - _now()).days
    except Exception:
        return None

def _est_commission(deal: dict) -> float:
    try:
        return float(deal.get("est_kwh") or 0) * float(deal.get("adder") or 0)
    except Exception:
        return 0.0

def _score_lead(lead: dict, deals: list, tasks: list) -> tuple[int, list[str], str]:
    now = _now()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = today_start + timedelta(days=1)
    score = 0
    reasons = []

    def _dt(s):
        return datetime.fromisoformat(s.replace("Z", "+00:00"))

    overdue = [t for t in tasks if t.get("status") != "completed" and t.get("due_date") and _dt(t["due_date"]) < now]
    today_t = [t for t in tasks if t.get("status") != "completed" and t.get("due_date") and today_start <= _dt(t["due_date"]) < today_end]
    completed = [t for t in tasks if t.get("status") == "completed"]
    has_high = any(t.get("priority") == "high" for t in tasks if t.get("status") != "completed")

    if overdue:
        days_overdue = int((now - _dt(overdue[0]["due_date"])).days)
        score += 40
        reasons.append(f"Follow-up overdue by {days_overdue} day{'s' if days_overdue != 1 else ''}")
    if today_t:
        score += 25
        reasons.append("Follow-up due today")
    if has_high:
        score += 10

    try:
        age_days = (now - _dt(lead.get("created_at", ""))).days
        if age_days <= 3:
            score += 20
            reasons.append("New lead — high conversion window")
        elif age_days >= 7 and not completed:
            score += 10
            reasons.append(f"No response in {age_days} days")
    except Exception:
        pass

    active_deals = [d for d in deals if d.get("status") == "Active"]
    action = "Call to qualify"

    for deal in active_deals:
        days = _days_until(deal.get("end_date"))
        if days is not None:
            if days <= 30:
                score += 35
                reasons.append(f"Contract expires in {days} day{'s' if days != 1 else ''}")
                action = "Call for renewal — URGENT"
            elif days <= 60:
                score += 20
                reasons.append(f"Contract expires in {days} days")
                action = "Call for renewal"
            elif days <= 90:
                score += 5
                reasons.append(f"Renewal window opens soon ({days} days)")
                action = "Start renewal conversation"

        last_contact = None
        if completed:
            try:
                last_contact = max(_dt(t["completed_at"]) for t in completed if t.get("completed_at"))
            except Exception:
                pass
        if last_contact is None or (now - last_contact).days > 14:
            if days is None or days > 30:
                score += 25
                if "no recent contact" not in " ".join(reasons):
                    reasons.append("Active deal — no recent contact")

        if str(deal.get("product_type") or "").lower() == "commercial":
            score += 15
        if _est_commission(deal) > 100:
            score += 15

    if not reasons:
        reasons.append("Scheduled follow-up")

    return min(score, 100), reasons, action


def _score_crm_customer(customer: dict, deals: list, tasks: list) -> tuple[int, list[str], str]:
    now = _now()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = today_start + timedelta(days=1)
    score = 0
    reasons = []

    def _dt(s):
        return datetime.fromisoformat(s.replace("Z", "+00:00"))

    overdue = [t for t in tasks if t.get("status") != "completed" and t.get("due_date") and _dt(t["due_date"]) < now]
    today_t = [t for t in tasks if t.get("status") != "completed" and t.get("due_date") and today_start <= _dt(t["due_date"]) < today_end]
    has_high = any(t.get("priority") == "high" for t in tasks if t.get("status") != "completed")

    if overdue:
        days_overdue = int((now - _dt(overdue[0]["due_date"])).days)
        score += 40
        reasons.append(f"Follow-up overdue by {days_overdue} day{'s' if days_overdue != 1 else ''}")
    if today_t:
        score += 25
        reasons.append("Follow-up due today")
    if has_high:
        score += 10

    active_deals = [d for d in deals if d.get("deal_status") == "ACTIVE"]
    action = "Check in with customer"

    for deal in active_deals:
        days = _days_until(deal.get("contract_end_date"))
        if days is not None:
            if days <= 30:
                score += 35
                reasons.append(f"Contract expires in {days} day{'s' if days != 1 else ''}")
                action = "Call for renewal — URGENT"
            elif days <= 60:
                score += 20
                reasons.append(f"Contract expires in {days} days")
                action = "Call for renewal"
            elif days <= 90:
                score += 5
                reasons.append(f"Renewal window opens soon ({days} days)")
                action = "Start renewal conversation"

        if str(deal.get("meter_type") or "").lower() == "commercial":
            score += 15

    if not reasons:
        reasons.append("Scheduled check-in")

    return min(score, 100), reasons, action


@router.get("")
def get_call_list(
    type_filter:     Optional[str] = Query(None),
    priority_filter: Optional[str] = Query(None),
    limit:           int           = Query(20),
    user: UserContext = Depends(get_current_user),
):
    db = get_client()
    results = []

    # ── Pool A: Leads ──────────────────────────────────────────────────────────
    if type_filter in (None, "leads"):
        leads = db.table("leads").select("*").neq("status", "inactive").range(0, 199).execute().data or []

        if leads:
            lead_ids = [l["id"] for l in leads]

            # Batch fetch all deals and tasks for all leads at once
            all_lead_deals = db.table("lead_deals").select("*").in_("lead_id", lead_ids).execute().data or []
            all_lead_tasks = db.table("tasks").select("*").in_("lead_id", lead_ids).execute().data or []

            # Group by lead_id
            deals_by_lead: dict = {}
            for d in all_lead_deals:
                deals_by_lead.setdefault(d["lead_id"], []).append(d)

            tasks_by_lead: dict = {}
            for t in all_lead_tasks:
                lid = t.get("lead_id")
                if lid:
                    tasks_by_lead.setdefault(lid, []).append(t)

            for lead in leads:
                lid = lead["id"]
                deals = deals_by_lead.get(lid, [])
                tasks = tasks_by_lead.get(lid, [])

                score, reasons, action = _score_lead(lead, deals, tasks)
                if score == 0:
                    continue

                active_deals = [d for d in deals if d.get("status") == "Active"]
                deal_id = active_deals[0]["id"] if active_deals else (deals[0]["id"] if deals else None)

                results.append({
                    "name":           f"{lead.get('first_name', '')} {lead.get('last_name', '')}".strip(),
                    "type":           "Lead",
                    "phone":          lead.get("phone") or "—",
                    "priority_score": score,
                    "reason":         " · ".join(reasons),
                    "action":         action,
                    "lead_id":        lid,
                    "deal_id":        deal_id,
                    "entity_url":     f"/crm/leads/{lid}",
                })

    # ── Pool B: CRM Customers ──────────────────────────────────────────────────
    if type_filter in (None, "customers"):
        customers = db.table("crm_customers").select("*").range(0, 199).execute().data or []

        if customers:
            cust_ids = [c["id"] for c in customers]

            all_crm_deals = db.table("crm_deals").select("*").in_("customer_id", cust_ids).execute().data or []
            all_crm_tasks = db.table("tasks").select("*").in_("customer_id", cust_ids).execute().data or []

            deals_by_cust: dict = {}
            for d in all_crm_deals:
                deals_by_cust.setdefault(d["customer_id"], []).append(d)

            tasks_by_cust: dict = {}
            for t in all_crm_tasks:
                cid = t.get("customer_id")
                if cid:
                    tasks_by_cust.setdefault(cid, []).append(t)

            for cust in customers:
                cid = cust["id"]
                deals = deals_by_cust.get(cid, [])
                tasks = tasks_by_cust.get(cid, [])

                score, reasons, action = _score_crm_customer(cust, deals, tasks)
                if score == 0:
                    continue

                active_deals = [d for d in deals if d.get("deal_status") == "ACTIVE"]
                deal_id = active_deals[0]["id"] if active_deals else None

                results.append({
                    "name":           cust.get("full_name") or f"{cust.get('first_name','')} {cust.get('last_name','')}".strip(),
                    "type":           "Customer",
                    "phone":          cust.get("phone") or "—",
                    "priority_score": score,
                    "reason":         " · ".join(reasons),
                    "action":         action,
                    "lead_id":        None,
                    "deal_id":        deal_id,
                    "entity_url":     f"/crm/customers/{cid}",
                })

    results.sort(key=lambda x: x["priority_score"], reverse=True)

    if priority_filter == "high":
        results = [r for r in results if r["priority_score"] >= 75]

    return results[:limit]
