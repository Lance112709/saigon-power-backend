from fastapi import APIRouter, Query
from typing import Optional
from datetime import datetime, timezone, timedelta
from app.db.client import get_client

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
        kwh = float(deal.get("est_kwh") or 0)
        adder = float(deal.get("adder") or 0)
        return kwh * adder
    except Exception:
        return 0.0

def _score_lead(lead: dict, deals: list, tasks: list) -> tuple[int, list[str], str]:
    now = _now()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = today_start + timedelta(days=1)
    score = 0
    reasons = []

    # Task signals
    overdue_tasks = [
        t for t in tasks
        if t.get("status") != "completed"
        and t.get("due_date")
        and datetime.fromisoformat(t["due_date"].replace("Z", "+00:00")) < now
    ]
    today_tasks = [
        t for t in tasks
        if t.get("status") != "completed"
        and t.get("due_date")
        and today_start <= datetime.fromisoformat(t["due_date"].replace("Z", "+00:00")) < today_end
    ]
    has_high_task = any(t.get("priority") == "high" for t in tasks if t.get("status") != "completed")
    completed_tasks = [t for t in tasks if t.get("status") == "completed"]

    if overdue_tasks:
        days_overdue = int((now - datetime.fromisoformat(overdue_tasks[0]["due_date"].replace("Z", "+00:00"))).days)
        score += 40
        reasons.append(f"Follow-up overdue by {days_overdue} day{'s' if days_overdue != 1 else ''}")

    if today_tasks:
        score += 25
        reasons.append("Follow-up due today")

    if has_high_task:
        score += 10

    # Lead age signals
    try:
        created = datetime.fromisoformat(lead.get("created_at", "").replace("Z", "+00:00"))
        age_days = (now - created).days
        if age_days <= 3:
            score += 20
            reasons.append("New lead — high conversion window")
        elif age_days >= 7 and not completed_tasks:
            score += 10
            reasons.append(f"No response in {age_days} days")
    except Exception:
        pass

    # Deal signals
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

        # Active deal with no recent contact
        last_contact = None
        if completed_tasks:
            try:
                last_contact = max(
                    datetime.fromisoformat(t["completed_at"].replace("Z", "+00:00"))
                    for t in completed_tasks
                    if t.get("completed_at")
                )
            except Exception:
                pass
        if last_contact is None or (now - last_contact).days > 14:
            if days is None or days > 30:  # don't double-count expiry deals
                score += 25
                if "no recent contact" not in " ".join(reasons):
                    reasons.append("Active deal — no recent contact")

        # Commercial bonus
        if str(deal.get("product_type") or "").lower() == "commercial":
            score += 15

        # High commission bonus
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

    overdue_tasks = [
        t for t in tasks
        if t.get("status") != "completed"
        and t.get("due_date")
        and datetime.fromisoformat(t["due_date"].replace("Z", "+00:00")) < now
    ]
    today_tasks = [
        t for t in tasks
        if t.get("status") != "completed"
        and t.get("due_date")
        and today_start <= datetime.fromisoformat(t["due_date"].replace("Z", "+00:00")) < today_end
    ]
    has_high_task = any(t.get("priority") == "high" for t in tasks if t.get("status") != "completed")

    if overdue_tasks:
        days_overdue = int((now - datetime.fromisoformat(overdue_tasks[0]["due_date"].replace("Z", "+00:00"))).days)
        score += 40
        reasons.append(f"Follow-up overdue by {days_overdue} day{'s' if days_overdue != 1 else ''}")

    if today_tasks:
        score += 25
        reasons.append("Follow-up due today")

    if has_high_task:
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
    type_filter:     Optional[str] = Query(None),   # leads | customers
    priority_filter: Optional[str] = Query(None),   # high
    limit:           int           = Query(20),
):
    db = get_client()
    now = _now()
    results = []

    # ── Pool A: Leads ──────────────────────────────────────────────────────────
    if type_filter in (None, "leads"):
        leads_res = db.table("leads").select("*").neq("status", "inactive").range(0, 499).execute()
        for lead in (leads_res.data or []):
            lid = lead["id"]
            deals = db.table("lead_deals").select("*").eq("lead_id", lid).execute().data or []
            tasks = db.table("tasks").select("*").eq("lead_id", lid).neq("status", "completed").execute().data or []
            completed_tasks = db.table("tasks").select("*").eq("lead_id", lid).eq("status", "completed").execute().data or []
            all_tasks = tasks + completed_tasks

            score, reasons, action = _score_lead(lead, deals, all_tasks)
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
        cust_res = db.table("crm_customers").select("*").range(0, 499).execute()
        for cust in (cust_res.data or []):
            cid = cust["id"]
            deals = db.table("crm_deals").select("*").eq("customer_id", cid).execute().data or []
            tasks = db.table("tasks").select("*").eq("customer_id", cid).execute().data or []

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

    # ── Sort + filter ──────────────────────────────────────────────────────────
    results.sort(key=lambda x: x["priority_score"], reverse=True)

    if priority_filter == "high":
        results = [r for r in results if r["priority_score"] >= 75]

    return results[:limit]
