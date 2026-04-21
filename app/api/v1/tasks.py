from fastapi import APIRouter, HTTPException, Query, Body, Depends
from typing import Optional
from datetime import datetime, timezone, timedelta
from app.db.client import get_client
from app.auth.deps import get_current_user, UserContext

router = APIRouter()

# ── Helpers ───────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

def _due(days: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()

def _effective_status(status: str, due_date: str) -> str:
    if status == "completed":
        return "completed"
    try:
        due = datetime.fromisoformat(due_date.replace("Z", "+00:00"))
        if due < datetime.now(timezone.utc):
            return "overdue"
    except Exception:
        pass
    return "pending"

# ── Auto-task creators (called from leads.py) ─────────────────────────────────

def create_lead_tasks(db, lead_id: str, lead_name: str) -> None:
    existing = db.table("tasks").select("id").eq("lead_id", lead_id).neq("task_type", "renewal_followup").execute()
    if existing.data:
        return
    tasks = [
        {
            "lead_id": lead_id, "task_type": "call",
            "title": f"Initial Contact — {lead_name}",
            "description": "First contact with new lead. Introduce Saigon Power and qualify.",
            "due_date": _due(1), "priority": "high", "status": "pending",
        },
        {
            "lead_id": lead_id, "task_type": "call",
            "title": f"Follow-up Attempt — {lead_name}",
            "description": "Second contact attempt if no response on day 1.",
            "due_date": _due(3), "priority": "medium", "status": "pending",
        },
        {
            "lead_id": lead_id, "task_type": "call",
            "title": f"Final Follow-up — {lead_name}",
            "description": "Final attempt before marking lead as cold.",
            "due_date": _due(7), "priority": "low", "status": "pending",
        },
    ]
    for t in tasks:
        db.table("tasks").insert(t).execute()

def create_deal_renewal_tasks(db, lead_id: str, deal_id: str, lead_name: str, end_date_str: str) -> None:
    if not end_date_str:
        return
    try:
        end_d = datetime.strptime(end_date_str[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return

    existing = db.table("tasks").select("id").eq("deal_id", deal_id).eq("task_type", "renewal_followup").execute()
    if existing.data:
        return

    now = datetime.now(timezone.utc)
    reminders = [
        (120, "medium", "Renewal Reminder — 120 Days",  "Contract expires in 120 days. Begin renewal conversation."),
        (90,  "medium", "Renewal Follow-up — 90 Days",  "Contract expires in 90 days. Confirm renewal intent."),
        (60,  "high",   "Renewal Follow-up — 60 Days",  "Contract expires in 60 days. Send renewal options."),
        (30,  "high",   "URGENT Renewal — 30 Days",     "Contract expires in 30 days. Finalize renewal immediately."),
    ]
    for days_before, priority, title, description in reminders:
        due = end_d - timedelta(days=days_before)
        if due <= now:
            continue
        db.table("tasks").insert({
            "lead_id": lead_id, "deal_id": deal_id,
            "task_type": "renewal_followup",
            "title": f"{title} — {lead_name}",
            "description": description,
            "due_date": due.isoformat(),
            "priority": priority,
            "status": "pending",
        }).execute()

# ── Routes ────────────────────────────────────────────────────────────────────

@router.post("/send-reminders")
def trigger_reminders(user: UserContext = Depends(get_current_user)):
    from app.services.email_reminders import send_task_reminders
    return send_task_reminders()

@router.get("/stats")
def task_stats(user: UserContext = Depends(get_current_user)):
    db = get_client()
    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end   = today_start + timedelta(days=1)
    week_end    = today_start + timedelta(days=7)

    overdue     = db.table("tasks").select("id", count="exact").neq("status", "completed").lt("due_date", now.isoformat()).execute()
    due_today   = db.table("tasks").select("id", count="exact").neq("status", "completed").gte("due_date", today_start.isoformat()).lt("due_date", today_end.isoformat()).execute()
    this_week   = db.table("tasks").select("id", count="exact").neq("status", "completed").gte("due_date", today_end.isoformat()).lt("due_date", week_end.isoformat()).execute()
    pending     = db.table("tasks").select("id", count="exact").neq("status", "completed").execute()

    return {
        "overdue":       overdue.count or 0,
        "due_today":     due_today.count or 0,
        "this_week":     this_week.count or 0,
        "total_pending": pending.count or 0,
    }

@router.get("")
def list_tasks(
    lead_id:      Optional[str] = Query(None),
    deal_id:      Optional[str] = Query(None),
    customer_id:  Optional[str] = Query(None),
    crm_deal_id:  Optional[str] = Query(None),
    window:       Optional[str] = Query(None),
    limit:  int = Query(200),
    offset: int = Query(0),
    user: UserContext = Depends(get_current_user),
):
    db = get_client()
    now         = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end   = today_start + timedelta(days=1)

    q = db.table("tasks").select("*, leads(first_name, last_name), crm_customers(full_name)")

    if lead_id:
        q = q.eq("lead_id", lead_id)
    if deal_id:
        q = q.eq("deal_id", deal_id)
    if customer_id:
        q = q.eq("customer_id", customer_id)
    if crm_deal_id:
        q = q.eq("crm_deal_id", crm_deal_id)

    if window == "overdue":
        q = q.neq("status", "completed").lt("due_date", now.isoformat())
    elif window == "today":
        q = q.neq("status", "completed").gte("due_date", today_start.isoformat()).lt("due_date", today_end.isoformat())
    elif window == "upcoming":
        q = q.neq("status", "completed").gte("due_date", today_end.isoformat())
    elif window == "completed":
        q = q.eq("status", "completed")
    # No window = all tasks

    res = q.order("due_date").range(offset, offset + limit - 1).execute()

    results = []
    for t in res.data:
        lead     = t.pop("leads", None) or {}
        customer = t.pop("crm_customers", None) or {}
        entity_name = (
            f"{lead.get('first_name','')} {lead.get('last_name','')}".strip()
            if lead else customer.get("full_name") or ""
        )
        # Compute effective status dynamically
        t["status"] = _effective_status(t["status"], t.get("due_date") or "")
        results.append({**t, "entity_name": entity_name})

    return results

@router.post("")
def create_task(data: dict = Body(...), user: UserContext = Depends(get_current_user)):
    db = get_client()
    if not data.get("lead_id") and not data.get("deal_id") and not data.get("customer_id") and not data.get("crm_deal_id"):
        raise HTTPException(status_code=400, detail="Task must be linked to a lead, deal, or customer")
    if not str(data.get("title") or "").strip():
        raise HTTPException(status_code=400, detail="Title is required")
    if not data.get("due_date"):
        raise HTTPException(status_code=400, detail="Due date is required")

    payload = {
        "lead_id":     data.get("lead_id") or None,
        "deal_id":     data.get("deal_id") or None,
        "customer_id": data.get("customer_id") or None,
        "crm_deal_id": data.get("crm_deal_id") or None,
        "task_type":   data.get("task_type") or "general",
        "title":       str(data["title"]).strip(),
        "description": str(data.get("description") or "").strip() or None,
        "due_date":    data["due_date"],
        "priority":    data.get("priority") or "medium",
        "status":      "pending",
        "assigned_to": str(data.get("assigned_to") or "").strip() or None,
    }
    res = db.table("tasks").insert(payload).execute()
    return res.data[0]

@router.patch("/{task_id}")
def update_task(task_id: str, data: dict = Body(...), user: UserContext = Depends(get_current_user)):
    db = get_client()
    allowed = {"title", "description", "due_date", "status", "priority", "assigned_to", "task_type"}
    payload = {k: v for k, v in data.items() if k in allowed}
    if not payload:
        raise HTTPException(status_code=400, detail="No valid fields")
    if payload.get("status") == "completed":
        payload["completed_at"] = _now()
    res = db.table("tasks").update(payload).eq("id", task_id).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Task not found")
    return res.data[0]

@router.delete("/{task_id}")
def delete_task(task_id: str, user: UserContext = Depends(get_current_user)):
    db = get_client()
    db.table("tasks").delete().eq("id", task_id).execute()
    return {"ok": True}
