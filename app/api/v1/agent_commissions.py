from fastapi import APIRouter, Depends, Body, HTTPException, Query
from typing import Optional
from datetime import datetime, timezone, date
import calendar
from app.db.client import get_client
from app.auth.deps import require_admin, UserContext

router = APIRouter()

VALID_TRANSITIONS = {
    "calculated": "approved",
    "approved":   "closed_out",
    "closed_out": "paid",
}

ACTION_META = {
    "approve":   ("calculated",  "approved_at",    "approved_by"),
    "close_out": ("approved",    "closed_out_at",  "closed_out_by"),
    "mark_paid": ("closed_out",  "paid_at",        "paid_by"),
}


def _calc_by_agent(db, month: int, year: int) -> dict:
    """Return {agent_name: {deals, commission}} for a given month."""
    first_day = date(year, month, 1).isoformat()
    last_day  = date(year, month, calendar.monthrange(year, month)[1]).isoformat()

    rows = (
        db.table("lead_deals")
        .select("id, sales_agent, est_kwh, adder, start_date, end_date")
        .eq("status", "Active")
        .lte("start_date", last_day)
        .execute()
        .data or []
    )

    by_agent: dict = {}
    for r in rows:
        end_d = r.get("end_date") or "9999-12-31"
        if end_d < first_day:
            continue
        agent = (r.get("sales_agent") or "").strip()
        if not agent:
            continue
        kwh   = float(r.get("est_kwh")  or 0)
        adder = float(r.get("adder")    or 0)
        if agent not in by_agent:
            by_agent[agent] = {"deals": 0, "commission": 0.0}
        by_agent[agent]["deals"]      += 1
        by_agent[agent]["commission"] += kwh * adder

    return by_agent


# ── List ──────────────────────────────────────────────────────────────────────

@router.get("")
def list_commissions(
    month:  Optional[int] = Query(None),
    year:   Optional[int] = Query(None),
    status: Optional[str] = Query(None),
    agent:  Optional[str] = Query(None),
    user: UserContext = Depends(require_admin),
):
    db = get_client()
    q = db.table("agent_commissions").select("*")
    if month:  q = q.eq("month", month)
    if year:   q = q.eq("year",  year)
    if status: q = q.eq("status", status)
    if agent:  q = q.ilike("agent_name", f"%{agent}%")
    rows = (
        q.order("year",  desc=True)
         .order("month", desc=True)
         .order("agent_name")
         .execute()
         .data or []
    )
    return rows


# ── Calculate / Recalculate ───────────────────────────────────────────────────

@router.post("/calculate")
def calculate_commissions(
    data: dict = Body(...),
    user: UserContext = Depends(require_admin),
):
    month = int(data.get("month") or datetime.now(timezone.utc).month)
    year  = int(data.get("year")  or datetime.now(timezone.utc).year)
    db    = get_client()
    now   = datetime.now(timezone.utc).isoformat()

    by_agent = _calc_by_agent(db, month, year)
    results  = []

    for agent, vals in by_agent.items():
        total_comm = round(vals["commission"], 4)
        existing = (
            db.table("agent_commissions")
            .select("id, status")
            .eq("agent_name", agent)
            .eq("month", month)
            .eq("year",  year)
            .limit(1)
            .execute()
            .data
        )

        if existing:
            rec    = existing[0]
            rec_id = rec["id"]
            if rec["status"] in ("approved", "closed_out", "paid"):
                results.append(rec)
                continue
            db.table("agent_commissions").update({
                "total_deals":      vals["deals"],
                "total_commission": total_comm,
                "status":           "calculated",
                "updated_at":       now,
            }).eq("id", rec_id).execute()
        else:
            ins = db.table("agent_commissions").insert({
                "agent_name":       agent,
                "month":            month,
                "year":             year,
                "total_deals":      vals["deals"],
                "total_commission": total_comm,
                "status":           "calculated",
                "created_at":       now,
                "updated_at":       now,
            }).execute()
            rec_id = ins.data[0]["id"] if ins.data else None

        # Audit log
        db.table("commission_logs").insert({
            "commission_id": rec_id,
            "action":        "recalculated",
            "performed_by":  user.name or user.email,
            "agent_name":    agent,
            "month":         month,
            "year":          year,
            "notes":         f"{vals['deals']} deals · ${total_comm}",
            "created_at":    now,
        }).execute()

        results.append({"agent_name": agent, "total_commission": total_comm})

    return {"ok": True, "calculated": len(results), "month": month, "year": year}


# ── Shared transition helper ──────────────────────────────────────────────────

def _transition(commission_id: str, action: str, user: UserContext, notes: Optional[str]):
    db    = get_client()
    row   = db.table("agent_commissions").select("*").eq("id", commission_id).limit(1).execute().data
    if not row:
        raise HTTPException(status_code=404, detail="Commission not found")
    rec = row[0]

    required_status, ts_field, by_field = ACTION_META[action]
    if rec["status"] != required_status:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot {action}: status is '{rec['status']}', expected '{required_status}'"
        )

    now        = datetime.now(timezone.utc).isoformat()
    new_status = VALID_TRANSITIONS[required_status]
    payload    = {"status": new_status, ts_field: now, by_field: user.name or user.email, "updated_at": now}
    if notes:
        payload["notes"] = notes

    db.table("agent_commissions").update(payload).eq("id", commission_id).execute()

    month_str = date(rec["year"], rec["month"], 1).strftime("%B %Y")
    db.table("commission_logs").insert({
        "commission_id": commission_id,
        "action":        action,
        "performed_by":  user.name or user.email,
        "agent_name":    rec["agent_name"],
        "month":         rec["month"],
        "year":          rec["year"],
        "notes":         notes or f"Status → {new_status} | Admin: {user.name or user.email} | Agent: {rec['agent_name']} — {month_str}",
        "created_at":    now,
    }).execute()

    return {"ok": True, "new_status": new_status}


# ── Approve ───────────────────────────────────────────────────────────────────

@router.patch("/{id}/approve")
def approve(id: str, data: dict = Body(default={}), user: UserContext = Depends(require_admin)):
    return _transition(id, "approve", user, data.get("notes"))


# ── Close Out ─────────────────────────────────────────────────────────────────

@router.patch("/{id}/close-out")
def close_out(id: str, data: dict = Body(default={}), user: UserContext = Depends(require_admin)):
    return _transition(id, "close_out", user, data.get("notes"))


# ── Mark Paid ─────────────────────────────────────────────────────────────────

@router.patch("/{id}/mark-paid")
def mark_paid(id: str, data: dict = Body(default={}), user: UserContext = Depends(require_admin)):
    return _transition(id, "mark_paid", user, data.get("notes"))


# ── Deal Breakdown ────────────────────────────────────────────────────────────

@router.get("/{id}/breakdown")
def get_breakdown(id: str, user: UserContext = Depends(require_admin)):
    db  = get_client()
    row = db.table("agent_commissions").select("*").eq("id", id).limit(1).execute().data
    if not row:
        raise HTTPException(status_code=404, detail="Commission not found")
    rec   = row[0]
    month = rec["month"]
    year  = rec["year"]
    agent = rec["agent_name"]

    first_day = date(year, month, 1).isoformat()
    last_day  = date(year, month, calendar.monthrange(year, month)[1]).isoformat()

    rows = (
        db.table("lead_deals")
        .select("id, sales_agent, est_kwh, adder, start_date, end_date, plan_name, supplier, lead_id, leads(first_name, last_name, phone)")
        .eq("status", "Active")
        .ilike("sales_agent", f"%{agent}%")
        .lte("start_date", last_day)
        .execute()
        .data or []
    )

    deals = []
    for r in rows:
        end_d = r.get("end_date") or "9999-12-31"
        if end_d < first_day:
            continue
        lead  = r.pop("leads", None) or {}
        kwh   = float(r.get("est_kwh")  or 0)
        adder = float(r.get("adder")    or 0)
        deals.append({
            "deal_id":      r["id"],
            "lead_id":      r.get("lead_id"),
            "customer_name": f"{lead.get('first_name','')} {lead.get('last_name','')}".strip() or "—",
            "phone":        lead.get("phone"),
            "supplier":     r.get("supplier") or "—",
            "plan_name":    r.get("plan_name") or "—",
            "est_kwh":      kwh,
            "adder":        adder,
            "commission":   round(kwh * adder, 4),
            "start_date":   r.get("start_date"),
            "end_date":     r.get("end_date"),
        })

    deals.sort(key=lambda x: x["commission"], reverse=True)
    return {"commission": rec, "deals": deals}


# ── Logs ──────────────────────────────────────────────────────────────────────

@router.get("/logs")
def get_logs(
    commission_id: Optional[str] = Query(None),
    month:         Optional[int] = Query(None),
    year:          Optional[int] = Query(None),
    user: UserContext = Depends(require_admin),
):
    db = get_client()
    q  = db.table("commission_logs").select("*")
    if commission_id: q = q.eq("commission_id", commission_id)
    if month:         q = q.eq("month", month)
    if year:          q = q.eq("year",  year)
    return q.order("created_at", desc=True).limit(200).execute().data or []
