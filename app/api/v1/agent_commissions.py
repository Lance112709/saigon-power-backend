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


def _apply_rules(rules: dict, deal: dict, kwh: float) -> float:
    """Apply an agent's commission_rules to a deal and return monthly commission $."""
    # Check excluded plan types (rate_type or plan_name field)
    plan_type     = (deal.get("rate_type") or deal.get("plan_name") or "").strip()
    exclude_types = rules.get("exclude_plan_types") or []
    if plan_type and any(pt.lower() == plan_type.lower() for pt in exclude_types):
        return 0.0

    # Supplier override?
    supplier  = (deal.get("supplier") or "").strip().lower()
    overrides = rules.get("overrides") or []
    override  = next((o for o in overrides if (o.get("supplier") or "").lower() == supplier), None)

    if override:
        rate      = float(override.get("rate") or 0)
        comm_type = override.get("type") or "per_kwh"
    else:
        rate      = float(rules.get("default_rate") or 0)
        comm_type = rules.get("default_type") or "per_kwh"

    # No commission_rules set → fall back to deal's adder
    if not rules or (not rate and not overrides):
        return kwh * float(deal.get("adder") or 0)

    if comm_type == "per_kwh":
        return kwh * rate
    elif comm_type == "flat_monthly":
        return rate
    elif comm_type == "flat_per_deal":
        return rate
    elif comm_type == "percentage":
        return kwh * float(deal.get("adder") or 0) * (rate / 100)
    return kwh * rate


def _agent_rules_map(db) -> dict:
    """Return {agent_name_lower: commission_rules} for all agents."""
    rows = db.table("sales_agents").select("name, commission_rules").execute().data or []
    return {
        (r.get("name") or "").strip().lower(): r.get("commission_rules") or {}
        for r in rows
    }


def _calc_by_agent(db, month: int, year: int) -> dict:
    """Return {agent_name: {deals, commission}} for a given month using commission_rules."""
    first_day  = date(year, month, 1).isoformat()
    last_day   = date(year, month, calendar.monthrange(year, month)[1]).isoformat()
    rules_map  = _agent_rules_map(db)

    rows = (
        db.table("lead_deals")
        .select("id, sales_agent, est_kwh, adder, rate_type, plan_name, supplier, start_date, end_date")
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
        kwh   = float(r.get("est_kwh") or 0)
        rules = rules_map.get(agent.lower(), {})
        comm  = _apply_rules(rules, r, kwh)

        if agent not in by_agent:
            by_agent[agent] = {"deals": 0, "commission": 0.0}
        by_agent[agent]["deals"]      += 1
        by_agent[agent]["commission"] += comm

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

    # Load this agent's commission_rules
    agent_row = db.table("sales_agents").select("commission_rules").ilike("name", f"%{agent}%").limit(1).execute().data
    rules = (agent_row[0].get("commission_rules") or {}) if agent_row else {}

    rows = (
        db.table("lead_deals")
        .select("id, sales_agent, est_kwh, adder, rate_type, plan_name, supplier, start_date, end_date, lead_id, leads(first_name, last_name, phone)")
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

        # Determine applied rate/type
        plan_type  = (r.get("rate_type") or r.get("plan_name") or "").strip()
        supplier_s = (r.get("supplier") or "").strip().lower()
        overrides  = rules.get("overrides") or []
        override   = next((o for o in overrides if (o.get("supplier") or "").lower() == supplier_s), None)
        excluded   = bool(plan_type and any(pt.lower() == plan_type.lower() for pt in (rules.get("exclude_plan_types") or [])))

        if excluded:
            applied_rate = 0.0
            applied_type = "excluded"
        elif override:
            applied_rate = float(override.get("rate") or 0)
            applied_type = override.get("type") or "per_kwh"
        elif rules.get("default_rate"):
            applied_rate = float(rules.get("default_rate") or 0)
            applied_type = rules.get("default_type") or "per_kwh"
        else:
            applied_rate = float(r.get("adder") or 0)
            applied_type = "per_kwh"

        comm = _apply_rules(rules, r, kwh)

        deals.append({
            "deal_id":       r["id"],
            "lead_id":       r.get("lead_id"),
            "customer_name": f"{lead.get('first_name','')} {lead.get('last_name','')}".strip() or "—",
            "phone":         lead.get("phone"),
            "supplier":      r.get("supplier") or "—",
            "plan_name":     plan_type or "—",
            "est_kwh":       kwh,
            "adder":         float(r.get("adder") or 0),
            "applied_rate":  applied_rate,
            "applied_type":  applied_type,
            "commission":    round(comm, 4),
            "start_date":    r.get("start_date"),
            "end_date":      r.get("end_date"),
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
