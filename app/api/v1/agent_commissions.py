"""Sales-agent commission payouts.

Calculated from ACTUAL provider payments (actual_commissions) using each
agent's custom plan (sales_agents.commission_rules) — see
app/services/agent_commission_engine.py. Workflow per agent per month:
calculated → approved → closed_out → paid, with an action log.
"""
from datetime import datetime, timezone, date
from typing import Optional

from fastapi import APIRouter, Depends, Body, HTTPException, Query

from app.db.client import get_client
from app.auth.deps import require_admin, UserContext
from app.services.agent_commission_engine import calculate_month, save_month_results, norm_name

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
    return (
        q.order("year",  desc=True)
         .order("month", desc=True)
         .order("agent_name")
         .execute()
         .data or []
    )


# ── Calculate / Recalculate (from provider-paid dollars) ─────────────────────

@router.post("/calculate")
def calculate_commissions(
    data: dict = Body(...),
    user: UserContext = Depends(require_admin),
):
    month = int(data.get("month") or datetime.now(timezone.utc).month)
    year  = int(data.get("year")  or datetime.now(timezone.utc).year)
    db    = get_client()

    result = calculate_month(db, year, month)
    if result["rows"] == 0 and not result["agents"]:
        raise HTTPException(
            status_code=400,
            detail=f"No provider payments imported for {year}-{month:02d}. "
                   f"Upload the commission statements first — agents are paid from received dollars.")

    saved, locked = save_month_results(db, year, month, result, performed_by=user.name or user.email)
    return {
        "ok": True,
        "month": month, "year": year,
        "calculated": len(saved),
        "locked": locked,  # already approved/paid — untouched
        "agents": saved,
        "unassigned": result["unassigned"],
        "warnings": result["warnings"],
        "statement_rows": result["rows"],
        "gross_total": result["gross_total"],
    }


# ── Shared transition helper ──────────────────────────────────────────────────

def _transition(commission_id: str, action: str, user: UserContext, notes: Optional[str]):
    db  = get_client()
    row = db.table("agent_commissions").select("*").eq("id", commission_id).limit(1).execute().data
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

    db.table("agent_commissions").update(payload).eq("id", commission_id).execute()

    month_str = date(rec["year"], rec["month"], 1).strftime("%B %Y")
    db.table("commission_logs").insert({
        "commission_id": commission_id,
        "action":        action,
        "performed_by":  user.name or user.email,
        "agent_name":    rec["agent_name"],
        "month":         rec["month"],
        "year":          rec["year"],
        "notes":         notes or f"Status → {new_status} | {rec['agent_name']} — {month_str}",
        "created_at":    now,
    }).execute()

    return {"ok": True, "new_status": new_status}


@router.patch("/{id}/approve")
def approve(id: str, data: dict = Body(default={}), user: UserContext = Depends(require_admin)):
    return _transition(id, "approve", user, data.get("notes"))


@router.patch("/{id}/close-out")
def close_out(id: str, data: dict = Body(default={}), user: UserContext = Depends(require_admin)):
    return _transition(id, "close_out", user, data.get("notes"))


@router.patch("/{id}/mark-paid")
def mark_paid(id: str, data: dict = Body(default={}), user: UserContext = Depends(require_admin)):
    return _transition(id, "mark_paid", user, data.get("notes"))


# ── Deal Breakdown (recomputed live from actual payments) ─────────────────────

@router.post("/held/{source}/{deal_id}/{decision}")
def decide_held_enrollment(source: str, deal_id: str, decision: str, data: dict = Body(default={}),
                           user: UserContext = Depends(require_admin)):
    """Admin decision on an enrollment bonus HELD for a duplicate service
    address: 'release' pays it on the next calculation, 'reject' keeps it at
    $0 for good. Recorded in audit_log (the engine reads the latest decision)."""
    from app.services.audit import audit
    from app.services.agent_commission_engine import HOLD_RELEASE, HOLD_REJECT
    if source not in ("crm_deals", "lead_deals") or decision not in ("release", "reject"):
        raise HTTPException(status_code=400, detail="source must be crm_deals|lead_deals and decision release|reject")
    db = get_client()
    exists = db.table(source).select("id").eq("id", deal_id).limit(1).execute().data
    if not exists:
        raise HTTPException(status_code=404, detail="Deal not found")
    audit(db, source, deal_id, HOLD_RELEASE if decision == "release" else HOLD_REJECT, None,
          {"decision": decision, "reason": (data.get("reason") or "")[:300], "month": data.get("month")},
          reason="Enrollment bonus duplicate-address review", actor=user.email or user.name or "admin")
    return {"ok": True, "deal_id": deal_id, "decision": decision,
            "next": "Recalculate the month to apply this decision."}


def _load_record(db, id: str) -> dict:
    row = db.table("agent_commissions").select("*").eq("id", id).limit(1).execute().data
    if not row:
        raise HTTPException(status_code=404, detail="Commission not found")
    return row[0]


@router.get("/{id}/breakdown")
def get_breakdown(id: str, user: UserContext = Depends(require_admin)):
    db  = get_client()
    rec = _load_record(db, id)
    result = calculate_month(db, rec["year"], rec["month"])
    match = next((v for k, v in result["agents"].items()
                  if norm_name(k) == norm_name(rec["agent_name"])), None)
    deals = match["deals"] if match else []
    summary = {k: v for k, v in (match or {}).items() if k != "deals"}
    return {"commission": rec, "summary": summary, "deals": deals,
            "warnings": result["warnings"]}


@router.get("/{id}/export")
def export_statement(id: str, user: UserContext = Depends(require_admin)):
    """Excel commission statement for one agent-month (to send to the agent)."""
    import io
    import pandas as pd
    from fastapi.responses import StreamingResponse

    db  = get_client()
    rec = _load_record(db, id)
    result = calculate_month(db, rec["year"], rec["month"])
    match = next((v for k, v in result["agents"].items()
                  if norm_name(k) == norm_name(rec["agent_name"])), None)
    deals = match["deals"] if match else []

    month_str = date(rec["year"], rec["month"], 1).strftime("%B %Y")
    summary = pd.DataFrame([{
        "Agent": rec["agent_name"], "Month": month_str,
        "Paid deals": (match or {}).get("deals_paid", 0),
        "Gross commission received": (match or {}).get("gross_received", 0),
        "Residuals": (match or {}).get("residual", 0),
        "New-deal bonuses": (match or {}).get("bonuses", 0),
        "Flat monthly": (match or {}).get("flat_monthly", 0),
        "Enrolled customers": (match or {}).get("enrolled", 0),
        "  of which brand-new": (match or {}).get("new_enrollments", 0),
        "  of which renewals": (match or {}).get("renewals", 0),
        "Enrollment bonuses": (match or {}).get("enrollment_bonuses", 0),
        "Held for review": (match or {}).get("held", 0),
        "TOTAL PAYOUT": (match or {}).get("total", rec.get("total_commission", 0)),
        "Status": rec.get("status"),
    }])
    # Enrollment bonuses (paid at contract start) first, then provider-paid
    # accounts; within each group the rows that pay the most come first.
    ordered = sorted(deals, key=lambda d: (0 if d.get("kind") == "enrollment" else 1, -float(d.get("commission") or 0)))
    detail = pd.DataFrame([{
        "Type": ("Enrollment — renewal" if d.get("enrollment_type") == "renewal" else "Enrollment — new customer")
                if d.get("kind") == "enrollment" else "Provider payment",
        "Customer": d["customer"], "ESI ID": d["esiid"], "Provider": d["supplier"],
        "Service address": d.get("address", ""),
        "Contract start": d.get("contract_start", ""),
        "Plan type": d["plan_type"], "kWh paid": d["kwh_paid"],
        "Gross received $": d["gross_received"],
        "New deal": "Yes" if d["first_payment"] else "",
        "Status": "HELD — needs review" if d.get("held") and d.get("hold_reason") != "rejected"
                  else ("Rejected (duplicate)" if d.get("hold_reason") == "rejected" else ""),
        "How calculated": d["applied"], "Commission $": d["commission"],
    } for d in ordered])

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        summary.to_excel(w, sheet_name="Summary", index=False)
        (detail if len(detail) else pd.DataFrame(columns=["Customer"])).to_excel(w, sheet_name="Deals", index=False)
    buf.seek(0)
    fname = f"commission_{rec['agent_name'].replace(' ', '_')}_{rec['year']}-{rec['month']:02d}.xlsx"
    return StreamingResponse(
        buf, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'})


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
