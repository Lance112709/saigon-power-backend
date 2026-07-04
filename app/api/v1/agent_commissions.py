"""Sales-agent commission payouts.

Calculated from ACTUAL provider payments (actual_commissions) using each
agent's custom plan (sales_agents.commission_rules) — see
app/services/agent_commission_engine.py. Workflow per agent per month:
calculated → approved → closed_out → paid, with an action log.
"""
import json
from datetime import datetime, timezone, date
from typing import Optional

from fastapi import APIRouter, Depends, Body, HTTPException, Query

from app.db.client import get_client
from app.auth.deps import require_admin, UserContext
from app.services.agent_commission_engine import calculate_month, norm_name

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
    now   = datetime.now(timezone.utc).isoformat()

    result = calculate_month(db, year, month)
    if result["rows"] == 0:
        raise HTTPException(
            status_code=400,
            detail=f"No provider payments imported for {year}-{month:02d}. "
                   f"Upload the commission statements first — agents are paid from received dollars.")

    saved, locked = [], []
    for agent, vals in result["agents"].items():
        existing = (
            db.table("agent_commissions")
            .select("id, status")
            .eq("agent_name", agent).eq("month", month).eq("year", year)
            .limit(1).execute().data
        )
        summary_note = json.dumps({
            "engine": "actuals-v1",
            "gross_received": vals["gross_received"],
            "residual": vals["residual"],
            "bonuses": vals["bonuses"],
            "flat_monthly": vals["flat_monthly"],
            "excluded_deals": vals["excluded_deals"],
        })

        if existing:
            rec = existing[0]
            if rec["status"] in ("approved", "closed_out", "paid"):
                locked.append(agent)
                continue
            db.table("agent_commissions").update({
                "total_deals":      vals["deals_paid"],
                "total_commission": vals["total"],
                "status":           "calculated",
                "notes":            summary_note,
                "updated_at":       now,
            }).eq("id", rec["id"]).execute()
            rec_id = rec["id"]
        else:
            ins = db.table("agent_commissions").insert({
                "agent_name":       agent,
                "month":            month,
                "year":             year,
                "total_deals":      vals["deals_paid"],
                "total_commission": vals["total"],
                "status":           "calculated",
                "notes":            summary_note,
                "created_at":       now,
                "updated_at":       now,
            }).execute().data
            rec_id = ins[0]["id"] if ins else None

        db.table("commission_logs").insert({
            "commission_id": rec_id,
            "action":        "recalculated",
            "performed_by":  user.name or user.email,
            "agent_name":    agent,
            "month":         month,
            "year":          year,
            "notes":         f"{vals['deals_paid']} paid deals · gross ${vals['gross_received']} · payout ${vals['total']}",
            "created_at":    now,
        }).execute()
        saved.append({"agent_name": agent, "total_commission": vals["total"],
                      "deals_paid": vals["deals_paid"], "gross_received": vals["gross_received"]})

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
        "TOTAL PAYOUT": (match or {}).get("total", rec.get("total_commission", 0)),
        "Status": rec.get("status"),
    }])
    detail = pd.DataFrame([{
        "Customer": d["customer"], "ESI ID": d["esiid"], "Provider": d["supplier"],
        "Plan type": d["plan_type"], "kWh paid": d["kwh_paid"],
        "Gross received $": d["gross_received"],
        "New deal": "Yes" if d["first_payment"] else "",
        "How calculated": d["applied"], "Commission $": d["commission"],
    } for d in deals])

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
