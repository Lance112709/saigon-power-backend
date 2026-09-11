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
from app.services.agent_commission_engine import calculate_month, save_month_results, enrollment_math, norm_name

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

    only_agent = (data.get("agent") or "").strip() or None  # history backfill for one agent
    result = calculate_month(db, year, month)
    if result["rows"] == 0 and not result["agents"]:
        raise HTTPException(
            status_code=400,
            detail=f"No provider payments imported for {year}-{month:02d}. "
                   f"Upload the commission statements first — agents are paid from received dollars.")

    saved, locked = save_month_results(db, year, month, result, performed_by=user.name or user.email,
                                       only_agent=only_agent)
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


@router.patch("/{id}/record-payment")
def record_payment(id: str, data: dict = Body(default={}), user: UserContext = Depends(require_admin)):
    """Record a payment that already happened (history backfill / paid outside
    the approve → close out → pay flow). Jumps the record straight to 'paid'
    with the real payment date; the skipped steps are stamped now.
    Body: {paid_at: YYYY-MM-DD, notes: str}."""
    import re as _re
    db  = get_client()
    rec = _load_record(db, id)
    if rec["status"] == "paid":
        raise HTTPException(status_code=400, detail="Already marked paid")
    paid_at = str(data.get("paid_at") or "").strip()
    if not _re.fullmatch(r"20\d{2}-\d{2}-\d{2}", paid_at):
        raise HTTPException(status_code=400, detail="paid_at must be YYYY-MM-DD")
    who = user.name or user.email
    now = datetime.now(timezone.utc).isoformat()
    payload = {"status": "paid", "paid_at": f"{paid_at}T12:00:00+00:00", "paid_by": who, "updated_at": now}
    if not rec.get("approved_at"):
        payload.update({"approved_at": now, "approved_by": who})
    if not rec.get("closed_out_at"):
        payload.update({"closed_out_at": now, "closed_out_by": who})
    db.table("agent_commissions").update(payload).eq("id", id).execute()
    month_str = date(rec["year"], rec["month"], 1).strftime("%B %Y")
    db.table("commission_logs").insert({
        "commission_id": id, "action": "mark_paid", "performed_by": who,
        "agent_name": rec["agent_name"], "month": rec["month"], "year": rec["year"],
        "notes": f"Payment recorded (paid {paid_at}) | {rec['agent_name']} — {month_str}"
                 + (f" | {data.get('notes')}" if data.get("notes") else ""),
        "created_at": now,
    }).execute()
    return {"ok": True, "new_status": "paid", "paid_at": paid_at}


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
    import pandas as pd

    db  = get_client()
    rec = _load_record(db, id)
    result = calculate_month(db, rec["year"], rec["month"])
    match = next((v for k, v in result["agents"].items()
                  if norm_name(k) == norm_name(rec["agent_name"])), None)
    deals = match["deals"] if match else []

    month_str = date(rec["year"], rec["month"], 1).strftime("%B %Y")
    m = match or {}
    if m.get("enrollment_only"):
        # paid per enrolled customer: the statement is just that list and the math
        summary = pd.DataFrame([{
            "Agent": rec["agent_name"], "Month": month_str,
            "Customers enrolled": m.get("enrolled", 0),
            "  of which brand-new": m.get("new_enrollments", 0),
            "  of which renewals": m.get("renewals", 0),
            "Held for review ($0)": m.get("held", 0),
            "Rate per enrolled customer": (m.get("enrollment_rate") or
                " · ".join(f"{r['segment']} ${r['amount']:g}" for r in m.get("enrollment_rates", []))),
            "Calculation": enrollment_math(m),
            "TOTAL PAYOUT": m.get("total", rec.get("total_commission", 0)),
            "Status": rec.get("status"),
        }])
        detail = pd.DataFrame([{
            "Customer": d["customer"], "ESI ID": d["esiid"], "Provider": d["supplier"],
            "Service address": d.get("address", ""), "Contract start": d.get("contract_start", ""),
            "Plan type": d["plan_type"],
            "Segment": (d.get("segment") or "residential").capitalize(),
            "Type": "Renewal" if d.get("enrollment_type") == "renewal" else "New customer",
            "Status": "HELD — needs review" if d.get("held") and d.get("hold_reason") != "rejected"
                      else ("Rejected (duplicate)" if d.get("hold_reason") == "rejected" else "Paid"),
            "Bonus $": d["commission"],
        } for d in sorted(deals, key=lambda d: (bool(d.get("held")), d.get("contract_start") or "", d["customer"]))])
        return _xlsx_response(rec, summary, detail, "Enrolled customers")

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
        "Provider statement rows": (match or {}).get("statement_rows", 0),
        "Provider statement share": (match or {}).get("statement_share", 0),
        "TOTAL PAYOUT": (match or {}).get("total", rec.get("total_commission", 0)),
        "Status": rec.get("status"),
    }])
    # Enrollment bonuses (paid at contract start) first, then provider-paid
    # accounts; within each group the rows that pay the most come first.
    ordered = sorted(deals, key=lambda d: (0 if d.get("kind") == "enrollment" else 1, -float(d.get("commission") or 0)))
    detail = pd.DataFrame([{
        "Type": ("Enrollment — renewal" if d.get("enrollment_type") == "renewal" else "Enrollment — new customer")
                if d.get("kind") == "enrollment" else ("Provider statement share" if d.get("kind") == "statement" else "Provider payment"),
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

    return _xlsx_response(rec, summary, detail, "Deals")


def _xlsx_response(rec: dict, summary, detail, detail_sheet: str):
    import io
    import pandas as pd
    from fastapi.responses import StreamingResponse
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        summary.to_excel(w, sheet_name="Summary", index=False)
        (detail if len(detail) else pd.DataFrame(columns=["Customer"])).to_excel(w, sheet_name=detail_sheet, index=False)
    buf.seek(0)
    fname = f"commission_{rec['agent_name'].replace(' ', '_')}_{rec['year']}-{rec['month']:02d}.xlsx"
    return StreamingResponse(
        buf, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'})


# ── One-click month close ─────────────────────────────────────────────────────

@router.post("/close-month")
def close_month(data: dict = Body(...), user: UserContext = Depends(require_admin)):
    """Approve, close out and mark paid every record of a month in one step.
    Body: {year, month, paid_at?: YYYY-MM-DD (default today), notes?, ids?: [..]
    (subset), skip_held?: bool (default true — records whose summary still has
    HELD enrollment bonuses are left alone)}."""
    import re as _re
    year, month = int(data.get("year") or 0), int(data.get("month") or 0)
    if not (year and 1 <= month <= 12):
        raise HTTPException(status_code=400, detail="year and month required")
    paid_at = str(data.get("paid_at") or datetime.now(timezone.utc).date().isoformat()).strip()
    if not _re.fullmatch(r"20\d{2}-\d{2}-\d{2}", paid_at):
        raise HTTPException(status_code=400, detail="paid_at must be YYYY-MM-DD")
    only = set(data.get("ids") or [])
    skip_held = data.get("skip_held", True)
    notes = (data.get("notes") or "").strip()
    who = user.name or user.email
    now = datetime.now(timezone.utc).isoformat()
    db = get_client()
    rows = db.table("agent_commissions").select("*").eq("year", year).eq("month", month).execute().data or []
    paid, skipped = [], []
    for rec in rows:
        if only and rec["id"] not in only:
            continue
        if rec["status"] == "paid":
            continue
        held = 0
        try:
            held = int((json.loads(rec.get("notes") or "{}") or {}).get("held") or 0)
        except Exception:
            pass
        if skip_held and held:
            skipped.append({"agent_name": rec["agent_name"], "reason": f"{held} held enrollment bonus(es) still need a decision"})
            continue
        if float(rec.get("total_commission") or 0) <= 0:
            skipped.append({"agent_name": rec["agent_name"], "reason": "nothing to pay"})
            continue
        payload = {"status": "paid", "paid_at": f"{paid_at}T12:00:00+00:00", "paid_by": who, "updated_at": now}
        if not rec.get("approved_at"):
            payload.update({"approved_at": now, "approved_by": who})
        if not rec.get("closed_out_at"):
            payload.update({"closed_out_at": now, "closed_out_by": who})
        db.table("agent_commissions").update(payload).eq("id", rec["id"]).execute()
        db.table("commission_logs").insert({
            "commission_id": rec["id"], "action": "mark_paid", "performed_by": who,
            "agent_name": rec["agent_name"], "month": month, "year": year,
            "notes": f"Month close — paid {paid_at} ${float(rec.get('total_commission') or 0):,.2f}"
                     + (f" | {notes}" if notes else ""),
            "created_at": now,
        }).execute()
        paid.append({"id": rec["id"], "agent_name": rec["agent_name"], "total_commission": rec.get("total_commission")})
    return {"ok": True, "year": year, "month": month, "paid_at": paid_at,
            "paid": paid, "paid_total": round(sum(float(p["total_commission"] or 0) for p in paid), 2),
            "skipped": skipped}


# ── Pre-payout checklist (digest) ────────────────────────────────────────────

@router.get("/digest")
def get_digest(year: int = Query(...), month: int = Query(...), user: UserContext = Depends(require_admin)):
    from app.services.commission_digest import build_digest
    return build_digest(year, month)


@router.post("/digest/send")
def send_digest_now(data: dict = Body(default={}), user: UserContext = Depends(require_admin)):
    from app.services.commission_digest import send_digest
    return send_digest(int(data.get("year") or 0) or None, int(data.get("month") or 0) or None)


# ── Agent name hygiene ───────────────────────────────────────────────────────

@router.get("/agent-names")
def agent_names_report(month: Optional[str] = Query(None), user: UserContext = Depends(require_admin)):
    from app.services.agent_names import agent_name_report
    return agent_name_report(get_client(), month_label=month)


@router.post("/agent-names/normalize")
def agent_names_normalize(data: dict = Body(default={}), user: UserContext = Depends(require_admin)):
    """Rewrite deal agent names that differ from a registered agent only by
    case or spacing. dry_run (default true) just reports what would change."""
    from app.services.agent_names import normalize_deal_agent_names
    from app.services.audit import audit
    db = get_client()
    try:
        res = normalize_deal_agent_names(db, dry_run=bool(data.get("dry_run", True)), renames=data.get("renames") or None)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not res["dry_run"] and res["changed"]:
        audit(db, "crm_deals", "agent-names", "agent_names_normalized", None,
              {"changed": res["changed"]}, reason="Agent name hygiene", actor=user.email or user.name or "admin")
    return res


# ── Paid-to-agents summary (month range / YTD) ───────────────────────────────

@router.get("/paid-summary")
def paid_summary(
    frm: Optional[str] = Query(None, alias="from"),  # YYYY-MM (commission period)
    to:  Optional[str] = Query(None),                # YYYY-MM inclusive
    user: UserContext = Depends(require_admin),
):
    """How much has gone to agents: totals by month and by agent over a range
    of commission periods. 'paid' = records marked paid (with the payment
    date); 'owed' = approved / closed out but not yet paid; 'pending' = only
    calculated so far. Omit both bounds for all time."""
    import re as _re
    from app.services.reconciliation_v2 import fetch_all

    def key(y, m): return y * 100 + m
    lo = hi = None
    for label, val in (("from", frm), ("to", to)):
        if val:
            if not _re.fullmatch(r"20\d{2}-\d{2}", val):
                raise HTTPException(status_code=400, detail=f"{label} must be YYYY-MM")
            y, m = int(val[:4]), int(val[5:7])
            if label == "from": lo = key(y, m)
            else: hi = key(y, m)

    db = get_client()
    rows = fetch_all(db, "agent_commissions",
                     "id,agent_name,year,month,status,total_commission,total_deals,paid_at,paid_by,approved_at,closed_out_at")
    rows = [r for r in rows if (lo is None or key(r["year"], r["month"]) >= lo)
                           and (hi is None or key(r["year"], r["month"]) <= hi)]

    def bucket(status):
        return "paid" if status == "paid" else ("owed" if status in ("approved", "closed_out") else "pending")

    by_month, by_agent = {}, {}
    totals = {"paid": 0.0, "owed": 0.0, "pending": 0.0}
    for r in rows:
        amt = float(r.get("total_commission") or 0)
        b = bucket(r.get("status"))
        totals[b] += amt
        m = by_month.setdefault((r["year"], r["month"]), {"year": r["year"], "month": r["month"],
                                                          "paid": 0.0, "owed": 0.0, "pending": 0.0, "agents": set()})
        m[b] += amt; m["agents"].add(r["agent_name"])
        a = by_agent.setdefault(norm_name(r["agent_name"]), {"agent_name": r["agent_name"], "paid": 0.0, "owed": 0.0,
                                                             "pending": 0.0, "months_paid": 0, "last_paid_at": None})
        a[b] += amt
        if b == "paid":
            a["months_paid"] += 1
            if r.get("paid_at") and (a["last_paid_at"] is None or r["paid_at"] > a["last_paid_at"]):
                a["last_paid_at"] = r["paid_at"]

    months = sorted(by_month.values(), key=lambda m: (m["year"], m["month"]))
    for m in months:
        m["agents"] = len(m["agents"])
        for k in ("paid", "owed", "pending"): m[k] = round(m[k], 2)
    agents = sorted(by_agent.values(), key=lambda a: (-a["paid"], -a["owed"], a["agent_name"]))
    for a in agents:
        for k in ("paid", "owed", "pending"): a[k] = round(a[k], 2)
    detail = sorted(({"id": r["id"], "agent_name": r["agent_name"], "year": r["year"], "month": r["month"],
                      "status": r["status"], "total_commission": round(float(r.get("total_commission") or 0), 2),
                      "paid_at": r.get("paid_at"), "paid_by": r.get("paid_by")} for r in rows),
                    key=lambda r: (-r["year"], -r["month"], r["agent_name"]))
    return {"from": frm, "to": to, "records": len(rows),
            "totals": {k: round(v, 2) for k, v in totals.items()},
            "by_month": months, "by_agent": agents, "detail": detail}


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
