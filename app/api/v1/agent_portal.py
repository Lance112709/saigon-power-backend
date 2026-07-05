"""Agent Portal — self-service view for sales agents.

Every number an agent sees here is scoped to their own book and computed from
the same engines the admin uses: commissions from provider-paid dollars,
alerts from reconciliation runs. Admins/managers can preview any agent's
portal with ?agent=<name>.
"""
import json
from datetime import date, datetime, timezone, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.db.client import get_client
from app.auth.deps import get_current_user, UserContext
from app.services.agent_commission_engine import (
    norm_name, load_agent_plans, load_deal_book, calculate_month, plan_components,
)
from app.services.reconciliation_v2 import fetch_all

router = APIRouter()


def _resolve_agent(user: UserContext, agent_param: Optional[str]) -> str:
    if user.is_sales_agent:
        name = (user.sales_agent_name or "").strip()
        if not name:
            raise HTTPException(status_code=400,
                                detail="Your login is not linked to a sales agent yet — ask the admin to set it.")
        return name
    if agent_param:
        return agent_param
    raise HTTPException(status_code=400, detail="Pass ?agent=<name> to preview an agent's portal.")


def _my_deals(db, agent_name: str) -> list:
    """The agent's deals across both deal tables."""
    me = norm_name(agent_name)
    book = load_deal_book(db)  # esiid-keyed deals
    mine = [d | {"esiid": es} for es, d in book.items() if norm_name(d["agent"]) == me]

    # deals with no ESIID never appear in the esiid book — fetch them too
    for d in fetch_all(db, "lead_deals",
                       "id,status,supplier,esiid,adder,rate_type,plan_name,sales_agent,end_date,start_date,"
                       "leads(first_name,last_name)"):
        if norm_name(d.get("sales_agent")) == me and not (d.get("esiid") or "").strip():
            lead = d.get("leads") or {}
            mine.append({"source": "lead_deals", "id": d["id"], "esiid": "",
                         "active": d.get("status") == "Active", "agent": agent_name,
                         "supplier": d.get("supplier") or "", "plan_type": d.get("rate_type") or d.get("plan_name") or "",
                         "adder": float(d["adder"]) if d.get("adder") is not None else None,
                         "customer": f"{lead.get('first_name','')} {lead.get('last_name','')}".strip(),
                         "start": d.get("start_date"), "end": d.get("end_date")})
    for d in fetch_all(db, "crm_deals",
                       "id,deal_status,provider,esiid,adder,product_type,sales_agent,contract_start_date,"
                       "contract_end_date,business_name,crm_customers(full_name)"):
        if norm_name(d.get("sales_agent")) == me and not (d.get("esiid") or "").strip():
            cust = d.get("crm_customers") or {}
            mine.append({"source": "crm_deals", "id": d["id"], "esiid": "",
                         "active": d.get("deal_status") == "ACTIVE", "agent": agent_name,
                         "supplier": d.get("provider") or "", "plan_type": d.get("product_type") or "",
                         "adder": float(d["adder"]) if d.get("adder") is not None else None,
                         "customer": cust.get("full_name") or d.get("business_name") or "",
                         "start": d.get("contract_start_date"), "end": d.get("contract_end_date")})
    return mine


def _recent_paid_esiids(db, months_back: int = 2) -> dict:
    """{label: set(esiids)} for the most recent statement months in the system."""
    labels = sorted({r["billing_month"][:7] for r in
                     (db.table("reconciliation_runs").select("billing_month")
                      .like("notes", '%"engine": "v2"%').order("billing_month", desc=True)
                      .limit(50).execute().data or [])}, reverse=True)[:months_back]
    out = {}
    for lb in labels:
        rows = fetch_all(db, "actual_commissions", "raw_esiid",
                         filters=[("eq", ("billing_month", f"{lb}-01"))])
        out[lb] = {r["raw_esiid"] for r in rows}
    return out


@router.get("/overview")
def overview(agent: Optional[str] = Query(None), user: UserContext = Depends(get_current_user)):
    db = get_client()
    name = _resolve_agent(user, agent)
    deals = _my_deals(db, name)
    active = [d for d in deals if d["active"]]

    from app.utils.deals import is_month_to_month
    today = date.today()
    soon = (today + timedelta(days=60)).isoformat()
    renewals_60d = sum(1 for d in active
                       if d.get("end") and today.isoformat() <= str(d["end"])[:10] <= soon
                       and not is_month_to_month(d.get("plan_type")))

    paid_recent = _recent_paid_esiids(db)
    latest_label = next(iter(paid_recent), None)
    my_esiids = {d["esiid"] for d in active if d["esiid"]}
    paid_last_month = len(my_esiids & paid_recent.get(latest_label, set())) if latest_label else 0

    comms = db.table("agent_commissions").select("*").ilike("agent_name", name) \
        .order("year", desc=True).order("month", desc=True).limit(1).execute().data
    last_comm = comms[0] if comms else None

    plans = load_agent_plans(db)
    my_plan = plans.get(norm_name(name))
    components = my_plan["components"] if my_plan else []

    return {
        "agent": name,
        "active_deals": len(active),
        "deals_missing_esiid": sum(1 for d in active if not d["esiid"]),
        "paid_last_month": paid_last_month,
        "latest_statement_month": latest_label,
        "renewals_60d": renewals_60d,
        "last_commission": last_comm,
        "plan_components": components,
        "has_plan": bool(components),
    }


@router.get("/book")
def book(agent: Optional[str] = Query(None), user: UserContext = Depends(get_current_user)):
    db = get_client()
    name = _resolve_agent(user, agent)
    deals = _my_deals(db, name)
    paid_recent = _recent_paid_esiids(db)
    labels = list(paid_recent.keys())

    out = []
    for d in sorted(deals, key=lambda x: (not x["active"], x["customer"] or "")):
        paid = {lb: (d["esiid"] in paid_recent[lb]) for lb in labels} if d["esiid"] else {}
        out.append({
            "customer": d["customer"], "esiid": d["esiid"], "provider": d["supplier"],
            "plan_type": d["plan_type"], "active": d["active"],
            "provider_status": d.get("provider_status"),
            "start": d.get("start"), "end": d.get("end"),
            "paid_by_month": paid,
            "paid_latest": bool(labels) and paid.get(labels[0], False),
        })
    return {"agent": name, "months_checked": labels, "deals": out}


@router.get("/commissions")
def commissions(agent: Optional[str] = Query(None), user: UserContext = Depends(get_current_user)):
    db = get_client()
    name = _resolve_agent(user, agent)
    rows = db.table("agent_commissions").select("*").ilike("agent_name", name) \
        .order("year", desc=True).order("month", desc=True).limit(24).execute().data or []
    for r in rows:
        try:
            r["summary"] = json.loads(r.get("notes") or "{}")
        except Exception:
            r["summary"] = {}
    return {"agent": name, "commissions": rows}


@router.get("/commissions/{id}/breakdown")
def my_breakdown(id: str, agent: Optional[str] = Query(None), user: UserContext = Depends(get_current_user)):
    db = get_client()
    name = _resolve_agent(user, agent)
    rec = db.table("agent_commissions").select("*").eq("id", id).limit(1).execute().data
    if not rec:
        raise HTTPException(status_code=404, detail="Not found")
    rec = rec[0]
    if norm_name(rec["agent_name"]) != norm_name(name):
        raise HTTPException(status_code=403, detail="Not your commission record.")
    result = calculate_month(db, rec["year"], rec["month"])
    match = next((v for k, v in result["agents"].items()
                  if norm_name(k) == norm_name(rec["agent_name"])), None)
    return {"commission": rec,
            "summary": {k: v for k, v in (match or {}).items() if k != "deals"},
            "deals": (match or {}).get("deals", [])}


@router.get("/alerts")
def alerts(agent: Optional[str] = Query(None), user: UserContext = Depends(get_current_user)):
    """Unresolved reconciliation issues on MY accounts (latest run per provider)."""
    db = get_client()
    name = _resolve_agent(user, agent)
    deals = _my_deals(db, name)
    my_esiids = [d["esiid"] for d in deals if d["esiid"]]
    if not my_esiids:
        return {"agent": name, "alerts": []}

    runs = db.table("reconciliation_runs").select("id,billing_month,supplier_id") \
        .like("notes", '%"engine": "v2"%').order("billing_month", desc=True).limit(200).execute().data or []
    latest_run_ids = []
    seen_sup = set()
    for r in runs:
        if r["supplier_id"] not in seen_sup:
            seen_sup.add(r["supplier_id"])
            latest_run_ids.append(r["id"])

    alerts = []
    for run_id in latest_run_ids:
        for i in range(0, len(my_esiids), 100):
            items = db.table("reconciliation_items").select(
                "esiid,billing_month,status,severity,resolution_notes,expected_amount,actual_amount,"
                "suppliers(name)") \
                .eq("reconciliation_run_id", run_id).eq("is_resolved", False) \
                .in_("status", ["missing", "unexpected", "short_paid"]) \
                .in_("esiid", my_esiids[i:i + 100]).limit(1000).execute().data or []
            for it in items:
                note = (it.get("resolution_notes") or "").replace("ROOT CAUSE: ", "")
                alerts.append({
                    "esiid": it["esiid"], "month": it["billing_month"][:7],
                    "provider": (it.get("suppliers") or {}).get("name", ""),
                    "type": it["status"], "severity": it.get("severity"),
                    "explanation": note,
                })
    sev_rank = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    alerts.sort(key=lambda a: sev_rank.get(a["severity"], 3))
    return {"agent": name, "alerts": alerts[:200]}
