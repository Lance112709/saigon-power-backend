from fastapi import APIRouter, HTTPException, Depends, Query, Body
from typing import Optional
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import re
from app.auth.deps import require_admin, UserContext
from app.db.client import get_client
from app.services.ai_agent import (
    get_dashboard, run_full_scan, generate_daily_report,
    generate_monthly_report, _resolve_alert, chat_with_context
)


def _norm_esiid(s: str) -> str:
    """Normalize ESIID: handle scientific notation, strip non-digits and leading zeros."""
    if not s:
        return ""
    s = str(s).strip()
    try:
        if "e" in s.lower():
            s = str(int(float(s)))
    except Exception:
        pass
    return re.sub(r"\D", "", s).lstrip("0")


def _months_between(start_str: str, end_str: Optional[str], now: datetime) -> list:
    """Return list of YYYY-MM strings from start to min(end, today)."""
    try:
        start = datetime(int(start_str[:4]), int(start_str[5:7]), 1, tzinfo=timezone.utc)
    except Exception:
        return []
    if end_str:
        try:
            end = datetime(int(end_str[:4]), int(end_str[5:7]), 1, tzinfo=timezone.utc)
        except Exception:
            end = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        end = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    # Cap at current month
    cap = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = min(end, cap)
    months = []
    cur = start
    while cur <= end:
        months.append(cur.strftime("%Y-%m"))
        cur = (cur + timedelta(days=32)).replace(day=1)
    return months

router = APIRouter()


@router.get("/dashboard")
def ai_dashboard(user: UserContext = Depends(require_admin)):
    return get_dashboard()


@router.post("/scan")
def manual_scan(user: UserContext = Depends(require_admin)):
    return run_full_scan()


@router.get("/alerts")
def list_alerts(user: UserContext = Depends(require_admin)):
    db = get_client()
    res = db.table("ai_alerts").select("*").eq("status", "open").order("created_at", desc=True).limit(200).execute()
    return res.data or []


@router.patch("/alerts/{alert_id}/resolve")
def resolve_alert(alert_id: str, user: UserContext = Depends(require_admin)):
    db = get_client()
    res = db.table("ai_alerts").select("type, entity_id").eq("id", alert_id).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Alert not found")
    row = res.data[0]
    _resolve_alert(db, row["type"], row["entity_id"])
    return {"ok": True}


@router.post("/reports/daily")
def trigger_daily_report(user: UserContext = Depends(require_admin)):
    return generate_daily_report()


@router.post("/reports/monthly")
def trigger_monthly_report(user: UserContext = Depends(require_admin)):
    return generate_monthly_report()


@router.get("/reports")
def list_reports(user: UserContext = Depends(require_admin)):
    db = get_client()
    res = db.table("ai_reports").select("*").order("report_date", desc=True).limit(30).execute()
    return res.data or []


@router.get("/leaderboard")
def agent_leaderboard(user: UserContext = Depends(require_admin)):
    db = get_client()
    now = datetime.now(timezone.utc)
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()

    all_deals = db.table("lead_deals").select("id, lead_id, sales_agent, status, created_at, est_kwh, adder").execute().data or []
    proposals  = db.table("proposals").select("rep_name, status, created_at").execute().data or []

    agents: dict = {}

    def _agent(name: str):
        if name not in agents:
            agents[name] = {
                "name": name,
                "total_deals": 0, "active_deals": 0, "future_deals": 0,
                "deals_this_month": 0,
                "unique_leads": set(), "converted_leads": set(),
                "proposals_sent": 0, "proposals_accepted": 0,
                "est_monthly_commission": 0.0,
            }
        return agents[name]

    for d in all_deals:
        name = d.get("sales_agent") or "Unassigned"
        a = _agent(name)
        a["total_deals"] += 1
        if d.get("lead_id"):
            a["unique_leads"].add(d["lead_id"])
        status = d.get("status", "")
        if status == "Active":
            a["active_deals"] += 1
            if d.get("lead_id"):
                a["converted_leads"].add(d["lead_id"])
            a["est_monthly_commission"] += float(d.get("est_kwh") or 0) * float(d.get("adder") or 0)
        elif status == "Future":
            a["future_deals"] += 1
        if (d.get("created_at") or "") >= month_start:
            a["deals_this_month"] += 1

    for p in proposals:
        name = p.get("rep_name") or "Unassigned"
        a = _agent(name)
        if p.get("status") in ("sent", "viewed", "accepted", "rejected"):
            a["proposals_sent"] += 1
        if p.get("status") == "accepted":
            a["proposals_accepted"] += 1

    results = []
    for name, a in agents.items():
        total_leads   = len(a["unique_leads"])
        converted     = len(a["converted_leads"])
        conv_rate     = round(converted / total_leads * 100, 1) if total_leads > 0 else 0.0
        prop_rate     = round(a["proposals_accepted"] / a["proposals_sent"] * 100, 1) if a["proposals_sent"] > 0 else 0.0
        results.append({
            "name":                   name,
            "deals_this_month":       a["deals_this_month"],
            "active_deals":           a["active_deals"],
            "future_deals":           a["future_deals"],
            "total_deals":            a["total_deals"],
            "unique_leads_touched":   total_leads,
            "conversion_rate":        conv_rate,
            "proposals_sent":         a["proposals_sent"],
            "proposals_accepted":     a["proposals_accepted"],
            "proposal_close_rate":    prop_rate,
            "est_monthly_commission": round(a["est_monthly_commission"], 2),
        })

    results.sort(key=lambda x: (x["deals_this_month"], x["active_deals"]), reverse=True)
    return results


@router.get("/pipeline")
def pipeline_value(user: UserContext = Depends(require_admin)):
    db = get_client()
    now = datetime.now(timezone.utc)

    active = db.table("lead_deals").select("id, est_kwh, adder, end_date, start_date, supplier, sales_agent, status").eq("status", "Active").execute().data or []
    future = db.table("lead_deals").select("id, est_kwh, adder, start_date, supplier, sales_agent").eq("status", "Future").execute().data or []

    def _commission(d):
        return float(d.get("est_kwh") or 0) * float(d.get("adder") or 0)

    monthly_commission = sum(_commission(d) for d in active)
    pipeline_value_amt = sum(_commission(d) for d in future)

    # Revenue at risk — active deals expiring within 90 days
    at_risk = []
    for d in active:
        days = None
        if d.get("end_date"):
            try:
                end = datetime.strptime(d["end_date"][:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
                days = (end - now).days
            except Exception:
                pass
        if days is not None and days <= 90:
            at_risk.append({
                "supplier":              d.get("supplier") or "—",
                "sales_agent":           d.get("sales_agent") or "Unassigned",
                "days_until_expiry":     days,
                "end_date":              d.get("end_date", "")[:10],
                "est_monthly_commission": round(_commission(d), 2),
            })
    at_risk.sort(key=lambda x: x["days_until_expiry"])

    # Expiry schedule — commission expiring by month
    expiry_by_month: dict = defaultdict(float)
    for d in active:
        if d.get("end_date"):
            try:
                month = d["end_date"][:7]
                expiry_by_month[month] += _commission(d)
            except Exception:
                pass

    # Commission added by start month (trend)
    start_by_month: dict = defaultdict(lambda: {"deals": 0, "commission": 0.0})
    for d in active:
        key = (d.get("start_date") or "")[:7] or (d.get("created_at") or "")[:7]
        if key:
            start_by_month[key]["deals"] += 1
            start_by_month[key]["commission"] = round(start_by_month[key]["commission"] + _commission(d), 2)

    # Commission by agent
    by_agent: dict = defaultdict(lambda: {"active": 0, "future": 0, "commission": 0.0})
    for d in active:
        name = d.get("sales_agent") or "Unassigned"
        by_agent[name]["active"] += 1
        by_agent[name]["commission"] = round(by_agent[name]["commission"] + _commission(d), 2)
    for d in future:
        name = d.get("sales_agent") or "Unassigned"
        by_agent[name]["future"] += 1

    return {
        "summary": {
            "active_monthly_commission": round(monthly_commission, 2),
            "pipeline_value":            round(pipeline_value_amt, 2),
            "active_deals":              len(active),
            "future_deals":              len(future),
            "at_risk_count":             len(at_risk),
            "at_risk_commission":        round(sum(x["est_monthly_commission"] for x in at_risk), 2),
        },
        "at_risk":             at_risk[:30],
        "expiry_by_month":     dict(sorted(expiry_by_month.items())),
        "commission_by_start": {k: v for k, v in sorted(start_by_month.items())},
        "by_agent":            {k: v for k, v in sorted(by_agent.items(), key=lambda x: x[1]["commission"], reverse=True)},
    }


@router.get("/deals-by-agent")
def deals_by_agent(
    mode: str = Query("month", regex="^(day|month)$"),
    months_back: int = Query(6, ge=1, le=24),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    user: UserContext = Depends(require_admin),
):
    db = get_client()

    # Filter & group by contract signed date. On lead_deals the signed date is
    # stored in expected_close_date (the "Contract Signed Date" field in the deal form).
    if date_from:
        cutoff = date_from[:10]
    else:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=months_back * 30)).date().isoformat()

    query = (
        db.table("lead_deals")
        .select("sales_agent, status, expected_close_date")
        .in_("status", ["Active", "Inactive"])
        .gte("expected_close_date", cutoff)
    )

    if date_to:
        query = query.lte("expected_close_date", date_to[:10])

    deals = query.execute().data or []

    # Group by agent → period → count (period = month or day of the signed date)
    counts: dict = defaultdict(lambda: defaultdict(int))
    agents: set = set()

    for d in deals:
        signed = d.get("expected_close_date")
        if not signed:
            continue
        agent = d.get("sales_agent") or "Unassigned"
        agents.add(agent)
        try:
            dt = datetime.fromisoformat(str(signed)[:10])
            period = dt.strftime("%Y-%m-%d") if mode == "day" else dt.strftime("%Y-%m")
        except Exception:
            continue
        counts[period][agent] += 1

    # Build sorted period list
    periods = sorted(counts.keys())
    agents_sorted = sorted(agents)

    rows = []
    for period in periods:
        row = {"period": period}
        for agent in agents_sorted:
            row[agent] = counts[period].get(agent, 0)
        row["total"] = sum(counts[period].values())
        rows.append(row)

    # Per-agent totals
    agent_totals = {agent: sum(counts[p].get(agent, 0) for p in periods) for agent in agents_sorted}

    return {
        "mode": mode,
        "periods": periods,
        "agents": agents_sorted,
        "rows": rows,
        "agent_totals": agent_totals,
    }


@router.get("/deals-by-agent/details")
def deals_by_agent_details(
    agent: str = Query(...),
    months_back: int = Query(6, ge=1, le=24),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    user: UserContext = Depends(require_admin),
):
    """Individual closed deals for one agent within the same period as deals-by-agent."""
    db = get_client()

    # Filter by contract signed date (expected_close_date on lead_deals).
    if date_from:
        cutoff = date_from[:10]
    else:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=months_back * 30)).date().isoformat()

    query = (
        db.table("lead_deals")
        .select(
            "id, lead_id, esiid, service_address, service_city, service_state, service_zip, "
            "supplier, plan_name, status, rate, adder, contract_term, start_date, end_date, "
            "expected_close_date, created_at, "
            "leads(first_name, last_name, address, city, state, zip, phone, email)"
        )
        .in_("status", ["Active", "Inactive"])
        .gte("expected_close_date", cutoff)
    )
    # "Unassigned" bucket = deals with no sales_agent set
    if agent == "Unassigned":
        query = query.is_("sales_agent", "null")
    else:
        query = query.eq("sales_agent", agent)

    if date_to:
        query = query.lte("expected_close_date", date_to[:10])

    deals = query.order("expected_close_date", desc=True).execute().data or []

    result = []
    for d in deals:
        lead = d.get("leads") or {}
        name = f"{lead.get('first_name', '') or ''} {lead.get('last_name', '') or ''}".strip()
        address = d.get("service_address") or lead.get("address") or ""
        city = d.get("service_city") or lead.get("city") or ""
        state = d.get("service_state") or lead.get("state") or ""
        zip_code = d.get("service_zip") or lead.get("zip") or ""
        full_address = ", ".join([p for p in [address, city, state, zip_code] if p])
        result.append({
            "deal_id": d.get("id"),
            "lead_id": d.get("lead_id"),
            "customer_name": name or "—",
            "phone": lead.get("phone"),
            "email": lead.get("email"),
            "service_address": full_address or "—",
            "esiid": d.get("esiid"),
            "provider": d.get("supplier"),
            "plan_name": d.get("plan_name"),
            "status": d.get("status"),
            "rate": d.get("rate"),
            "adder": d.get("adder"),
            "contract_term": d.get("contract_term"),
            "start_date": d.get("start_date"),
            "end_date": d.get("end_date"),
            "signed_date": d.get("expected_close_date"),
            "created_at": d.get("created_at"),
        })

    return {"agent": agent, "count": len(result), "deals": result}


@router.get("/data-quality/dup-addresses")
def get_dup_addresses(user: UserContext = Depends(require_admin)):
    db = get_client()
    rows = (
        db.table("crm_deals")
        .select("id, customer_id, service_address, deal_status, provider, created_at, sales_agent, esiid, crm_customers(id, full_name)")
        .execute()
        .data or []
    )

    groups: dict = {}
    for r in rows:
        addr = (r.get("service_address") or "").strip().upper()
        cust = r.get("customer_id") or ""
        if not addr or not cust:
            continue
        key = (cust, addr)
        deal = {
            "id":            r.get("id"),
            "customer_id":   cust,
            "customer_name": (r.get("crm_customers") or {}).get("full_name") or "Unknown",
            "service_address": r.get("service_address") or addr,
            "deal_status":   r.get("deal_status"),
            "provider":      r.get("provider"),
            "sales_agent":   r.get("sales_agent"),
            "esiid":         r.get("esiid"),
            "created_at":    r.get("created_at"),
        }
        groups.setdefault(key, []).append(deal)

    duplicates = []
    for (cust, addr), deals in groups.items():
        if len(deals) > 1:
            duplicates.append({
                "customer_id":    cust,
                "customer_name":  deals[0]["customer_name"],
                "service_address": deals[0]["service_address"],
                "deal_count":     len(deals),
                "deals":          sorted(deals, key=lambda d: d.get("created_at") or "", reverse=True),
            })

    duplicates.sort(key=lambda x: x["deal_count"], reverse=True)
    return duplicates


@router.get("/data-quality/dup-esiids")
def get_dup_esiids(user: UserContext = Depends(require_admin)):
    db = get_client()
    rows = (
        db.table("crm_deals")
        .select("id, customer_id, esiid, service_address, deal_status, provider, created_at, sales_agent, crm_customers(id, full_name)")
        .execute()
        .data or []
    )

    groups: dict = {}
    for r in rows:
        esiid = (r.get("esiid") or "").strip()
        if not esiid:
            continue
        deal = {
            "id":            r.get("id"),
            "customer_id":   r.get("customer_id") or "",
            "customer_name": (r.get("crm_customers") or {}).get("full_name") or "Unknown",
            "service_address": r.get("service_address") or "",
            "deal_status":   r.get("deal_status"),
            "provider":      r.get("provider"),
            "sales_agent":   r.get("sales_agent"),
            "esiid":         esiid,
            "created_at":    r.get("created_at"),
        }
        groups.setdefault(esiid, []).append(deal)

    duplicates = []
    for esiid, deals in groups.items():
        if len(deals) > 1:
            duplicates.append({
                "esiid":       esiid,
                "deal_count":  len(deals),
                "deals":       sorted(deals, key=lambda d: d.get("created_at") or "", reverse=True),
            })

    duplicates.sort(key=lambda x: x["deal_count"], reverse=True)
    return duplicates


@router.get("/reconciliation-gap")
def reconciliation_gap(user: UserContext = Depends(require_admin)):
    db = get_client()
    now = datetime.now(timezone.utc)

    # All signed deals (Active or Inactive) that have an ESIID and start date
    deals = (
        db.table("lead_deals")
        .select("id, lead_id, supplier, esiid, start_date, end_date, status, rate, est_kwh, adder, sales_agent")
        .in_("status", ["Active", "Inactive"])
        .execute()
        .data or []
    )
    valid = [d for d in deals if d.get("esiid") and d.get("start_date")]

    if not valid:
        return {
            "summary": {
                "deals_analyzed": 0, "total_payments_expected": 0,
                "total_payments_received": 0, "total_payments_missing": 0,
                "pct_received": 0, "est_missing_commission": 0,
            },
            "deals": [],
        }

    # Batch-fetch all actual commissions for these ESIIDs (both raw and normalized)
    raw_esiids = list({d["esiid"] for d in valid})
    # Also include normalized forms to cast a wider net
    norm_esiids = list({_norm_esiid(e) for e in raw_esiids if _norm_esiid(e)})

    actuals_raw  = db.table("actual_commissions").select("raw_esiid, billing_month, raw_amount").in_("raw_esiid", raw_esiids).execute().data or []
    # Also try fetching by normalized esiid where raw may differ (scientific notation etc.)
    actuals_norm = db.table("actual_commissions").select("raw_esiid, billing_month, raw_amount").execute().data or []

    # Build paid map: norm_esiid → {billing_month: amount}
    paid: dict = defaultdict(dict)
    for a in actuals_norm:
        norm = _norm_esiid(a.get("raw_esiid") or "")
        if norm:
            month = (a.get("billing_month") or "")[:7]
            if month:
                paid[norm][month] = float(a.get("raw_amount") or 0)
    # Also index by raw esiid (no normalization) for exact matches
    paid_raw: dict = defaultdict(dict)
    for a in actuals_raw:
        raw = str(a.get("raw_esiid") or "").strip()
        month = (a.get("billing_month") or "")[:7]
        if raw and month:
            paid_raw[raw][month] = float(a.get("raw_amount") or 0)

    # Batch-fetch lead names
    lead_ids = list({d["lead_id"] for d in valid if d.get("lead_id")})
    leads_data: dict = {}
    if lead_ids:
        leads_res = db.table("leads").select("id, first_name, last_name").in_("id", lead_ids).execute().data or []
        leads_data = {l["id"]: l for l in leads_res}

    results = []
    for deal in valid:
        esiid_raw  = str(deal["esiid"]).strip()
        esiid_norm = _norm_esiid(esiid_raw)

        expected_months = _months_between(deal["start_date"], deal.get("end_date"), now)
        if not expected_months:
            continue

        # Merge paid months from both raw and normalized lookups
        paid_months_map: dict = {}
        paid_months_map.update(paid.get(esiid_norm, {}))
        paid_months_map.update(paid_raw.get(esiid_raw, {}))

        paid_list    = sorted(m for m in expected_months if m in paid_months_map)
        missing_list = sorted(m for m in expected_months if m not in paid_months_map)

        # Extra payments received outside the contract window
        extra_list = sorted(m for m in paid_months_map if m not in expected_months)

        commission_pm = float(deal.get("est_kwh") or 0) * float(deal.get("adder") or 0)

        lead = leads_data.get(deal.get("lead_id") or "")
        lead_name = f"{lead.get('first_name','')} {lead.get('last_name','')}".strip() if lead else "—"

        results.append({
            "deal_id":              deal["id"],
            "lead_id":              deal.get("lead_id"),
            "lead_name":            lead_name,
            "supplier":             deal.get("supplier") or "—",
            "sales_agent":          deal.get("sales_agent") or "Unassigned",
            "esiid":                esiid_raw,
            "status":               deal["status"],
            "start_date":           deal["start_date"][:10],
            "end_date":             (deal.get("end_date") or "")[:10] or None,
            "contract_months":      len(expected_months),
            "expected_months":      expected_months,
            "paid_months":          paid_list,
            "missing_months":       missing_list,
            "extra_months":         extra_list,
            "payments_expected":    len(expected_months),
            "payments_received":    len(paid_list),
            "payments_missing":     len(missing_list),
            "est_monthly_comm":     round(commission_pm, 2),
            "est_missing_comm":     round(len(missing_list) * commission_pm, 2),
            "est_total_comm":       round(len(expected_months) * commission_pm, 2),
            "pct_received":         round(len(paid_list) / len(expected_months) * 100, 1) if expected_months else 0,
        })

    results.sort(key=lambda x: (x["payments_missing"], -x["est_missing_comm"]), reverse=True)

    total_expected  = sum(r["payments_expected"]  for r in results)
    total_received  = sum(r["payments_received"]  for r in results)
    total_missing   = sum(r["payments_missing"]   for r in results)
    est_missing_com = sum(r["est_missing_comm"]   for r in results)

    return {
        "summary": {
            "deals_analyzed":          len(results),
            "total_payments_expected": total_expected,
            "total_payments_received": total_received,
            "total_payments_missing":  total_missing,
            "pct_received":            round(total_received / total_expected * 100, 1) if total_expected else 0,
            "est_missing_commission":  round(est_missing_com, 2),
        },
        "deals": results,
    }


@router.get("/commission-tracker")
def commission_tracker(months_back: int = Query(12), user: UserContext = Depends(require_admin)):
    """
    Returns monthly commission totals received per supplier + flags which
    suppliers have not submitted a payment for each recent month.
    """
    db = get_client()
    now = datetime.now(timezone.utc)

    # Build list of last N months (YYYY-MM-01 strings)
    month_list = []
    for i in range(months_back - 1, -1, -1):
        y = now.year - ((now.month - 1 - i + 12 * 100) // 12 - 100 if (now.month - 1 - i) < 0 else 0)
        m = ((now.month - 1 - i) % 12) + 1
        y = now.year + (now.month - 1 - i) // 12 - (1 if (now.month - 1 - i) < 0 else 0)
        month_list.append(f"{y:04d}-{m:02d}-01")

    # Easier: just subtract months properly
    month_list = []
    for i in range(months_back - 1, -1, -1):
        total_months = now.month - 1 - i
        y = now.year + total_months // 12
        m = total_months % 12 + 1
        if total_months < 0:
            neg = abs(total_months)
            y = now.year - (neg + 11) // 12
            m = 12 - (neg - 1) % 12
        month_list.append(f"{y:04d}-{m:02d}-01")

    # Fetch all upload_batches (confirmed) with amount_received
    batches = db.table("upload_batches") \
        .select("id, supplier_id, billing_month_override:billing_month, amount_received, total_affinity_amount, confirmed_at, suppliers(name)") \
        .eq("status", "confirmed") \
        .execute().data or []

    # Fetch actual_commissions aggregated by supplier + billing_month
    actuals = db.table("actual_commissions") \
        .select("supplier_id, billing_month, raw_amount") \
        .execute().data or []

    # Aggregate: total received per supplier per month
    # Key: (supplier_id, billing_month YYYY-MM-01)
    received: dict = defaultdict(float)
    for row in actuals:
        sid = row.get("supplier_id") or "unknown"
        bm  = (row.get("billing_month") or "")[:7]  # YYYY-MM
        if bm:
            received[(sid, bm + "-01")] += float(row.get("raw_amount") or 0)

    # Get all known suppliers that have sent any commission
    supplier_ids = list({row.get("supplier_id") for row in actuals if row.get("supplier_id")})
    suppliers_info: dict = {}
    if supplier_ids:
        s_rows = db.table("suppliers").select("id, name").in_("id", supplier_ids).execute().data or []
        suppliers_info = {s["id"]: s["name"] for s in s_rows}

    # Build per-month totals
    monthly_totals = []
    for bm in month_list:
        label = datetime.strptime(bm, "%Y-%m-%d").strftime("%b '%y")
        total = sum(v for (sid, m), v in received.items() if m == bm)
        monthly_totals.append({"month": bm, "label": label, "total": round(total, 2)})

    # Find missing: which suppliers had NO payment in each recent month
    # Only flag months that are at least 30 days old
    missing_by_month = []
    for bm in month_list:
        bm_dt = datetime.strptime(bm, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        age_days = (now - bm_dt).days
        if age_days < 30:
            continue
        paid_suppliers = {sid for (sid, m) in received if m == bm and received[(sid, m)] > 0}
        all_suppliers  = set(supplier_ids)
        missing = []
        for sid in all_suppliers:
            if sid not in paid_suppliers:
                missing.append({"supplier_id": sid, "supplier_name": suppliers_info.get(sid, sid)})
        if missing:
            missing_by_month.append({
                "month": bm,
                "label": datetime.strptime(bm, "%Y-%m-%d").strftime("%b %Y"),
                "missing_suppliers": missing,
            })

    # Amount received vs affinity total per supplier (overall)
    supplier_summary = []
    for sid in supplier_ids:
        name = suppliers_info.get(sid, sid)
        total_recv = sum(v for (s, m), v in received.items() if s == sid)
        supplier_summary.append({
            "supplier_id":   sid,
            "supplier_name": name,
            "total_received": round(total_recv, 2),
            "months_paid":   len({m for (s, m) in received if s == sid and received[(s, m)] > 0}),
        })
    supplier_summary.sort(key=lambda x: x["total_received"], reverse=True)

    return {
        "monthly_totals":   monthly_totals,
        "missing_by_month": missing_by_month,
        "supplier_summary": supplier_summary,
    }


@router.post("/chat")
def ai_chat(data: dict = Body(...), user: UserContext = Depends(require_admin)):
    message = str(data.get("message", "")).strip()
    history = data.get("history", [])
    if not message:
        raise HTTPException(status_code=400, detail="message is required")
    reply = chat_with_context(message, history)
    return {"reply": reply}


@router.get("/command-center")
def command_center(user: UserContext = Depends(require_admin)):
    """One call for the AI Operation Center: KPIs, provider scorecard,
    integration health, and a prioritized needs-attention list."""
    import os

    from app.services.business_health import build_business_health

    db = get_client()
    bh = build_business_health(db)

    # open reconciliation issues per supplier (excluding matched/unexpected noise)
    sups = {s["id"]: s for s in (db.table("suppliers").select("id,code,name").execute().data or [])}
    open_items = []
    off = 0
    while True:
        page = db.table("reconciliation_items") \
            .select("supplier_id,status,is_resolved,billing_month,expected_amount,actual_amount") \
            .eq("is_resolved", False).neq("status", "matched") \
            .order("id").range(off, off + 999).execute().data or []
        open_items.extend(page)
        if len(page) < 1000 or len(open_items) >= 6000:
            break
        off += 1000

    by_sup: dict = {}
    for i in open_items:
        b = by_sup.setdefault(i["supplier_id"], {"disputes": 0, "unknown": 0, "dollars": 0.0})
        if i["status"] == "unexpected":
            b["unknown"] += 1
        else:
            b["disputes"] += 1
            b["dollars"] += max(0.0, float(i.get("expected_amount") or 0) - float(i.get("actual_amount") or 0))

    integrations = {
        "ai": bool(os.environ.get("ANTHROPIC_API_KEY", "").strip()),
        "email": bool(os.environ.get("RESEND_API_KEY", "").strip()),
        "sms": bool(os.environ.get("TELNYX_API_KEY", "").strip()),
        "gmail_ingest": bool(os.environ.get("GMAIL_APP_PASSWORD", "").strip()),
    }

    # actions, most severe first
    actions = []
    total_disputes = sum(b["disputes"] for b in by_sup.values())
    total_dollars = round(sum(b["dollars"] for b in by_sup.values()), 2)
    if total_disputes:
        worst = max(by_sup.items(), key=lambda kv: kv[1]["dollars"])
        actions.append({
            "severity": "high", "icon": "💸",
            "title": f"{total_disputes} open payment dispute{'s' if total_disputes != 1 else ''} worth ${total_dollars:,.0f}",
            "detail": f"Largest exposure: {sups.get(worst[0], {}).get('name', '?')} (${worst[1]['dollars']:,.0f}). "
                      "Review root causes and raise them with the provider rep.",
            "link": "/reconciliation",
        })
    unknown_now = sum(b["unknown"] for b in by_sup.values())
    if unknown_now:
        actions.append({
            "severity": "medium", "icon": "❓",
            "title": f"{unknown_now} payments for accounts not in the CRM",
            "detail": "Providers are paying ESI IDs the CRM doesn't know. Import or link them so the book stays complete.",
            "link": "/reconciliation",
        })
    stale = [p for p in bh.get("providers", []) if (p.get("months_not_reporting") or 0) >= 2]
    for p in stale[:3]:
        actions.append({
            "severity": "high", "icon": "📭",
            "title": f"{p['name']}: no statement for {p['months_not_reporting']} months",
            "detail": "Chase the provider for missing commission statements — this is unverified revenue.",
            "link": "/uploads",
        })
    wb = bh.get("winback") or {}
    if wb.get("count"):
        actions.append({
            "severity": "medium", "icon": "📞",
            "title": f"{wb['count']} win-back candidates worth ~${wb.get('monthly_value', 0):,.0f}/mo",
            "detail": "Accounts the provider reports as leaving or gone. Call before the switch is final.",
            "link": "/crm/dropped",
        })
    missing = [k for k, v in integrations.items() if not v]
    if missing:
        labels = {"ai": "AI briefings (Anthropic key)", "email": "customer email (Resend)",
                  "sms": "SMS (Telnyx)", "gmail_ingest": "statement auto-ingest (Gmail)"}
        actions.append({
            "severity": "low", "icon": "🔌",
            "title": f"{len(missing)} integration{'s' if len(missing) != 1 else ''} not configured",
            "detail": "Waiting on API keys in Railway: " + ", ".join(labels[m] for m in missing) + ".",
            "link": None,
        })

    scorecard = []
    for p in bh.get("providers", []):
        sid = next((k for k, v in sups.items() if v["name"] == p["name"]), None)
        issues = by_sup.get(sid, {})
        scorecard.append({**p, "open_disputes": issues.get("disputes", 0),
                          "unknown_accounts": issues.get("unknown", 0),
                          "open_dollars": round(issues.get("dollars", 0.0), 2)})

    return {
        "kpis": {
            **(bh.get("book") or {}),
            "open_dispute_dollars": total_dollars,
            "open_disputes": total_disputes,
        },
        "months": bh.get("months", []),
        "growth": bh.get("growth", []),
        "providers": scorecard,
        "actions": actions,
        "integrations": integrations,
        "winback": wb,
        "computed_at": bh.get("computed_at"),
    }
