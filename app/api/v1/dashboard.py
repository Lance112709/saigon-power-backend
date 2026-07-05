from fastapi import APIRouter, Query, Depends
from typing import Optional
from datetime import date, datetime, timezone, timedelta
from app.db.client import get_client
from app.auth.deps import require_manager, get_current_user, UserContext

router = APIRouter()

def get_latest_month(db) -> str:
    res = db.table("actual_commissions").select("billing_month").order("billing_month", desc=True).limit(1).execute()
    if res.data:
        return res.data[0]["billing_month"]
    return date.today().replace(day=1).isoformat()

def sum_all(db, table: str, column: str, filters: dict) -> float:
    """Fetch all pages and sum a column — works around Supabase 1000-row limit."""
    total = 0.0
    offset = 0
    limit = 1000
    while True:
        q = db.table(table).select(column)
        for k, v in filters.items():
            q = q.eq(k, v)
        res = q.range(offset, offset + limit - 1).execute()
        if not res.data:
            break
        total += sum(r[column] for r in res.data if r[column] is not None)
        if len(res.data) < limit:
            break
        offset += limit
    return total

def _latest_v2_runs(db, billing_month: Optional[str] = None) -> list:
    """Latest reconciliation-v2 run per supplier (optionally pinned to a month)."""
    q = db.table("reconciliation_runs").select(
        "id,billing_month,supplier_id,total_expected,total_actual,total_discrepancy,"
        "missing_count,short_paid_count,over_paid_count,matched_count,unexpected_count,"
        "suppliers(name,code)"
    ).like("notes", '%"engine": "v2"%').order("billing_month", desc=True)
    if billing_month:
        q = q.eq("billing_month", billing_month)
    runs = q.limit(1000).execute().data or []
    latest = {}
    for r in runs:
        key = r["supplier_id"]
        if key not in latest:
            latest[key] = r
    return list(latest.values())


@router.get("/overview")
def get_overview(billing_month: Optional[str] = Query(None), user: UserContext = Depends(require_manager)):
    """Reconciliation snapshot from engine-v2 runs (latest per provider)."""
    db = get_client()
    runs = _latest_v2_runs(db, billing_month)

    total_expected = sum(r["total_expected"] or 0 for r in runs)
    total_actual = sum(r["total_actual"] or 0 for r in runs)
    missing = sum(r["missing_count"] or 0 for r in runs)
    wrong_rate = sum(r["short_paid_count"] or 0 for r in runs)
    unresolved = db.table("reconciliation_items").select("id", count="exact") \
        .eq("is_resolved", False).in_("status", ["missing", "short_paid", "over_paid"]).execute()
    pending_uploads = db.table("upload_batches").select("id", count="exact").in_("status", ["pending", "review"]).execute()

    return {
        "billing_month": billing_month or (max((r["billing_month"] for r in runs), default=None)),
        "total_expected": round(total_expected, 2),
        "total_actual": round(total_actual, 2),
        "net_discrepancy": round(total_actual - total_expected, 2),
        "missing_payments": missing,
        "wrong_rate_accounts": wrong_rate,
        "unresolved_discrepancies": unresolved.count or 0,
        "pending_uploads": pending_uploads.count or 0,
        "providers": [
            {"name": r["suppliers"]["name"], "month": r["billing_month"][:7],
             "received": r["total_actual"], "missing": r["missing_count"]}
            for r in sorted(runs, key=lambda x: -(x["total_actual"] or 0))
        ],
    }

@router.get("/business-health")
def business_health(user: UserContext = Depends(require_manager)):
    """Growth, book value, provider quality, win-back queue, open dollars,
    agent scoreboard — all from verified payment data. Cached 10 minutes."""
    from app.services.business_health import build_business_health
    db = get_client()
    return build_business_health(db)


@router.get("/leads-stats")
def get_leads_stats(user: UserContext = Depends(get_current_user)):
    db = get_client()
    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    week_start = (now - timedelta(days=7)).isoformat()
    thirty_days_out = (now + timedelta(days=30)).date().isoformat()
    today_str = now.date().isoformat()

    is_agent = user.is_sales_agent
    # Always fetch fresh from DB — never trust stale token value
    agent_name = None
    if is_agent:
        u = db.table("users").select("sales_agent_name").eq("id", user.user_id).limit(1).execute()
        agent_name = (u.data[0].get("sales_agent_name") or "").strip() or None

    EMPTY_STATS = {
        "leads_today": 0, "leads_this_week": 0, "active_deals": 0,
        "expiring_soon": 0, "pipeline": {"lead": 0, "converted": 0},
        "portfolio": {"active_contracts": 0, "total_kwh": 0, "commission_mo": 0, "at_risk": 0},
        "recent_leads": [],
    }
    if is_agent and not agent_name:
        return EMPTY_STATS

    if is_agent:
        scoped_leads = db.table("leads").select("id").eq("sales_agent", agent_name).execute().data or []
        scoped_ids = [l["id"] for l in scoped_leads]
        if not scoped_ids:
            return EMPTY_STATS
        leads_today     = db.table("leads").select("id", count="exact").eq("sales_agent", agent_name).gte("created_at", today_start).execute()
        leads_week      = db.table("leads").select("id", count="exact").eq("sales_agent", agent_name).gte("created_at", week_start).execute()
        active_deals    = db.table("lead_deals").select("id, est_kwh, adder", count="exact").eq("status", "Active").in_("lead_id", scoped_ids).execute()
        expiring        = db.table("lead_deals").select("id", count="exact").eq("status", "Active").in_("lead_id", scoped_ids).lte("end_date", thirty_days_out).gte("end_date", today_str).execute()
        expiring_crm    = db.table("crm_deals").select("id", count="exact").eq("deal_status", "ACTIVE").ilike("sales_agent", f"%{agent_name}%").lte("contract_end_date", thirty_days_out).gte("contract_end_date", today_str).execute()
        leads_count     = db.table("leads").select("id", count="exact").eq("status", "lead").eq("sales_agent", agent_name).execute()
        converted_count = db.table("leads").select("id", count="exact").eq("status", "converted").eq("sales_agent", agent_name).execute()
        recent_raw      = db.table("leads").select("*, lead_deals(id, status, product_type)").eq("sales_agent", agent_name).order("created_at", desc=True).limit(5).execute()
    else:
        leads_today     = db.table("leads").select("id", count="exact").gte("created_at", today_start).execute()
        leads_week      = db.table("leads").select("id", count="exact").gte("created_at", week_start).execute()
        active_deals    = db.table("lead_deals").select("id, est_kwh, adder", count="exact").eq("status", "Active").execute()
        expiring        = db.table("lead_deals").select("id", count="exact").eq("status", "Active").lte("end_date", thirty_days_out).gte("end_date", today_str).execute()
        expiring_crm    = db.table("crm_deals").select("id", count="exact").eq("deal_status", "ACTIVE").lte("contract_end_date", thirty_days_out).gte("contract_end_date", today_str).execute()
        leads_count     = db.table("leads").select("id", count="exact").eq("status", "lead").execute()
        converted_count = db.table("leads").select("id", count="exact").eq("status", "converted").execute()
        recent_raw      = db.table("leads").select("*, lead_deals(id, status, product_type)").order("created_at", desc=True).limit(5).execute()

    pipeline = {"lead": leads_count.count or 0, "converted": converted_count.count or 0}
    total_kwh     = sum((r.get("est_kwh") or 0) for r in active_deals.data)
    commission_mo = sum((r.get("est_kwh") or 0) * (r.get("adder") or 0) for r in active_deals.data)

    # Deals added recently to the pipeline CRM
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()
    nd_q = db.table("lead_deals").select("id", count="exact").gte("created_at", month_start)
    nw_q = db.table("lead_deals").select("id", count="exact").gte("created_at", week_start)
    if is_agent:
        nd_q = nd_q.in_("lead_id", scoped_ids)
        nw_q = nw_q.in_("lead_id", scoped_ids)
    deals_added_month = nd_q.limit(1).execute().count or 0
    deals_added_week = nw_q.limit(1).execute().count or 0

    # Full book: pipeline deals (lead_deals) + imported contracts (crm_deals)
    crm_q = db.table("crm_deals").select("id", count="exact").eq("deal_status", "ACTIVE")
    if is_agent:
        crm_q = crm_q.ilike("sales_agent", f"%{agent_name}%")
    crm_active = crm_q.limit(1).execute().count or 0
    pipeline_active = active_deals.count or 0

    # Real dollars: commission received per month from reconciliation-v2 runs
    finance = None
    if not is_agent:
        runs = db.table("reconciliation_runs").select("billing_month,total_actual,supplier_id") \
            .like("notes", '%"engine": "v2"%').limit(1000).execute().data or []
        by_month: dict = {}
        provs_by_month: dict = {}
        for r in runs:
            m = r["billing_month"][:7]
            by_month[m] = by_month.get(m, 0) + (r["total_actual"] or 0)
            provs_by_month.setdefault(m, set()).add(r["supplier_id"])
        months = sorted(by_month.keys())[-6:]
        total_providers = len({p for s in provs_by_month.values() for p in s})
        finance = {
            "received_history": [{"month": m, "amount": round(by_month[m], 2),
                                  "providers_reported": len(provs_by_month.get(m, set()))} for m in months],
            "received_last_month": round(by_month[months[-1]], 2) if months else 0,
            "received_month": months[-1] if months else None,
            "providers_reported": len(provs_by_month.get(months[-1], set())) if months else 0,
            "total_providers": total_providers,
        }

    recent_leads = []
    for l in recent_raw.data:
        deals = l.pop("lead_deals", []) or []
        recent_leads.append({
            **l,
            "full_name": f"{l.get('first_name','')} {l.get('last_name','')}".strip(),
            "product_type": next((d.get("product_type") for d in deals if d.get("product_type")), None),
            "deal_status": next((d.get("status") for d in deals), None),
        })

    return {
        "leads_today":     leads_today.count or 0,
        "leads_this_week": leads_week.count or 0,
        "active_deals":    pipeline_active + crm_active,
        "active_deals_pipeline": pipeline_active,
        "active_deals_imported": crm_active,
        "deals_added_this_month": deals_added_month,
        "deals_added_this_week": deals_added_week,
        "expiring_soon":   (expiring.count or 0) + (expiring_crm.count or 0),
        "pipeline":        pipeline,
        "finance":         finance,
        "portfolio": {
            "active_contracts": pipeline_active + crm_active,
            "total_kwh":        round(total_kwh, 2),
            "commission_mo":    round(commission_mo, 2),
            "at_risk":          (expiring.count or 0) + (expiring_crm.count or 0),
        },
        "recent_leads": recent_leads,
    }

@router.get("/expiring-deals")
def get_expiring_deals(user: UserContext = Depends(get_current_user)):
    db = get_client()
    today = datetime.now(timezone.utc).date()
    sixty_out = (today + timedelta(days=60)).isoformat()
    today_str = today.isoformat()

    agent_name = None
    if user.is_sales_agent:
        u = db.table("users").select("sales_agent_name").eq("id", user.user_id).limit(1).execute()
        agent_name = (u.data[0].get("sales_agent_name") or "").strip() or None
        if not agent_name:
            return []

    results = []

    # ── CRM Leads deals ──────────────────────────────────────────────────────────
    q = db.table("lead_deals").select(
        "id, end_date, supplier, plan_name, contract_term, lead_id, leads(first_name, last_name, phone, sgp_customer_id, sales_agent)"
    ).eq("status", "Active").lte("end_date", sixty_out).gte("end_date", today_str).order("end_date")
    for d in q.execute().data:
        lead = d.pop("leads", None) or {}
        if agent_name and (lead.get("sales_agent") or "").lower() != agent_name.lower():
            continue
        end = d.get("end_date")
        days_left = (date.fromisoformat(end) - today).days if end else None
        results.append({
            "deal_id":         d["id"],
            "lead_id":         d.get("lead_id"),
            "customer_id":     None,
            "source":          "crm",
            "sgp_customer_id": lead.get("sgp_customer_id"),
            "full_name":       f"{lead.get('first_name','')} {lead.get('last_name','')}".strip(),
            "phone":           lead.get("phone"),
            "supplier":        d.get("supplier"),
            "plan_name":       d.get("plan_name"),
            "contract_term":   d.get("contract_term"),
            "end_date":        end,
            "days_left":       days_left,
        })

    # ── Imported Customers deals ─────────────────────────────────────────────────
    q2 = db.table("crm_deals").select(
        "id, contract_end_date, provider, contract_term, customer_id, sales_agent, "
        "crm_customers(full_name, phone)"
    ).eq("deal_status", "ACTIVE").lte("contract_end_date", sixty_out).gte("contract_end_date", today_str).order("contract_end_date")
    for d in q2.execute().data:
        cust = d.pop("crm_customers", None) or {}
        if agent_name and (d.get("sales_agent") or "").lower() != agent_name.lower():
            continue
        end = d.get("contract_end_date")
        days_left = (date.fromisoformat(end[:10]) - today).days if end else None
        results.append({
            "deal_id":         d["id"],
            "lead_id":         None,
            "customer_id":     d.get("customer_id"),
            "source":          "imported",
            "sgp_customer_id": None,
            "full_name":       cust.get("full_name", ""),
            "phone":           cust.get("phone"),
            "supplier":        d.get("provider"),
            "plan_name":       None,
            "contract_term":   d.get("contract_term"),
            "end_date":        end[:10] if end else None,
            "days_left":       days_left,
        })

    results.sort(key=lambda x: x["end_date"] or "")
    return results

@router.get("/commission-history")
def get_commission_history(user: UserContext = Depends(get_current_user)):
    db = get_client()
    twelve_ago = (datetime.now(timezone.utc) - timedelta(days=365)).date().isoformat()
    res = db.table("lead_deals").select("start_date, est_kwh, adder").eq("status", "Active").gte("start_date", twelve_ago).execute()
    monthly: dict = {}
    for r in res.data:
        if not r.get("start_date"):
            continue
        month = r["start_date"][:7]
        monthly[month] = monthly.get(month, 0) + (r.get("est_kwh") or 0) * (r.get("adder") or 0)
    sorted_months = sorted(monthly.keys())[-6:]
    return [{"month": m, "amount": round(monthly[m], 2)} for m in sorted_months]

@router.get("/revenue-forecast")
def get_revenue_forecast(user: UserContext = Depends(get_current_user)):
    db = get_client()
    today = date.today()
    cutoff = date(today.year + 2, today.month, 1)

    # kWh/month assumptions by meter type (Texas averages)
    KWH_DEFAULTS = {"Residential": 1100.0, "Commercial": 2500.0}
    DEFAULT_KWH = 1100.0

    monthly: dict = {}
    by_supplier: dict = {}
    contributing = 0
    skipped = 0

    def _next_month(d: date) -> date:
        return (d.replace(day=28) + timedelta(days=4)).replace(day=1)

    def _project(kwh: float, adder: float, end_d: date, supplier: str):
        nonlocal contributing
        if not kwh or not adder or end_d <= today:
            return
        commission_mo = kwh * adder
        contributing += 1
        cur = today.replace(day=1)
        while cur <= end_d and cur < cutoff:
            key = cur.strftime("%Y-%m")
            monthly[key] = monthly.get(key, 0) + commission_mo
            by_supplier[supplier] = by_supplier.get(supplier, 0) + commission_mo
            cur = _next_month(cur)

    # ── Source 1: lead_deals (est_kwh * adder) ───────────────────────────────
    ld_offset = 0
    while True:
        rows = db.table("lead_deals").select(
            "est_kwh, adder, end_date, supplier, status"
        ).eq("status", "Active").range(ld_offset, ld_offset + 999).execute().data or []
        for d in rows:
            kwh   = float(d.get("est_kwh") or 0)
            adder = float(d.get("adder") or 0)
            end_raw = (d.get("end_date") or "")[:10]
            if not kwh or not adder or not end_raw:
                skipped += 1
                continue
            try:
                end_d = datetime.strptime(end_raw, "%Y-%m-%d").date()
            except Exception:
                skipped += 1
                continue
            _project(kwh, adder, end_d, d.get("supplier") or "Unknown")
        if len(rows) < 1000:
            break
        ld_offset += 1000

    # ── Source 2: crm_deals (assumed kWh by meter_type * adder) ─────────────
    cd_offset = 0
    while True:
        rows = db.table("crm_deals").select(
            "adder, meter_type, contract_end_date, provider, deal_status"
        ).eq("deal_status", "ACTIVE").not_.is_("adder", "null").not_.is_("contract_end_date", "null").range(cd_offset, cd_offset + 999).execute().data or []
        for d in rows:
            adder = float(d.get("adder") or 0)
            kwh   = KWH_DEFAULTS.get(d.get("meter_type") or "", DEFAULT_KWH)
            end_raw = (d.get("contract_end_date") or "")[:10]
            if not adder or not end_raw:
                skipped += 1
                continue
            try:
                end_d = datetime.strptime(end_raw, "%Y-%m-%d").date()
            except Exception:
                skipped += 1
                continue
            _project(kwh, adder, end_d, d.get("provider") or "Unknown")
        if len(rows) < 1000:
            break
        cd_offset += 1000

    sorted_months = sorted(monthly.keys())
    total = sum(monthly.values())
    return {
        "monthly": [{"month": m, "amount": round(monthly[m], 2)} for m in sorted_months],
        "by_supplier": [{"supplier": k, "amount": round(v, 2)} for k, v in sorted(by_supplier.items(), key=lambda x: -x[1])],
        "total_projected": round(total, 2),
        "avg_monthly": round(total / len(monthly), 2) if monthly else 0,
        "contributing_deals": contributing,
        "total_in_report": contributing + skipped,
        "months_out": len(sorted_months),
    }

@router.get("/supplier-breakdown")
def supplier_breakdown(billing_month: Optional[str] = Query(None), user: UserContext = Depends(get_current_user)):
    """Per-provider expected vs received from the latest v2 reconciliation runs."""
    db = get_client()
    runs = _latest_v2_runs(db, billing_month)
    result = []
    for r in sorted(runs, key=lambda x: -(x["total_actual"] or 0)):
        result.append({
            "supplier_id": r["supplier_id"],
            "supplier_name": r["suppliers"]["name"],
            "supplier_code": r["suppliers"]["code"],
            "billing_month": r["billing_month"][:7],
            "expected": round(r["total_expected"] or 0, 2),
            "actual": round(r["total_actual"] or 0, 2),
            "discrepancy": round((r["total_actual"] or 0) - (r["total_expected"] or 0), 2),
            "missing": r["missing_count"] or 0,
            "wrong_rate": r["short_paid_count"] or 0,
        })
    return result
