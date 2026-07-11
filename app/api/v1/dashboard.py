from fastapi import APIRouter, Query, Depends
from typing import Optional
import re
from datetime import date, datetime, timezone, timedelta
from app.db.client import get_client
from app.auth.deps import require_manager, require_admin, get_current_user, UserContext

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

    from app.utils.deals import is_month_to_month

    def _count_expiring_lead(q):
        rows = q.execute().data or []
        return sum(1 for r in rows if not is_month_to_month(r.get("rate_type"), r.get("plan_name"), r.get("contract_term")))

    def _count_expiring_crm(q):
        rows = q.execute().data or []
        return sum(1 for r in rows if not is_month_to_month(r.get("product_type"), r.get("contract_term")))

    _LEAD_EXP_COLS = "id, rate_type, plan_name, contract_term"
    _CRM_EXP_COLS = "id, product_type, contract_term"

    if is_agent:
        scoped_leads = db.table("leads").select("id").eq("sales_agent", agent_name).execute().data or []
        scoped_ids = [l["id"] for l in scoped_leads]
        if not scoped_ids:
            return EMPTY_STATS
        leads_today     = db.table("leads").select("id", count="exact").eq("sales_agent", agent_name).gte("created_at", today_start).execute()
        leads_week      = db.table("leads").select("id", count="exact").eq("sales_agent", agent_name).gte("created_at", week_start).execute()
        active_deals    = db.table("lead_deals").select("id, est_kwh, adder", count="exact").eq("status", "Active").in_("lead_id", scoped_ids).execute()
        expiring_n      = _count_expiring_lead(db.table("lead_deals").select(_LEAD_EXP_COLS).eq("status", "Active").in_("lead_id", scoped_ids).lte("end_date", thirty_days_out).gte("end_date", today_str))
        expiring_crm_n  = _count_expiring_crm(db.table("crm_deals").select(_CRM_EXP_COLS).eq("deal_status", "ACTIVE").ilike("sales_agent", f"%{agent_name}%").lte("contract_end_date", thirty_days_out).gte("contract_end_date", today_str))
        leads_count     = db.table("leads").select("id", count="exact").eq("status", "lead").eq("sales_agent", agent_name).execute()
        converted_count = db.table("leads").select("id", count="exact").eq("status", "converted").eq("sales_agent", agent_name).execute()
        recent_raw      = db.table("leads").select("*, lead_deals(id, status, product_type)").eq("sales_agent", agent_name).order("created_at", desc=True).limit(5).execute()
    else:
        leads_today     = db.table("leads").select("id", count="exact").gte("created_at", today_start).execute()
        leads_week      = db.table("leads").select("id", count="exact").gte("created_at", week_start).execute()
        active_deals    = db.table("lead_deals").select("id, est_kwh, adder", count="exact").eq("status", "Active").execute()
        expiring_n      = _count_expiring_lead(db.table("lead_deals").select(_LEAD_EXP_COLS).eq("status", "Active").lte("end_date", thirty_days_out).gte("end_date", today_str))
        expiring_crm_n  = _count_expiring_crm(db.table("crm_deals").select(_CRM_EXP_COLS).eq("deal_status", "ACTIVE").lte("contract_end_date", thirty_days_out).gte("contract_end_date", today_str))
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
        "expiring_soon":   expiring_n + expiring_crm_n,
        "pipeline":        pipeline,
        "finance":         finance,
        "portfolio": {
            "active_contracts": pipeline_active + crm_active,
            "total_kwh":        round(total_kwh, 2),
            "commission_mo":    round(commission_mo, 2),
            "at_risk":          expiring_n + expiring_crm_n,
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
    from app.utils.deals import is_month_to_month
    q = db.table("lead_deals").select(
        "id, end_date, supplier, plan_name, rate_type, contract_term, lead_id, leads(first_name, last_name, phone, sgp_customer_id, sales_agent)"
    ).eq("status", "Active").lte("end_date", sixty_out).gte("end_date", today_str).order("end_date")
    for d in q.execute().data:
        if is_month_to_month(d.get("rate_type"), d.get("plan_name"), d.get("contract_term")):
            continue
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
        "id, contract_end_date, provider, product_type, contract_term, customer_id, sales_agent, "
        "crm_customers(full_name, phone)"
    ).eq("deal_status", "ACTIVE").lte("contract_end_date", sixty_out).gte("contract_end_date", today_str).order("contract_end_date")
    for d in q2.execute().data:
        if is_month_to_month(d.get("product_type"), d.get("contract_term")):
            continue
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

    # Real trailing usage per ESI ID from the last 5 statement months;
    # flat Texas averages are only the fallback for meters never seen
    # on a statement.
    DEFAULT_RES_KWH = 1100.0
    DEFAULT_COM_KWH = 2500.0

    def _default_kwh(meter_type: str) -> float:
        return DEFAULT_COM_KWH if "commercial" in (meter_type or "").lower() else DEFAULT_RES_KWH

    usage_floor = (today.replace(day=1) - timedelta(days=155)).isoformat()
    usage_sum: dict = {}
    usage_months: dict = {}
    u_off = 0
    while True:
        page = db.table("actual_commissions").select("raw_esiid, raw_kwh, billing_month") \
            .gte("billing_month", usage_floor).order("id").range(u_off, u_off + 999).execute().data or []
        for r in page:
            kwh = float(r.get("raw_kwh") or 0)
            es = re.sub(r"\D", "", r.get("raw_esiid") or "")
            if not es or kwh <= 0:
                continue
            usage_sum[es] = usage_sum.get(es, 0.0) + kwh
            usage_months.setdefault(es, set()).add(r["billing_month"][:7])
        if len(page) < 1000:
            break
        u_off += 1000
    usage_avg = {es: usage_sum[es] / len(m) for es, m in usage_months.items() if m}

    monthly: dict = {}
    by_supplier: dict = {}
    contributing = 0
    skipped = 0
    usage_based = 0
    active_esiids: set = set()   # every active deal's ESI, for actual-usage totals
    not_projected = {"expired_or_month_to_month": 0, "missing_adder": 0, "missing_end_date": 0}

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

    # ── Source 1: lead_deals (real usage, else est_kwh) ─────────────────────
    ld_offset = 0
    while True:
        rows = db.table("lead_deals").select(
            "esiid, est_kwh, adder, end_date, supplier, status"
        ).eq("status", "Active").range(ld_offset, ld_offset + 999).execute().data or []
        for d in rows:
            es = re.sub(r"\D", "", d.get("esiid") or "")
            if es:
                active_esiids.add(es)
            real = usage_avg.get(es)
            kwh   = real or float(d.get("est_kwh") or 0)
            adder = float(d.get("adder") or 0)
            end_raw = (d.get("end_date") or "")[:10]
            if not adder:
                not_projected["missing_adder"] += 1
                skipped += 1
                continue
            if adder > 0.05 or adder < 0.0005:
                not_projected["suspect_adder"] = not_projected.get("suspect_adder", 0) + 1
                skipped += 1
                continue
            if not end_raw:
                not_projected["missing_end_date"] += 1
                skipped += 1
                continue
            if not kwh:
                skipped += 1
                continue
            try:
                end_d = datetime.strptime(end_raw, "%Y-%m-%d").date()
            except Exception:
                skipped += 1
                continue
            if end_d <= today:
                not_projected["expired_or_month_to_month"] += 1
                skipped += 1
                continue
            if real:
                usage_based += 1
            _project(kwh, adder, end_d, d.get("supplier") or "Unknown")
        if len(rows) < 1000:
            break
        ld_offset += 1000

    # ── Source 2: crm_deals (real usage, else meter-type average) ───────────
    cd_offset = 0
    while True:
        rows = db.table("crm_deals").select(
            "esiid, adder, meter_type, contract_end_date, provider, deal_status"
        ).eq("deal_status", "ACTIVE").range(cd_offset, cd_offset + 999).execute().data or []
        for d in rows:
            es = re.sub(r"\D", "", d.get("esiid") or "")
            if es:
                active_esiids.add(es)
            adder = float(d.get("adder") or 0)
            if not adder:
                not_projected["missing_adder"] += 1
                skipped += 1
                continue
            if adder > 0.05 or adder < 0.0005:
                not_projected["suspect_adder"] = not_projected.get("suspect_adder", 0) + 1
                skipped += 1
                continue
            end_raw = (d.get("contract_end_date") or "")[:10]
            if not end_raw:
                not_projected["missing_end_date"] += 1
                skipped += 1
                continue
            try:
                end_d = datetime.strptime(end_raw, "%Y-%m-%d").date()
            except Exception:
                skipped += 1
                continue
            if end_d <= today:
                not_projected["expired_or_month_to_month"] += 1
                skipped += 1
                continue
            real = usage_avg.get(es)
            kwh = real or _default_kwh(d.get("meter_type"))
            if real:
                usage_based += 1
            _project(kwh, adder, end_d, d.get("provider") or "Unknown")
        if len(rows) < 1000:
            break
        cd_offset += 1000

    sorted_months = sorted(monthly.keys())
    total = sum(monthly.values())

    # Actual usage you're getting paid on: sum the real trailing-average
    # monthly kWh across the UNIQUE active-deal ESI IDs that appear on
    # statements (fallback estimates are excluded — this is billed-on usage).
    usage_esiids = active_esiids & usage_avg.keys()
    actual_usage_kwh_mo = round(sum(usage_avg[es] for es in usage_esiids))

    return {
        "monthly": [{"month": m, "amount": round(monthly[m], 2)} for m in sorted_months],
        "by_supplier": [{"supplier": k, "amount": round(v, 2)} for k, v in sorted(by_supplier.items(), key=lambda x: -x[1])],
        "total_projected": round(total, 2),
        "avg_monthly": round(total / len(monthly), 2) if monthly else 0,
        "contributing_deals": contributing,
        "usage_based_deals": usage_based,
        "total_in_report": contributing + skipped,
        "not_projected": not_projected,
        "months_out": len(sorted_months),
        # Actual metered usage from provider statements, across the active book
        "actual_usage_kwh_mo": actual_usage_kwh_mo,          # avg monthly kWh billed on
        "actual_usage_kwh_yr": actual_usage_kwh_mo * 12,     # annualized
        "actual_usage_accounts": len(usage_esiids),          # active ESIs with statement usage
        "active_accounts_total": len(active_esiids),         # all unique active ESIs
        "usage_window_months": 5,                            # trailing window used
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


@router.get("/commission-intelligence")
def commission_intelligence(billing_month: Optional[str] = Query(None),
                            user: UserContext = Depends(require_manager)):
    """Executive commission-audit numbers: expected vs received, money at
    risk (open exception cases), disputes, recovery rate."""
    from app.services.reconciliation_v2 import fetch_all
    from app.services.exception_cases import OPEN_STATUSES
    db = get_client()
    runs = _latest_v2_runs(db, billing_month)
    total_expected = round(sum(r["total_expected"] or 0 for r in runs), 2)
    total_received = round(sum(r["total_actual"] or 0 for r in runs), 2)

    def _safe(table, cols, filters=None):
        try:
            return fetch_all(db, table, cols, filters=filters)
        except Exception:
            return []  # migration 008 not applied yet

    cases = _safe("exception_cases",
                  "supplier_id,workflow_status,estimated_loss,recovered_amount,billing_month")
    open_cases = [c for c in cases if c.get("workflow_status") in OPEN_STATUSES]
    money_at_risk = round(sum(float(c.get("estimated_loss") or 0) for c in open_cases), 2)
    recovered_total = round(sum(float(c.get("recovered_amount") or 0) for c in cases), 2)
    this_month = datetime.now(timezone.utc).strftime("%Y-%m")
    recovered_this_month = round(sum(
        float(c.get("recovered_amount") or 0) for c in cases
        if c.get("workflow_status") == "recovered"
        and str(c.get("billing_month"))[:7] == this_month), 2)
    denom = recovered_total + money_at_risk
    recovery_rate = round(recovered_total / denom * 100, 1) if denom else None

    disputes = _safe("disputes", "status,total_claimed,total_recovered")
    pending = [d for d in disputes if d.get("status") in ("draft", "sent", "provider_responded")]
    findings = _safe("audit_findings",
                     "id,title,finding_type,estimated_impact,affected_count,status,billing_month,supplier_id")
    open_findings = sorted([f for f in findings if f.get("status") in ("open", "investigating", "disputed")],
                           key=lambda f: -(float(f.get("estimated_impact") or 0)))

    sups = {s["id"]: s for s in db.table("suppliers").select("id,name,code").limit(500).execute().data or []}
    accuracy = []
    for r in runs:
        total_items = sum((r.get(k) or 0) for k in
                          ("matched_count", "short_paid_count", "over_paid_count",
                           "missing_count", "unexpected_count"))
        open_loss = round(sum(float(c.get("estimated_loss") or 0) for c in open_cases
                              if c["supplier_id"] == r["supplier_id"]), 2)
        accuracy.append({
            "supplier_id": r["supplier_id"],
            "supplier_name": (r.get("suppliers") or {}).get("name")
                             or sups.get(r["supplier_id"], {}).get("name", ""),
            "billing_month": str(r["billing_month"])[:7],
            "accuracy_pct": round((r.get("matched_count") or 0) / total_items * 100, 1)
                            if total_items else None,
            "open_loss": open_loss,
        })
    accuracy.sort(key=lambda a: (a["accuracy_pct"] if a["accuracy_pct"] is not None else 101))

    return {
        "total_expected": total_expected,
        "total_received": total_received,
        "total_missing": round(max(0.0, total_expected - total_received), 2),
        "money_at_risk": money_at_risk,
        "open_cases": len(open_cases),
        "recovered_total": recovered_total,
        "recovered_this_month": recovered_this_month,
        "recovery_rate": recovery_rate,
        "pending_disputes": {"count": len(pending),
                             "claimed": round(sum(float(d.get("total_claimed") or 0)
                                                  for d in pending), 2)},
        "recovered_via_disputes": round(sum(float(d.get("total_recovered") or 0)
                                            for d in disputes), 2),
        "open_findings": [{**f, "supplier_name": sups.get(f.get("supplier_id"), {}).get("name", "")}
                          for f in open_findings[:8]],
        "provider_accuracy": accuracy,
    }


@router.get("/provider-scorecards")
def provider_scorecards(months: int = Query(6), user: UserContext = Depends(require_admin)):
    """Per-provider monthly accuracy/discrepancy history for the scorecard chart."""
    db = get_client()
    runs = db.table("reconciliation_runs").select(
        "billing_month,supplier_id,total_expected,total_actual,total_discrepancy,"
        "matched_count,short_paid_count,over_paid_count,missing_count,unexpected_count,"
        "suppliers(name,code)"
    ).like("notes", '%"engine": "v2"%').order("billing_month", desc=True) \
        .limit(1000).execute().data or []

    seen_months = sorted({str(r["billing_month"])[:7] for r in runs}, reverse=True)[:months]
    out = {}
    for r in runs:
        m = str(r["billing_month"])[:7]
        if m not in seen_months:
            continue
        key = r["supplier_id"]
        entry = out.setdefault(key, {
            "supplier_id": key,
            "supplier_name": (r.get("suppliers") or {}).get("name", ""),
            "months": {},
        })
        total_items = sum((r.get(k) or 0) for k in
                          ("matched_count", "short_paid_count", "over_paid_count",
                           "missing_count", "unexpected_count"))
        if m not in entry["months"]:
            entry["months"][m] = {
                "expected": round(r.get("total_expected") or 0, 2),
                "received": round(r.get("total_actual") or 0, 2),
                "discrepancy": round(r.get("total_discrepancy") or 0, 2),
                "accuracy_pct": round((r.get("matched_count") or 0) / total_items * 100, 1)
                                if total_items else None,
                "issues": total_items - (r.get("matched_count") or 0),
            }
    return {"months": sorted(seen_months), "providers": list(out.values())}


@router.get("/commission-forecast")
def get_commission_forecast(user: UserContext = Depends(require_manager)):
    """12-month commission projection from verified payments, contract
    roll-offs, and clawback exposure. Deterministic — see
    services/commission_forecast.py."""
    from app.services.commission_forecast import commission_forecast
    db = get_client()
    return commission_forecast(db)
