from fastapi import APIRouter, Query, Depends, HTTPException
import threading
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

# ── Revenue forecast ─────────────────────────────────────────────────────────
# Rebuilt 2026-09-10. Per meter: baseline kWh = mean of (statement kWh /
# seasonal index) over the last 12 statement months; projected kWh for a
# future month = baseline x that calendar month's index. The index is the
# median same-meter month/mean ratio across meters with (near) full-year
# history. Rate = the meter's own observed paid $/kWh when it has been on a
# statement; otherwise the contract adder x the supplier's realization
# factor (what the supplier actually paid vs contract on meters we can
# see). Supplier names are canonicalized to the paying entity.
FORECAST_MONTHS = 60
_FORECAST_CACHE: dict = {"at": None, "data": None}
_FORECAST_TTL = timedelta(hours=6)

_SUPPLIER_ALIASES = {
    "directenergy": "Discount Power", "discountpower": "Discount Power",
    "nrg": "NRG Commercial", "nrgcommercial": "NRG Commercial", "nrgenergy": "NRG Commercial",
    "budgetpower": "Budget Power", "budget": "Budget Power",
    "heritagepower": "Heritage Power", "heritage": "Heritage Power",
    "ironhorse": "Iron Horse", "ironhorseenergy": "Iron Horse",
    "hudsonenergy": "Hudson Energy", "hudson": "Hudson Energy",
    "chariotenergy": "Chariot Energy", "chariot": "Chariot Energy",
    "cleanskyenergy": "CleanSky Energy", "cleansky": "CleanSky Energy",
    "taraenergy": "Tara Energy", "tara": "Tara Energy",
    "apge": "APG&E", "reliant": "Reliant Energy", "reliantenergy": "Reliant Energy",
    "cirro": "Cirro Energy", "cirroenergy": "Cirro Energy",
    "pennywise": "Pennywise Energy", "pennywiseenergy": "Pennywise Energy",
}


def canon_supplier(name: Optional[str]) -> str:
    raw = (name or "").strip()
    if not raw:
        return "Unknown"
    key = re.sub(r"[^a-z0-9]", "", raw.lower())
    return _SUPPLIER_ALIASES.get(key, raw.title() if raw.isupper() else raw)


def _median(xs: list) -> float:
    xs = sorted(xs)
    n = len(xs)
    if not n:
        return 0.0
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2


def _shift_month(d: date, n: int) -> date:
    y, m = d.year, d.month + n
    y += (m - 1) // 12
    m = (m - 1) % 12 + 1
    return date(y, m, 1)


def _build_revenue_forecast(db) -> dict:
    today = date.today()
    this_month = today.replace(day=1)
    cutoff = _shift_month(this_month, FORECAST_MONTHS)
    window_floor = _shift_month(this_month, -14).isoformat()   # 14 months back covers every calendar month once
    rate_floor = _shift_month(this_month, -6).isoformat()
    DEFAULT_RES_KWH, DEFAULT_COM_KWH = 1100.0, 2500.0

    # ── 1. Statement history: per meter per month ───────────────────────────
    meter: dict = {}            # es -> {month(YYYY-MM): [kwh, amount]}
    stmt_supplier: dict = {}    # es -> (month, canonical supplier of latest statement)
    month_rows: dict = {}       # month -> row count (to spot partially imported months)
    off = 0
    while True:
        page = db.table("actual_commissions") \
            .select("raw_esiid, raw_kwh, raw_amount, billing_month, suppliers(name)") \
            .gte("billing_month", window_floor).order("id").range(off, off + 999).execute().data or []
        for r in page:
            es = re.sub(r"\D", "", r.get("raw_esiid") or "")
            m = (r.get("billing_month") or "")[:7]
            if not es or not m:
                continue
            month_rows[m] = month_rows.get(m, 0) + 1
            cell = meter.setdefault(es, {}).setdefault(m, [0.0, 0.0])
            cell[0] += float(r.get("raw_kwh") or 0)
            cell[1] += float(r.get("raw_amount") or 0)
            sup = canon_supplier((r.get("suppliers") or {}).get("name"))
            if m > stmt_supplier.get(es, ("", ""))[0]:
                stmt_supplier[es] = (m, sup)
        if len(page) < 1000:
            break
        off += 1000

    # A month with under half the typical row count is still being imported —
    # keep its rows for meters, but not for the seasonal curve.
    typical = _median(list(month_rows.values())) if month_rows else 0
    complete_months = {m for m, n in month_rows.items() if typical and n >= 0.5 * typical}

    # ── 2. Seasonal index by calendar month (same-meter ratios) ─────────────
    def _ratios(min_months: int) -> dict:
        acc: dict = {i: [] for i in range(1, 13)}
        for es, months in meter.items():
            pts = [(m, v[0]) for m, v in months.items() if m in complete_months and v[0] > 0]
            if len(pts) < min_months:
                continue
            mean = sum(k for _, k in pts) / len(pts)
            for m, k in pts:
                acc[int(m[5:7])].append(k / mean)
        return acc

    ratios = _ratios(11)
    seasonal_meters = min((len(v) for v in ratios.values()), default=0)
    if seasonal_meters < 100:
        ratios = _ratios(8)
        seasonal_meters = min((len(v) for v in ratios.values()), default=0)
    index = {i: (_median(v) if len(v) >= 20 else 1.0) for i, v in ratios.items()}
    mean_idx = sum(index.values()) / 12
    index = {i: v / mean_idx for i, v in index.items()} if mean_idx else {i: 1.0 for i in index}

    # ── 3. Per-meter baseline kWh (deseasonalized) and observed paid rate ──
    base: dict = {}
    meter_rate: dict = {}
    usage_latest: dict = {}
    usage_latest_month: dict = {}
    for es, months in meter.items():
        recent = [(m, v) for m, v in months.items() if m >= window_floor[:7] and v[0] > 0]
        if recent:
            base[es] = sum(v[0] / index[int(m[5:7])] for m, v in recent) / len(recent)
            lm = max(recent, key=lambda t: t[0])
            usage_latest[es], usage_latest_month[es] = lm[1][0], lm[0]
        rates = [v[1] / v[0] for m, v in months.items()
                 if m >= rate_floor[:7] and v[0] > 0 and v[1] > 0 and 0.0005 <= v[1] / v[0] <= 0.05]
        if rates:
            meter_rate[es] = _median(rates)

    # ── 4. Active deals ────────────────────────────────────────────────────
    deals: list = []
    ld_offset = 0
    while True:
        rows = db.table("lead_deals").select("esiid, est_kwh, adder, end_date, supplier, status") \
            .eq("status", "Active").range(ld_offset, ld_offset + 999).execute().data or []
        for d in rows:
            deals.append({"es": re.sub(r"\D", "", d.get("esiid") or ""), "adder": float(d.get("adder") or 0),
                          "end": (d.get("end_date") or "")[:10], "supplier": d.get("supplier"),
                          "fallback_kwh": float(d.get("est_kwh") or 0) or DEFAULT_RES_KWH})
        if len(rows) < 1000:
            break
        ld_offset += 1000
    cd_offset = 0
    while True:
        rows = db.table("crm_deals").select("esiid, adder, meter_type, contract_end_date, provider, deal_status") \
            .eq("deal_status", "ACTIVE").range(cd_offset, cd_offset + 999).execute().data or []
        for d in rows:
            com = "commercial" in (d.get("meter_type") or "").lower()
            deals.append({"es": re.sub(r"\D", "", d.get("esiid") or ""), "adder": float(d.get("adder") or 0),
                          "end": (d.get("contract_end_date") or "")[:10], "supplier": d.get("provider"),
                          "fallback_kwh": DEFAULT_COM_KWH if com else DEFAULT_RES_KWH})
        if len(rows) < 1000:
            break
        cd_offset += 1000

    def _deal_supplier(d: dict) -> str:
        st = stmt_supplier.get(d["es"])
        return st[1] if st else canon_supplier(d["supplier"])

    # ── 5. Supplier realization: observed paid rate vs contract adder ───────
    real_acc: dict = {}
    for d in deals:
        r = meter_rate.get(d["es"])
        if r and 0.0005 <= d["adder"] <= 0.05:
            real_acc.setdefault(_deal_supplier(d), []).append(r / d["adder"])
    realization = {s: max(0.5, min(1.1, _median(v))) for s, v in real_acc.items() if len(v) >= 10}

    # ── 6. Project ─────────────────────────────────────────────────────────
    monthly: dict = {}
    by_supplier: dict = {}
    contributing = usage_based = skipped = 0
    rate_sources = {"meter_observed": 0, "contract_adjusted": 0, "contract": 0}
    not_projected = {"expired_or_month_to_month": 0, "missing_adder": 0, "missing_end_date": 0, "suspect_adder": 0}
    active_esiids: set = set()

    for d in deals:
        es = d["es"]
        if es:
            active_esiids.add(es)
        observed = meter_rate.get(es)
        adder = d["adder"]
        if not observed:
            if not adder:
                not_projected["missing_adder"] += 1; skipped += 1; continue
            if adder > 0.05 or adder < 0.0005:
                not_projected["suspect_adder"] += 1; skipped += 1; continue
        if not d["end"]:
            not_projected["missing_end_date"] += 1; skipped += 1; continue
        try:
            end_d = datetime.strptime(d["end"], "%Y-%m-%d").date()
        except Exception:
            skipped += 1; continue
        if end_d <= today:
            not_projected["expired_or_month_to_month"] += 1; skipped += 1; continue

        supplier = _deal_supplier(d)
        if observed:
            rate = observed; rate_sources["meter_observed"] += 1
        elif supplier in realization:
            rate = adder * realization[supplier]; rate_sources["contract_adjusted"] += 1
        else:
            rate = adder; rate_sources["contract"] += 1
        kwh_base = base.get(es)
        if kwh_base:
            usage_based += 1
        else:
            kwh_base = d["fallback_kwh"]
        if not kwh_base or not rate:
            skipped += 1; continue

        contributing += 1
        cur = this_month
        while cur <= end_d and cur < cutoff:
            amt = kwh_base * index[cur.month] * rate
            key = cur.strftime("%Y-%m")
            monthly[key] = monthly.get(key, 0.0) + amt
            by_supplier[supplier] = by_supplier.get(supplier, 0.0) + amt
            cur = _shift_month(cur, 1)

    sorted_months = sorted(monthly.keys())
    total = sum(monthly.values())
    usage_esiids = active_esiids & usage_latest.keys()
    actual_usage_kwh_mo = round(sum(usage_latest[es] for es in usage_esiids))
    latest_statement_month = max((usage_latest_month[es] for es in usage_esiids), default=None)
    MON = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    return {
        "monthly": [{"month": m, "amount": round(monthly[m], 2)} for m in sorted_months],
        "by_supplier": [{"supplier": k, "amount": round(v, 2)} for k, v in sorted(by_supplier.items(), key=lambda x: -x[1])],
        "total_projected": round(total, 2),
        "avg_monthly": round(total / len(monthly), 2) if monthly else 0,
        "next_12_total": round(sum(monthly[m] for m in sorted_months[:12]), 2),
        "contributing_deals": contributing,
        "usage_based_deals": usage_based,
        "total_in_report": contributing + skipped,
        "not_projected": not_projected,
        "months_out": len(sorted_months),
        "horizon_months": FORECAST_MONTHS,
        "rate_sources": rate_sources,
        "realization": {k: round(v, 3) for k, v in sorted(realization.items())},
        "seasonality": [{"month": MON[i - 1], "index": round(index[i], 3)} for i in range(1, 13)],
        "seasonality_meters": seasonal_meters,
        "statement_months_used": sorted(complete_months),
        "actual_usage_kwh_mo": actual_usage_kwh_mo,
        "actual_usage_kwh_yr": actual_usage_kwh_mo * 12,
        "actual_usage_accounts": len(usage_esiids),
        "active_accounts_total": len(active_esiids),
        "latest_statement_month": (latest_statement_month or "")[:7] or None,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


_forecast_lock = threading.Lock()


def _refresh_forecast_cache() -> None:
    """Rebuild the forecast cache; no-op if a rebuild is already running."""
    if not _forecast_lock.acquire(blocking=False):
        return
    try:
        data = _build_revenue_forecast(get_client())
        _FORECAST_CACHE["at"], _FORECAST_CACHE["data"] = datetime.now(timezone.utc), data
    except Exception:
        pass
    finally:
        _forecast_lock.release()


def warm_revenue_forecast() -> None:
    """Called at app startup so the first visit doesn't pay the build cost."""
    threading.Thread(target=_refresh_forecast_cache, daemon=True).start()


@router.get("/revenue-forecast")
def get_revenue_forecast(refresh: bool = Query(False), user: UserContext = Depends(get_current_user)):
    now = datetime.now(timezone.utc)
    c = _FORECAST_CACHE
    fresh = c["data"] is not None and c["at"] and now - c["at"] < _FORECAST_TTL
    if fresh and not refresh:
        return c["data"]
    if c["data"] is not None and not refresh:
        # stale: serve what we have, rebuild in the background
        threading.Thread(target=_refresh_forecast_cache, daemon=True).start()
        return c["data"]
    _refresh_forecast_cache()
    if c["data"] is None:
        raise HTTPException(status_code=503, detail="Forecast could not be built — try again shortly.")
    return c["data"]


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
