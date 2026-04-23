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

@router.get("/overview")
def get_overview(billing_month: Optional[str] = Query(None), user: UserContext = Depends(require_manager)):
    db = get_client()
    month = billing_month or get_latest_month(db)

    total_actual = sum_all(db, "actual_commissions", "raw_amount", {"billing_month": month})
    total_expected = sum_all(db, "expected_commissions", "expected_amount", {"billing_month": month})
    unresolved = db.table("reconciliation_items").select("id", count="exact").eq("is_resolved", False).execute()
    pending_uploads = db.table("upload_batches").select("id", count="exact").in_("status", ["pending", "review"]).execute()

    return {
        "billing_month": month,
        "total_expected": round(total_expected, 2),
        "total_actual": round(total_actual, 2),
        "net_discrepancy": round(total_actual - total_expected, 2),
        "unresolved_discrepancies": unresolved.count or 0,
        "pending_uploads": pending_uploads.count or 0,
    }

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
        leads_count     = db.table("leads").select("id", count="exact").eq("status", "lead").eq("sales_agent", agent_name).execute()
        converted_count = db.table("leads").select("id", count="exact").eq("status", "converted").eq("sales_agent", agent_name).execute()
        recent_raw      = db.table("leads").select("*, lead_deals(id, status, product_type)").eq("sales_agent", agent_name).order("created_at", desc=True).limit(5).execute()
    else:
        leads_today     = db.table("leads").select("id", count="exact").gte("created_at", today_start).execute()
        leads_week      = db.table("leads").select("id", count="exact").gte("created_at", week_start).execute()
        active_deals    = db.table("lead_deals").select("id, est_kwh, adder", count="exact").eq("status", "Active").execute()
        expiring        = db.table("lead_deals").select("id", count="exact").eq("status", "Active").lte("end_date", thirty_days_out).gte("end_date", today_str).execute()
        leads_count     = db.table("leads").select("id", count="exact").eq("status", "lead").execute()
        converted_count = db.table("leads").select("id", count="exact").eq("status", "converted").execute()
        recent_raw      = db.table("leads").select("*, lead_deals(id, status, product_type)").order("created_at", desc=True).limit(5).execute()

    pipeline = {"lead": leads_count.count or 0, "converted": converted_count.count or 0}
    total_kwh     = sum((r.get("est_kwh") or 0) for r in active_deals.data)
    commission_mo = sum((r.get("est_kwh") or 0) * (r.get("adder") or 0) for r in active_deals.data)

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
        "active_deals":    active_deals.count or 0,
        "expiring_soon":   expiring.count or 0,
        "pipeline":        pipeline,
        "portfolio": {
            "active_contracts": active_deals.count or 0,
            "total_kwh":        round(total_kwh, 2),
            "commission_mo":    round(commission_mo, 2),
            "at_risk":          expiring.count or 0,
        },
        "recent_leads": recent_leads,
    }

@router.get("/expiring-deals")
def get_expiring_deals(user: UserContext = Depends(get_current_user)):
    db = get_client()
    today = datetime.now(timezone.utc).date()
    thirty_out = (today + timedelta(days=30)).isoformat()
    today_str = today.isoformat()

    q = db.table("lead_deals").select(
        "id, end_date, supplier, plan_name, contract_term, lead_id, leads(first_name, last_name, phone, sgp_customer_id, sales_agent)"
    ).eq("status", "Active").lte("end_date", thirty_out).gte("end_date", today_str).order("end_date")

    res = q.execute()

    agent_name = None
    if user.is_sales_agent:
        u = db.table("users").select("sales_agent_name").eq("id", user.user_id).limit(1).execute()
        agent_name = (u.data[0].get("sales_agent_name") or "").strip() or None
        if not agent_name:
            return []
    results = []
    for d in res.data:
        lead = d.pop("leads", None) or {}
        if agent_name and (lead.get("sales_agent") or "").lower() != agent_name.lower():
            continue
        end = d.get("end_date")
        days_left = (date.fromisoformat(end) - today).days if end else None
        results.append({
            "deal_id":        d["id"],
            "lead_id":        d.get("lead_id"),
            "sgp_customer_id": lead.get("sgp_customer_id"),
            "full_name":      f"{lead.get('first_name','')} {lead.get('last_name','')}".strip(),
            "phone":          lead.get("phone"),
            "supplier":       d.get("supplier"),
            "plan_name":      d.get("plan_name"),
            "contract_term":  d.get("contract_term"),
            "end_date":       end,
            "days_left":      days_left,
        })
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

    # ── Fetch all rows from actual_commissions (paginate past 1000 limit) ────
    all_rows = []
    offset = 0
    while True:
        res = db.table("actual_commissions").select(
            "raw_esiid, raw_kwh, raw_rate, billing_month, raw_row_data, suppliers(name)"
        ).range(offset, offset + 999).execute()
        if not res.data:
            break
        all_rows.extend(res.data)
        if len(res.data) < 1000:
            break
        offset += 1000

    # Keep latest billing_month record per ESIID
    best: dict = {}
    for r in all_rows:
        esiid = r.get("raw_esiid") or ""
        if not esiid:
            continue
        existing = best.get(esiid)
        if not existing or (r.get("billing_month") or "") > (existing.get("billing_month") or ""):
            best[esiid] = r

    # Build ESIID → contract_end_date fallback from crm_deals
    crm_end: dict = {}
    offset2 = 0
    while True:
        res2 = db.table("crm_deals").select("esiid, contract_end_date").not_.is_("esiid", "null").not_.is_("contract_end_date", "null").range(offset2, offset2 + 999).execute()
        if not res2.data:
            break
        for row in res2.data:
            esiid = (row.get("esiid") or "").strip()
            if esiid and row.get("contract_end_date"):
                crm_end[esiid] = row["contract_end_date"]
        if len(res2.data) < 1000:
            break
        offset2 += 1000

    monthly: dict = {}
    by_supplier: dict = {}
    contributing = 0

    for esiid, r in best.items():
        kwh   = r.get("raw_kwh") or 0
        adder = r.get("raw_rate") or 0
        if not kwh or not adder:
            continue

        # Try raw_row_data first, fall back to crm_deals
        rd = r.get("raw_row_data") or {}
        end_raw = rd.get("contract_end") or crm_end.get(esiid.strip()) or ""
        if not end_raw:
            continue

        try:
            end_d = datetime.strptime(end_raw[:10], "%Y-%m-%d").date()
        except Exception:
            continue

        if end_d <= today:
            continue

        commission_mo = round(kwh * adder, 4)
        supplier = (r.get("suppliers") or {}).get("name") or "Unknown"
        contributing += 1

        cur = today.replace(day=1)
        while cur <= end_d and cur < cutoff:
            key = cur.strftime("%Y-%m")
            monthly[key]          = monthly.get(key, 0) + commission_mo
            by_supplier[supplier] = by_supplier.get(supplier, 0) + commission_mo
            cur = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)

    sorted_months = sorted(monthly.keys())
    total = sum(monthly.values())
    return {
        "monthly": [{"month": m, "amount": round(monthly[m], 2)} for m in sorted_months],
        "by_supplier": [{"supplier": k, "amount": round(v, 2)} for k, v in sorted(by_supplier.items(), key=lambda x: -x[1])],
        "total_projected": round(total, 2),
        "avg_monthly": round(total / len(monthly), 2) if monthly else 0,
        "contributing_deals": contributing,
        "total_in_report": len(all_rows),
        "months_out": len(sorted_months),
    }

@router.get("/supplier-breakdown")
def supplier_breakdown(billing_month: Optional[str] = Query(None), user: UserContext = Depends(get_current_user)):
    db = get_client()
    month = billing_month or get_latest_month(db)

    suppliers = db.table("suppliers").select("id, name, code").eq("is_active", True).execute()
    result = []
    for s in suppliers.data:
        total_act = sum_all(db, "actual_commissions", "raw_amount", {"supplier_id": s["id"], "billing_month": month})
        total_exp = sum_all(db, "expected_commissions", "expected_amount", {"supplier_id": s["id"], "billing_month": month})
        if total_exp > 0 or total_act > 0:
            result.append({
                "supplier_id": s["id"],
                "supplier_name": s["name"],
                "supplier_code": s["code"],
                "expected": round(total_exp, 2),
                "actual": round(total_act, 2),
                "discrepancy": round(total_act - total_exp, 2),
            })
    return result
