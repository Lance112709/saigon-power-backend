"""
AI Operations Agent — Saigon Power CRM
Deterministic data auditor + business intelligence engine.
"""
from datetime import datetime, timezone, timedelta
from typing import Optional
import os
from app.db.client import get_client

# ── Helpers ────────────────────────────────────────────────────────────────────

def _now() -> datetime:
    return datetime.now(timezone.utc)

def _today() -> str:
    return _now().date().isoformat()

def _days_until(date_str: Optional[str]) -> Optional[int]:
    if not date_str:
        return None
    try:
        d = datetime.strptime(date_str[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return (d - _now()).days
    except Exception:
        return None

def _days_ago(date_str: Optional[str]) -> Optional[int]:
    if not date_str:
        return None
    try:
        d = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return (_now() - d).days
    except Exception:
        return None

# ── Alert Engine ────────────────────────────────────────────────────────────────

def _open_alert_key(db, alert_type: str, entity_id: str) -> bool:
    """Return True if an open alert already exists for this type+entity."""
    res = db.table("ai_alerts").select("id").eq("type", alert_type).eq("entity_id", str(entity_id)).eq("status", "open").execute()
    return bool(res.data)

def _create_alert(db, alert_type: str, entity_type: str, entity_id: str,
                  message: str, severity: str, user_id: Optional[str] = None,
                  metadata: Optional[dict] = None):
    if _open_alert_key(db, alert_type, entity_id):
        return
    db.table("ai_alerts").insert({
        "type": alert_type,
        "entity_type": entity_type,
        "entity_id": str(entity_id),
        "user_id": user_id,
        "message": message,
        "severity": severity,
        "status": "open",
        "metadata": metadata or {},
    }).execute()

def _resolve_alert(db, alert_type: str, entity_id: str):
    db.table("ai_alerts").update({
        "status": "resolved",
        "updated_at": _now().isoformat(),
    }).eq("type", alert_type).eq("entity_id", str(entity_id)).eq("status", "open").execute()

# ── Data Quality Scan ──────────────────────────────────────────────────────────

def scan_lead_deals(db) -> dict:
    """Scan lead_deals for missing critical fields."""
    deals = db.table("lead_deals").select("*").neq("status", "Inactive").execute().data or []
    issues = {"missing_rate": 0, "missing_esiid": 0, "missing_agent": 0,
              "missing_dates": 0, "missing_rate_ids": [], "total_scanned": len(deals)}

    for d in deals:
        did = d["id"]
        lead_id = d.get("lead_id", "")
        supplier = d.get("supplier") or "Unknown Supplier"
        esiid = str(d.get("esiid") or "").strip()
        rate = d.get("rate")
        agent = str(d.get("sales_agent") or "").strip()
        start = d.get("start_date")
        end = d.get("end_date")

        # Missing rate
        if rate is None or float(rate) <= 0:
            issues["missing_rate"] += 1
            issues["missing_rate_ids"].append(did)
            _create_alert(db, "missing_rate", "deal", did,
                f"Deal ({supplier}) is missing a valid rate. This will impact commission calculations.",
                "high", metadata={"lead_id": lead_id})
        else:
            _resolve_alert(db, "missing_rate", did)

        # Missing ESIID (only relevant for active deals)
        if d.get("status") == "Active" and not esiid:
            issues["missing_esiid"] += 1
            _create_alert(db, "missing_esiid", "deal", did,
                f"Active deal ({supplier}) is missing ESIID. Required for commission reconciliation.",
                "high", metadata={"lead_id": lead_id})
        elif esiid:
            _resolve_alert(db, "missing_esiid", did)

        # Missing agent
        if not agent:
            issues["missing_agent"] += 1
            _create_alert(db, "missing_agent", "deal", did,
                f"Deal ({supplier}) has no assigned sales agent.",
                "medium", metadata={"lead_id": lead_id})
        else:
            _resolve_alert(db, "missing_agent", did)

        # Missing dates on active deals
        if d.get("status") == "Active" and (not start or not end):
            issues["missing_dates"] += 1
            _create_alert(db, "missing_dates", "deal", did,
                f"Active deal ({supplier}) is missing start or end date.",
                "medium", metadata={"lead_id": lead_id})
        elif start and end:
            _resolve_alert(db, "missing_dates", did)

    return issues

def scan_renewals(db) -> dict:
    """Flag deals expiring within 90 days."""
    deals = db.table("lead_deals").select("id, lead_id, supplier, end_date, sales_agent").eq("status", "Active").execute().data or []
    r = {"30_days": 0, "60_days": 0, "90_days": 0}

    for d in deals:
        days = _days_until(d.get("end_date"))
        if days is None:
            continue
        did = d["id"]
        supplier = d.get("supplier") or "Unknown"
        if days <= 30:
            r["30_days"] += 1
            _create_alert(db, "renewal_30", "deal", did,
                f"URGENT: Deal ({supplier}) expires in {days} day(s). Immediate renewal action required.",
                "high", metadata={"lead_id": d.get("lead_id"), "days_until": days})
        elif days <= 60:
            r["60_days"] += 1
            _create_alert(db, "renewal_60", "deal", did,
                f"Deal ({supplier}) expires in {days} days. Begin renewal conversation.",
                "medium", metadata={"lead_id": d.get("lead_id"), "days_until": days})
        elif days <= 90:
            r["90_days"] += 1
            _create_alert(db, "renewal_90", "deal", did,
                f"Deal ({supplier}) expires in {days} days. Start renewal planning.",
                "low", metadata={"lead_id": d.get("lead_id"), "days_until": days})

    return r

def scan_inactive_leads(db) -> int:
    """Flag leads with no activity in 14+ days."""
    leads = db.table("leads").select("id, first_name, last_name, created_at").eq("status", "lead").execute().data or []
    flagged = 0
    lead_ids = [l["id"] for l in leads]
    if not lead_ids:
        return 0

    recent_tasks = db.table("tasks").select("lead_id").in_("lead_id", lead_ids).eq("status", "completed").execute().data or []
    active_lead_ids = {t["lead_id"] for t in recent_tasks}

    for lead in leads:
        lid = lead["id"]
        age = _days_ago(lead.get("created_at")) or 0
        if age >= 14 and lid not in active_lead_ids:
            flagged += 1
            name = f"{lead.get('first_name','')} {lead.get('last_name','')}".strip()
            _create_alert(db, "inactive_lead", "lead", lid,
                f"Lead '{name}' has had no activity for {age} days.",
                "low")
        elif lid in active_lead_ids:
            _resolve_alert(db, "inactive_lead", lid)

    return flagged

def scan_duplicate_leads(db) -> int:
    """Detect leads with same name + address."""
    leads = db.table("leads").select("id, first_name, last_name, address").execute().data or []
    seen: dict = {}
    dupes = 0
    for l in leads:
        key = f"{(l.get('first_name') or '').strip().lower()}|{(l.get('last_name') or '').strip().lower()}|{(l.get('address') or '').strip().lower()[:30]}"
        if key in seen:
            dupes += 1
            _create_alert(db, "duplicate_lead", "lead", l["id"],
                f"Possible duplicate lead: '{l.get('first_name','')} {l.get('last_name','')}' at '{l.get('address','')}'",
                "medium", metadata={"original_id": seen[key]})
        else:
            seen[key] = l["id"]
    return dupes

def run_full_scan() -> dict:
    """Run all scans and return summary."""
    db = get_client()
    deal_issues = scan_lead_deals(db)
    renewals    = scan_renewals(db)
    inactive    = scan_inactive_leads(db)
    dupes       = scan_duplicate_leads(db)
    return {
        "scanned_at": _now().isoformat(),
        "deal_issues": deal_issues,
        "renewals": renewals,
        "inactive_leads": inactive,
        "duplicate_leads": dupes,
    }

# ── Metrics ────────────────────────────────────────────────────────────────────

def get_daily_metrics(db) -> dict:
    today_start = _now().replace(hour=0, minute=0, second=0, microsecond=0)
    today_iso = today_start.isoformat()
    month_start = today_start.replace(day=1).isoformat()

    new_leads    = db.table("leads").select("id", count="exact").gte("created_at", today_iso).execute().count or 0
    new_deals    = db.table("lead_deals").select("id", count="exact").gte("created_at", today_iso).execute().count or 0
    active_deals = db.table("lead_deals").select("id", count="exact").eq("status", "Active").execute().count or 0
    total_leads  = db.table("leads").select("id", count="exact").execute().count or 0

    today_str = _today()

    # Expired but still ACTIVE — lead_deals
    expired_lead = db.table("lead_deals").select("id", count="exact") \
        .eq("status", "Active").lt("end_date", today_str).execute().count or 0

    # Expired but still ACTIVE — crm_deals
    expired_crm = db.table("crm_deals").select("id", count="exact") \
        .eq("deal_status", "ACTIVE").lt("contract_end_date", today_str).execute().count or 0

    expired_active = expired_lead + expired_crm

    # Revenue at risk — lead_deals active, expiring within 60 days
    sixty_days = (_now() + __import__("datetime").timedelta(days=60)).date().isoformat()
    at_risk_rows = db.table("lead_deals").select("est_kwh, adder") \
        .eq("status", "Active").gte("end_date", today_str).lte("end_date", sixty_days).execute().data or []
    revenue_at_risk = sum(
        float(d.get("est_kwh") or 0) * float(d.get("adder") or 0)
        for d in at_risk_rows
    )
    deals_at_risk = len(at_risk_rows)

    # Duplicate deal detection (crm_deals)
    crm_deals_raw = db.table("crm_deals").select("esiid, service_address, customer_id").execute().data or []

    esiid_counts: dict = {}
    for d in crm_deals_raw:
        e = (d.get("esiid") or "").strip()
        if e:
            esiid_counts[e] = esiid_counts.get(e, 0) + 1
    dup_esiid = sum(1 for v in esiid_counts.values() if v > 1)

    addr_counts: dict = {}
    for d in crm_deals_raw:
        a = (d.get("service_address") or "").strip().upper()
        cust = d.get("customer_id") or ""
        if a:
            key = (cust, a)
            addr_counts[key] = addr_counts.get(key, 0) + 1
    dup_address = sum(1 for v in addr_counts.values() if v > 1)

    # Renewals
    r30 = db.table("ai_alerts").select("id", count="exact").eq("type", "renewal_30").eq("status", "open").execute().count or 0
    r60 = db.table("ai_alerts").select("id", count="exact").eq("type", "renewal_60").eq("status", "open").execute().count or 0
    r90 = db.table("ai_alerts").select("id", count="exact").eq("type", "renewal_90").eq("status", "open").execute().count or 0

    # Commission estimate (rate × est_kwh × adder for today's new active deals)
    new_active = db.table("lead_deals").select("est_kwh, adder").eq("status", "Active").gte("created_at", today_iso).execute().data or []
    est_commission_today = sum(
        float(d.get("est_kwh") or 0) * float(d.get("adder") or 0)
        for d in new_active
    )

    # Month-to-date
    mtd_leads = db.table("leads").select("id", count="exact").gte("created_at", month_start).execute().count or 0
    mtd_deals = db.table("lead_deals").select("id", count="exact").gte("created_at", month_start).execute().count or 0
    mtd_active = db.table("lead_deals").select("est_kwh, adder").eq("status", "Active").gte("created_at", month_start).execute().data or []
    mtd_commission = sum(
        float(d.get("est_kwh") or 0) * float(d.get("adder") or 0)
        for d in mtd_active
    )

    # Open critical alerts
    critical_alerts = db.table("ai_alerts").select("id", count="exact").eq("status", "open").eq("severity", "high").execute().count or 0
    total_open = db.table("ai_alerts").select("id", count="exact").eq("status", "open").execute().count or 0

    return {
        "date": _today(),
        "today": {
            "new_leads": new_leads,
            "new_deals": new_deals,
            "est_commission": round(est_commission_today, 2),
        },
        "pipeline": {
            "total_leads": total_leads,
            "active_deals": active_deals,
        },
        "data_quality": {
            "dup_esiid": dup_esiid,
            "dup_address": dup_address,
            "total_issues": dup_esiid + dup_address,
        },
        "health": {
            "expired_active": expired_active,
            "revenue_at_risk": round(revenue_at_risk, 2),
            "deals_at_risk": deals_at_risk,
        },
        "renewals": {"30_days": r30, "60_days": r60, "90_days": r90},
        "alerts": {"critical": critical_alerts, "total_open": total_open},
        "mtd": {
            "leads": mtd_leads,
            "deals": mtd_deals,
            "est_commission": round(mtd_commission, 2),
        },
    }

# ── AI Summary Generator ───────────────────────────────────────────────────────

def _template_summary(m: dict) -> str:
    t = m["today"]
    dq = m["data_quality"]
    r = m["renewals"]
    al = m["alerts"]
    parts = []

    parts.append(
        f"Today, {t['new_leads']} new lead{'s were' if t['new_leads'] != 1 else ' was'} added "
        f"and {t['new_deals']} new deal{'s were' if t['new_deals'] != 1 else ' was'} created."
    )
    if dq["total_issues"] > 0:
        parts.append(
            f"{dq['total_issues']} deal{'s are' if dq['total_issues'] != 1 else ' is'} missing critical data "
            f"({dq['missing_rate']} missing rate, {dq['missing_esiid']} missing ESIID, {dq['missing_agent']} unassigned)."
        )
    if r["30_days"] > 0:
        parts.append(f"⚠️ {r['30_days']} deal{'s' if r['30_days'] != 1 else ''} expiring within 30 days — urgent renewal action needed.")
    if r["60_days"] + r["90_days"] > 0:
        parts.append(f"{r['60_days'] + r['90_days']} more deal{'s' if r['60_days']+r['90_days'] != 1 else ''} expiring in 30–90 days.")
    if t["est_commission"] > 0:
        parts.append(f"Estimated commission added today: ${t['est_commission']:,.2f}.")
    if al["critical"] > 0:
        parts.append(f"{al['critical']} critical alert{'s' if al['critical'] != 1 else ''} require immediate attention.")

    return " ".join(parts) if parts else "All systems operating normally. No critical issues detected."

def generate_ai_summary(metrics: dict) -> str:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return _template_summary(metrics)
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            messages=[{"role": "user", "content": f"""You are a business intelligence assistant for Saigon Power LLC, an energy broker in Texas.
Write a concise 2-3 sentence daily operations summary from these metrics. Be specific with numbers. Lead with the most critical issue.
Metrics: {metrics}"""}]
        )
        return msg.content[0].text.strip()
    except Exception:
        return _template_summary(metrics)

# ── Recommendations ────────────────────────────────────────────────────────────

def get_recommendations(db) -> list[dict]:
    recs = []

    unassigned = db.table("ai_alerts").select("id", count="exact").eq("type", "missing_agent").eq("status", "open").execute().count or 0
    if unassigned > 0:
        recs.append({"icon": "👤", "priority": "high", "text": f"Assign a sales agent to {unassigned} unassigned deal{'s' if unassigned != 1 else ''}."})

    no_rate = db.table("ai_alerts").select("id", count="exact").eq("type", "missing_rate").eq("status", "open").execute().count or 0
    if no_rate > 0:
        recs.append({"icon": "💲", "priority": "high", "text": f"Add missing rate to {no_rate} deal{'s' if no_rate != 1 else ''} to ensure accurate commission tracking."})

    no_esiid = db.table("ai_alerts").select("id", count="exact").eq("type", "missing_esiid").eq("status", "open").execute().count or 0
    if no_esiid > 0:
        recs.append({"icon": "🔌", "priority": "high", "text": f"Enter ESIID for {no_esiid} active deal{'s' if no_esiid != 1 else ''} to enable reconciliation."})

    inactive = db.table("ai_alerts").select("id", count="exact").eq("type", "inactive_lead").eq("status", "open").execute().count or 0
    if inactive > 0:
        recs.append({"icon": "📞", "priority": "medium", "text": f"Follow up with {inactive} lead{'s' if inactive != 1 else ''} that {'have' if inactive != 1 else 'has'} had no activity in 14+ days."})

    r30 = db.table("ai_alerts").select("id", count="exact").eq("type", "renewal_30").eq("status", "open").execute().count or 0
    if r30 > 0:
        recs.append({"icon": "⏰", "priority": "high", "text": f"Renew {r30} contract{'s' if r30 != 1 else ''} expiring within 30 days immediately."})

    dupes = db.table("ai_alerts").select("id", count="exact").eq("type", "duplicate_lead").eq("status", "open").execute().count or 0
    if dupes > 0:
        recs.append({"icon": "🔁", "priority": "medium", "text": f"Review {dupes} possible duplicate lead{'s' if dupes != 1 else ''} to keep data clean."})

    if not recs:
        recs.append({"icon": "✅", "priority": "low", "text": "All data looks clean. No immediate actions required."})

    return recs

# ── Daily Report ───────────────────────────────────────────────────────────────

def generate_daily_report() -> dict:
    db = get_client()
    run_full_scan()
    metrics  = get_daily_metrics(db)
    summary  = generate_ai_summary(metrics)
    recs     = get_recommendations(db)

    report = {
        "report_type": "daily",
        "report_date": _today(),
        "data": {**metrics, "recommendations": recs},
        "summary": summary,
    }

    # Upsert into ai_reports
    existing = db.table("ai_reports").select("id").eq("report_type", "daily").eq("report_date", _today()).execute()
    if existing.data:
        db.table("ai_reports").update({"data": report["data"], "summary": summary}).eq("id", existing.data[0]["id"]).execute()
    else:
        db.table("ai_reports").insert(report).execute()

    return report

# ── Monthly Report ─────────────────────────────────────────────────────────────

def generate_monthly_report() -> dict:
    db = get_client()
    today = _now()
    month_start = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    prev_month_start = (month_start - timedelta(days=1)).replace(day=1)
    month_iso = month_start.isoformat()
    prev_iso  = prev_month_start.isoformat()
    report_date = month_start.date().isoformat()

    # This month
    mtd_leads = db.table("leads").select("id", count="exact").gte("created_at", month_iso).execute().count or 0
    mtd_deals = db.table("lead_deals").select("id", count="exact").gte("created_at", month_iso).execute().count or 0
    mtd_active = db.table("lead_deals").select("id", count="exact").eq("status", "Active").gte("created_at", month_iso).execute().count or 0
    mtd_commission_rows = db.table("lead_deals").select("est_kwh, adder").eq("status", "Active").gte("created_at", month_iso).execute().data or []
    mtd_commission = sum(float(d.get("est_kwh") or 0) * float(d.get("adder") or 0) for d in mtd_commission_rows)

    # Previous month
    prev_leads = db.table("leads").select("id", count="exact").gte("created_at", prev_iso).lt("created_at", month_iso).execute().count or 0
    prev_deals = db.table("lead_deals").select("id", count="exact").gte("created_at", prev_iso).lt("created_at", month_iso).execute().count or 0
    prev_commission_rows = db.table("lead_deals").select("est_kwh, adder").eq("status", "Active").gte("created_at", prev_iso).lt("created_at", month_iso).execute().data or []
    prev_commission = sum(float(d.get("est_kwh") or 0) * float(d.get("adder") or 0) for d in prev_commission_rows)

    conversion_rate = round((mtd_active / mtd_leads * 100) if mtd_leads > 0 else 0, 1)
    commission_growth = round(((mtd_commission - prev_commission) / prev_commission * 100) if prev_commission > 0 else 0, 1)

    # Risk: open high-severity alerts
    critical = db.table("ai_alerts").select("*").eq("status", "open").eq("severity", "high").execute().data or []
    at_risk = db.table("ai_alerts").select("id", count="exact").eq("type", "missing_rate").eq("status", "open").execute().count or 0

    # Renewal pipeline value
    r30 = db.table("ai_alerts").select("id", count="exact").eq("type", "renewal_30").eq("status", "open").execute().count or 0
    r60 = db.table("ai_alerts").select("id", count="exact").eq("type", "renewal_60").eq("status", "open").execute().count or 0

    data = {
        "month": month_start.strftime("%B %Y"),
        "performance": {
            "total_leads": mtd_leads,
            "total_deals": mtd_deals,
            "active_deals": mtd_active,
            "conversion_rate": conversion_rate,
            "vs_prev_leads": mtd_leads - prev_leads,
            "vs_prev_deals": mtd_deals - prev_deals,
        },
        "revenue": {
            "est_commission": round(mtd_commission, 2),
            "prev_commission": round(prev_commission, 2),
            "commission_growth_pct": commission_growth,
        },
        "risk": {
            "deals_missing_rate": at_risk,
            "critical_alerts": len(critical),
            "renewals_30": r30,
            "renewals_60": r60,
        },
    }

    summary_parts = [
        f"This month, Saigon Power generated {mtd_leads} new leads and {mtd_deals} deals with a {conversion_rate}% conversion rate.",
        f"Estimated commission is ${mtd_commission:,.2f}" + (f", {'up' if commission_growth >= 0 else 'down'} {abs(commission_growth)}% from last month." if prev_commission > 0 else "."),
    ]
    if at_risk > 0:
        summary_parts.append(f"⚠️ {at_risk} deal{'s are' if at_risk != 1 else ' is'} missing rate data — revenue tracking may be impacted.")
    if r30 > 0:
        summary_parts.append(f"{r30} contract{'s' if r30 != 1 else ''} expiring within 30 days need immediate renewal attention.")
    summary = " ".join(summary_parts)

    report = {"report_type": "monthly", "report_date": report_date, "data": data, "summary": summary}
    existing = db.table("ai_reports").select("id").eq("report_type", "monthly").eq("report_date", report_date).execute()
    if existing.data:
        db.table("ai_reports").update({"data": data, "summary": summary}).eq("id", existing.data[0]["id"]).execute()
    else:
        db.table("ai_reports").insert(report).execute()

    return report

# ── Full Dashboard ─────────────────────────────────────────────────────────────

def get_dashboard() -> dict:
    db = get_client()

    # Run scan to refresh alerts
    run_full_scan()

    metrics = get_daily_metrics(db)
    summary = generate_ai_summary(metrics)
    recs    = get_recommendations(db)

    # Recent open alerts grouped by severity
    alerts = db.table("ai_alerts").select("*").eq("status", "open").order("created_at", desc=True).limit(50).execute().data or []
    critical = [a for a in alerts if a["severity"] == "high"]
    warnings = [a for a in alerts if a["severity"] == "medium"]
    info     = [a for a in alerts if a["severity"] == "low"]

    # Latest daily report
    latest_report = db.table("ai_reports").select("*").eq("report_type", "daily").order("report_date", desc=True).limit(1).execute().data
    daily_report = latest_report[0] if latest_report else None

    return {
        "summary": summary,
        "metrics": metrics,
        "alerts": {"critical": critical, "warnings": warnings, "info": info},
        "recommendations": recs,
        "daily_report": daily_report,
    }


# ── AI Chat (free, no API key required) ─────────────────────────────────────────

def _fmt_money(v) -> str:
    try:
        return f"${float(v):,.2f}"
    except Exception:
        return "$0.00"

def _days_until_str(date_str) -> str:
    d = _days_until(date_str)
    if d is None:
        return "unknown"
    if d < 0:
        return f"expired {abs(d)} days ago"
    if d == 0:
        return "today"
    return f"in {d} days"

def _section(title: str, lines: list) -> str:
    body = "\n".join(f"  • {l}" for l in lines) if lines else "  No data."
    return f"**{title}**\n{body}"


def chat_with_context(message: str, history: list) -> str:
    db = get_client()
    q = message.lower()

    # ── helpers ──
    def _want(*keywords):
        return any(k in q for k in keywords)

    parts = []

    # ── Full summary / overview / status ──
    if _want("summary", "overview", "status", "everything", "update", "report", "whats going on", "what's going on", "tell me about", "how are we", "how is"):
        # Leads
        leads = db.table("leads").select("id, status, created_at").execute().data or []
        by_status: dict = {}
        for l in leads:
            s = (l.get("status") or "unknown").title()
            by_status[s] = by_status.get(s, 0) + 1
        lead_lines = [f"{s}: {c}" for s, c in sorted(by_status.items(), key=lambda x: -x[1])]
        parts.append(_section(f"Leads — {len(leads)} total", lead_lines))

        # Deals
        deals = db.table("lead_deals").select("id, status, supplier, sales_agent, est_kwh, adder, end_date").execute().data or []
        active = [d for d in deals if (d.get("status") or "").lower() == "active"]
        future = [d for d in deals if (d.get("status") or "").lower() == "future"]
        est_rev = sum(float(d.get("est_kwh") or 0) * float(d.get("adder") or 0) for d in active)
        expiring_30 = [d for d in active if (_days_until(d.get("end_date")) or 999) <= 30]
        deal_lines = [
            f"Active: {len(active)}",
            f"Future/Pending: {len(future)}",
            f"Est. monthly commission: {_fmt_money(est_rev)}",
            f"Expiring within 30 days: {len(expiring_30)}",
        ]
        parts.append(_section("Deals", deal_lines))

        # Commissions
        comms = db.table("actual_commissions").select("billing_month, raw_amount").order("billing_month", desc=True).limit(500).execute().data or []
        by_month: dict = {}
        for c in comms:
            m = (c.get("billing_month") or "")[:7]
            if m:
                by_month[m] = by_month.get(m, 0.0) + (c.get("raw_amount") or 0.0)
        comm_lines = [f"{m}: {_fmt_money(amt)}" for m, amt in sorted(by_month.items(), reverse=True)[:4]]
        parts.append(_section("Commission Payments", comm_lines or ["No payments recorded yet."]))

        # Alerts
        alerts = db.table("ai_alerts").select("severity, message").eq("status", "open").order("severity").limit(10).execute().data or []
        critical = [a for a in alerts if a.get("severity") == "high"]
        alert_lines = [f"[URGENT] {a['message'][:100]}" for a in critical[:3]]
        if len(alerts) > 3:
            alert_lines.append(f"...and {len(alerts) - 3} more open alerts")
        parts.append(_section(f"Open Alerts — {len(alerts)} total ({len(critical)} urgent)", alert_lines or ["All clear."]))

        return "\n\n".join(parts)

    # ── Agents / sales team (must come before generic "deals" check) ──
    if _want("agent", "agents", "sales", "rep", "team", "staff", "top agent", "performing", "leaderboard", "who is", "who's"):
        agents = db.table("users").select("id, first_name, last_name, role").eq("role", "sales_agent").execute().data or []
        deals = db.table("lead_deals").select("sales_agent, status, est_kwh, adder").execute().data or []

        agent_map: dict = {}
        for d in deals:
            name = d.get("sales_agent") or "Unassigned"
            if name not in agent_map:
                agent_map[name] = {"active": 0, "total": 0, "est": 0.0}
            agent_map[name]["total"] += 1
            if (d.get("status") or "").lower() == "active":
                agent_map[name]["active"] += 1
                agent_map[name]["est"] += float(d.get("est_kwh") or 0) * float(d.get("adder") or 0)

        ranked = sorted(agent_map.items(), key=lambda x: -x[1]["active"])
        lines = []
        for i, (name, stats) in enumerate(ranked, 1):
            lines.append(f"#{i} {name}: {stats['active']} active, {stats['total']} total deals, est. {_fmt_money(stats['est'])}/mo")
        return _section(f"Sales Team Performance — {len(agents)} agent(s)", lines or ["No agent data found."])

    # ── Expiring / renewals (must come before generic "deals" check) ──
    if _want("expir", "renew", "renewal", "ending", "expire", "soon"):
        deals = db.table("lead_deals").select("id, supplier, sales_agent, end_date, status").eq("status", "Active").execute().data or []
        buckets = {"30 days": [], "60 days": [], "90 days": []}
        for d in deals:
            days = _days_until(d.get("end_date"))
            if days is None:
                continue
            sup = d.get("supplier") or "Unknown"
            agent = d.get("sales_agent") or "Unassigned"
            label = f"{sup} (agent: {agent}) — expires {_days_until_str(d.get('end_date'))}"
            if days <= 30:
                buckets["30 days"].append(label)
            elif days <= 60:
                buckets["60 days"].append(label)
            elif days <= 90:
                buckets["90 days"].append(label)
        lines = []
        for bucket, items in buckets.items():
            lines.append(f"Within {bucket}: {len(items)}")
            lines += [f"  → {i}" for i in items[:4]]
        return _section("Expiring Deals", lines or ["No deals expiring within 90 days."])

    # ── Leads ──
    if _want("lead", "leads", "prospect", "customer"):
        leads = db.table("leads").select("id, first_name, last_name, status, created_at, address").execute().data or []
        by_status: dict = {}
        for l in leads:
            s = (l.get("status") or "unknown").title()
            by_status[s] = by_status.get(s, 0) + 1
        lines = [f"{s}: {c}" for s, c in sorted(by_status.items(), key=lambda x: -x[1])]
        recent = sorted(leads, key=lambda x: x.get("created_at") or "", reverse=True)[:5]
        lines.append("")
        lines.append("Most recent leads:")
        for l in recent:
            name = f"{l.get('first_name','')} {l.get('last_name','')}".strip()
            lines.append(f"{name} — {(l.get('status') or '').title()}")
        return _section(f"Leads — {len(leads)} total", lines)

    # ── Deals / active / pipeline ──
    if _want("deal", "deals", "active", "pipeline", "contract", "accounts"):
        deals = db.table("lead_deals").select("id, status, supplier, sales_agent, est_kwh, adder, start_date, end_date").execute().data or []
        active = [d for d in deals if (d.get("status") or "").lower() == "active"]
        future = [d for d in deals if (d.get("status") or "").lower() == "future"]
        est_rev = sum(float(d.get("est_kwh") or 0) * float(d.get("adder") or 0) for d in active)

        by_supplier: dict = {}
        for d in active:
            s = d.get("supplier") or "Unknown"
            by_supplier[s] = by_supplier.get(s, 0) + 1
        sup_lines = [f"{s}: {c} deals" for s, c in sorted(by_supplier.items(), key=lambda x: -x[1])[:6]]

        lines = [
            f"Active deals: {len(active)}",
            f"Future/Pending: {len(future)}",
            f"Est. monthly commission from active: {_fmt_money(est_rev)}",
            "",
            "Active deals by supplier:",
        ] + sup_lines
        return _section(f"Deals — {len(deals)} total", lines)

    # ── Commissions / money / revenue / finances ──
    if _want("commission", "commissions", "money", "revenue", "payment", "paid", "earn", "financ", "income"):
        comms = db.table("actual_commissions").select("billing_month, raw_amount, supplier_id").order("billing_month", desc=True).limit(1000).execute().data or []
        by_month: dict = {}
        for c in comms:
            m = (c.get("billing_month") or "")[:7]
            if m:
                by_month[m] = by_month.get(m, 0.0) + (c.get("raw_amount") or 0.0)
        total = sum(by_month.values())
        lines = [f"{m}: {_fmt_money(amt)}" for m, amt in sorted(by_month.items(), reverse=True)[:6]]
        lines.append(f"")
        lines.append(f"Total recorded: {_fmt_money(total)} across {len(by_month)} months")

        # Also show estimated from active deals
        deals = db.table("lead_deals").select("est_kwh, adder").eq("status", "Active").execute().data or []
        est = sum(float(d.get("est_kwh") or 0) * float(d.get("adder") or 0) for d in deals)
        lines.append(f"Est. monthly commission (active deals): {_fmt_money(est)}")
        return _section("Commission & Revenue", lines or ["No commission data recorded yet."])


    # ── Alerts / issues / urgent ──
    if _want("alert", "alerts", "issue", "issues", "problem", "urgent", "warning", "critical"):
        alerts = db.table("ai_alerts").select("severity, message, type, created_at").eq("status", "open").order("severity").execute().data or []
        critical = [a for a in alerts if a.get("severity") == "high"]
        medium = [a for a in alerts if a.get("severity") == "medium"]
        low = [a for a in alerts if a.get("severity") == "low"]
        lines = [f"🔴 Urgent: {len(critical)}  🟡 Warning: {len(medium)}  🔵 Info: {len(low)}", ""]
        for a in (critical + medium)[:8]:
            prefix = "🔴" if a.get("severity") == "high" else "🟡"
            lines.append(f"{prefix} {a.get('message','')[:120]}")
        return _section(f"Open Alerts — {len(alerts)} total", lines or ["No open alerts. All clear!"])

    # ── Uploads / statements ──
    if _want("upload", "uploads", "statement", "statements", "file", "files"):
        uploads = db.table("upload_batches").select("original_filename, status, rows_imported, created_at, suppliers(name)").order("created_at", desc=True).limit(10).execute().data or []
        lines = []
        for u in uploads:
            supplier = (u.get("suppliers") or {}).get("name") or "Unknown"
            rows = u.get("rows_imported") or 0
            status = (u.get("status") or "").title()
            lines.append(f"{u.get('original_filename','?')} — {supplier}, {rows} rows, {status}")
        return _section("Recent Commission Uploads", lines or ["No uploads found."])

    # ── Fallback — show help ──
    return (
        "I can answer questions about your business. Try asking:\n\n"
        "  • **Summary** — full company overview\n"
        "  • **Leads** — lead counts and recent prospects\n"
        "  • **Deals** / **Active deals** — deal pipeline\n"
        "  • **Expiring deals** — what's up for renewal\n"
        "  • **Commissions** / **Revenue** — payment history\n"
        "  • **Agents** — sales team performance\n"
        "  • **Alerts** — open issues and warnings\n"
        "  • **Uploads** — recent commission statements"
    )
