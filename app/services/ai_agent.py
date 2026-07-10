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

def _resolve_alert(db, alert_type: str, entity_id: str):
    db.table("ai_alerts").update({
        "status": "resolved",
        "updated_at": _now().isoformat(),
    }).eq("type", alert_type).eq("entity_id", str(entity_id)).eq("status", "open").execute()

# ── Optimized Batch Scan ───────────────────────────────────────────────────────

def run_full_scan() -> dict:
    """
    Run all scans using batch DB queries.
    Pre-fetches all open alerts once, accumulates inserts/resolves in memory,
    then writes in batches — O(~8 queries) regardless of data size.
    """
    db = get_client()

    # 1. Pre-load all open alerts into a dict keyed by (type, entity_id)
    existing_raw = db.table("ai_alerts").select("id, type, entity_id").eq("status", "open").execute().data or []
    open_alerts: dict = {(a["type"], a["entity_id"]): a["id"] for a in existing_raw}

    to_insert: list = []
    to_resolve_ids: set = set()

    def _add(alert_type, entity_type, entity_id, message, severity, metadata=None):
        key = (alert_type, str(entity_id))
        if key not in open_alerts:
            to_insert.append({
                "type": alert_type, "entity_type": entity_type,
                "entity_id": str(entity_id), "message": message,
                "severity": severity, "status": "open",
                "metadata": metadata or {},
            })
            open_alerts[key] = "__pending__"

    def _resolve(alert_type, entity_id):
        key = (alert_type, str(entity_id))
        aid = open_alerts.get(key)
        if aid and aid != "__pending__":
            to_resolve_ids.add(aid)

    # 2. Scan lead_deals
    deals = db.table("lead_deals").select(
        "id, lead_id, supplier, esiid, rate, sales_agent, start_date, end_date, status, "
        "rate_type, plan_name, contract_term"
    ).neq("status", "Inactive").execute().data or []

    deal_issues = {"missing_rate": 0, "missing_esiid": 0, "missing_agent": 0,
                   "missing_dates": 0, "missing_rate_ids": [], "total_scanned": len(deals)}

    for d in deals:
        did = d["id"]
        lead_id = d.get("lead_id", "")
        supplier = d.get("supplier") or "Unknown Supplier"
        esiid = str(d.get("esiid") or "").strip()
        agent = str(d.get("sales_agent") or "").strip()
        start = d.get("start_date")
        end = d.get("end_date")

        try:
            has_rate = d.get("rate") is not None and float(d["rate"]) > 0
        except (ValueError, TypeError):
            has_rate = False

        if not has_rate:
            deal_issues["missing_rate"] += 1
            deal_issues["missing_rate_ids"].append(did)
            _add("missing_rate", "deal", did,
                 f"Deal ({supplier}) is missing a valid rate. This will impact commission calculations.",
                 "high", metadata={"lead_id": lead_id})
        else:
            _resolve("missing_rate", did)

        if d.get("status") == "Active" and not esiid:
            deal_issues["missing_esiid"] += 1
            _add("missing_esiid", "deal", did,
                 f"Active deal ({supplier}) is missing ESIID. Required for commission reconciliation.",
                 "high", metadata={"lead_id": lead_id})
        elif esiid:
            _resolve("missing_esiid", did)

        if not agent:
            deal_issues["missing_agent"] += 1
            _add("missing_agent", "deal", did,
                 f"Deal ({supplier}) has no assigned sales agent.",
                 "medium", metadata={"lead_id": lead_id})
        else:
            _resolve("missing_agent", did)

        if d.get("status") == "Active" and (not start or not end):
            deal_issues["missing_dates"] += 1
            _add("missing_dates", "deal", did,
                 f"Active deal ({supplier}) is missing start or end date.",
                 "medium", metadata={"lead_id": lead_id})
        elif start and end:
            _resolve("missing_dates", did)

    # 3. Scan renewals (reuse already-fetched deals)
    from app.utils.deals import is_month_to_month
    renewals = {"30_days": 0, "60_days": 0, "90_days": 0}
    for d in deals:
        if d.get("status") != "Active":
            continue
        did = d["id"]
        if is_month_to_month(d.get("rate_type"), d.get("plan_name"), d.get("contract_term")):
            # month-to-month never expires — clear any stale renewal alerts
            for t in ("renewal_30", "renewal_60", "renewal_90"):
                _resolve(t, did)
            continue
        days = _days_until(d.get("end_date"))
        if days is None:
            continue
        supplier = d.get("supplier") or "Unknown"
        if days <= 30:
            renewals["30_days"] += 1
            _add("renewal_30", "deal", did,
                 f"URGENT: Deal ({supplier}) expires in {days} day(s). Immediate renewal action required.",
                 "high", metadata={"lead_id": d.get("lead_id"), "days_until": days})
        elif days <= 60:
            renewals["60_days"] += 1
            _add("renewal_60", "deal", did,
                 f"Deal ({supplier}) expires in {days} days. Begin renewal conversation.",
                 "medium", metadata={"lead_id": d.get("lead_id"), "days_until": days})
        elif days <= 90:
            renewals["90_days"] += 1
            _add("renewal_90", "deal", did,
                 f"Deal ({supplier}) expires in {days} days. Start renewal planning.",
                 "low", metadata={"lead_id": d.get("lead_id"), "days_until": days})

    # 4. Scan inactive leads (2 queries total)
    leads = db.table("leads").select("id, first_name, last_name, created_at").eq("status", "lead").execute().data or []
    inactive_count = 0
    if leads:
        lead_ids = [l["id"] for l in leads]
        recent_tasks = db.table("tasks").select("lead_id").in_("lead_id", lead_ids).eq("status", "completed").execute().data or []
        active_lead_ids = {t["lead_id"] for t in recent_tasks}
        for lead in leads:
            lid = lead["id"]
            age = _days_ago(lead.get("created_at")) or 0
            if age >= 14 and lid not in active_lead_ids:
                inactive_count += 1
                name = f"{lead.get('first_name','')} {lead.get('last_name','')}".strip()
                _add("inactive_lead", "lead", lid,
                     f"Lead '{name}' has had no activity for {age} days.", "low")
            elif lid in active_lead_ids:
                _resolve("inactive_lead", lid)

    # 5. Scan duplicate leads (1 query, in-memory dedup)
    all_leads = db.table("leads").select("id, first_name, last_name, address").execute().data or []
    seen: dict = {}
    dupes = 0
    for l in all_leads:
        key = (
            f"{(l.get('first_name') or '').strip().lower()}|"
            f"{(l.get('last_name') or '').strip().lower()}|"
            f"{(l.get('address') or '').strip().lower()[:30]}"
        )
        if key in seen:
            dupes += 1
            _add("duplicate_lead", "lead", l["id"],
                 f"Possible duplicate lead: '{l.get('first_name','')} {l.get('last_name','')}' at '{l.get('address','')}'",
                 "medium", metadata={"original_id": seen[key]})
        else:
            seen[key] = l["id"]

    # 6. Batch write — inserts chunked at 50, resolves chunked at 50
    for i in range(0, len(to_insert), 50):
        db.table("ai_alerts").insert(to_insert[i:i + 50]).execute()

    resolved_list = list(to_resolve_ids)
    for i in range(0, len(resolved_list), 50):
        db.table("ai_alerts").update({
            "status": "resolved",
            "updated_at": _now().isoformat(),
        }).in_("id", resolved_list[i:i + 50]).execute()

    return {
        "scanned_at": _now().isoformat(),
        "deal_issues": deal_issues,
        "renewals": renewals,
        "inactive_leads": inactive_count,
        "duplicate_leads": dupes,
        "alerts_created": len(to_insert),
        "alerts_resolved": len(to_resolve_ids),
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

    # Leads with NO active deal (potential to work / convert)
    leads_with_active_rows = db.table("lead_deals").select("lead_id").eq("status", "Active").execute().data or []
    leads_with_active_ids  = set(d["lead_id"] for d in leads_with_active_rows if d.get("lead_id"))
    leads_no_active_deal   = max(total_leads - len(leads_with_active_ids), 0)

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

    # Active imported customer accounts (distinct customers with at least 1 ACTIVE crm_deal)
    crm_active_rows     = db.table("crm_deals").select("customer_id").eq("deal_status", "ACTIVE").execute().data or []
    active_crm_customers = len(set(d["customer_id"] for d in crm_active_rows if d.get("customer_id")))

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
            "leads_no_active_deal": leads_no_active_deal,
            "active_crm_customers": active_crm_customers,
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
            f"{dq['total_issues']} data quality issue{'s' if dq['total_issues'] != 1 else ''} found: "
            f"{dq['dup_esiid']} duplicate ESI ID{'s' if dq['dup_esiid'] != 1 else ''}, "
            f"{dq['dup_address']} duplicate address{'es' if dq['dup_address'] != 1 else ''}."
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

def executive_context(db) -> dict:
    """The whole business, condensed for the AI: verified money, growth,
    book value, provider quality, churn risk, agents, payouts, pipeline."""
    import json as _json
    from app.services.business_health import build_business_health

    health = build_business_health(db)

    # payout pipeline
    payouts = db.table("agent_commissions").select("agent_name,month,year,total_commission,status") \
        .order("year", desc=True).order("month", desc=True).limit(20).execute().data or []

    # statement coverage this month (providers pay by the 7th)
    from app.services.statement_watchdog import last_month_label
    from app.services.file_parser.provider_parsers import PROVIDER_SUPPLIERS
    label = last_month_label()
    reported = set()
    for p in health.get("providers", []):
        if p.get("latest_month") and p["latest_month"] >= label:
            reported.add(p["name"])
    awaiting = [PROVIDER_SUPPLIERS[g]["name"] for g in PROVIDER_SUPPLIERS
                if PROVIDER_SUPPLIERS[g]["name"] not in reported]

    metrics = get_daily_metrics(db)

    ctx = {
        "company": "Saigon Power LLC — Texas retail energy broker. Revenue = residual commissions "
                   "from providers (REPs) on customer electricity usage, paid monthly by the 7th.",
        "book_value": health.get("book"),
        "account_growth_recent_months": [
            {k: v for k, v in g.items() if k != "by_provider"} for g in health.get("growth", [])
        ],
        "providers": health.get("providers"),
        "winback": {k: v for k, v in (health.get("winback") or {}).items() if k != "queue"},
        "winback_top": (health.get("winback") or {}).get("queue", [])[:5],
        "open_money_issues": health.get("chasing"),
        "agents": health.get("agents"),
        "agent_payouts_recent": payouts,
        "statement_month_expected": label,
        "statements_still_awaited_from": awaiting,
        "pipeline_today": metrics.get("today"),
        "pipeline": metrics.get("pipeline"),
        "month_to_date": metrics.get("mtd"),
        "renewals_open": metrics.get("renewals"),
        "data_quality": metrics.get("data_quality"),
    }

    # Commission audit intelligence (Phase 1 engine): what each provider owes
    # us right now, the biggest systemic findings, and dispute recovery status.
    try:
        ctx["commission_audit"] = _commission_audit_context(db)
    except Exception:
        pass  # migration 008 not applied yet

    # keep the prompt lean
    return _json.loads(_json.dumps(ctx, default=str))


def _commission_audit_context(db) -> dict:
    """Bounded summary of exception cases, findings, and disputes for the AI."""
    from app.services.reconciliation_v2 import fetch_all
    from app.services.exception_cases import OPEN_STATUSES

    sups = {s["id"]: s["name"] for s in
            db.table("suppliers").select("id,name").limit(500).execute().data or []}

    per_provider: dict = {}
    cases = fetch_all(db, "exception_cases",
                      "supplier_id,workflow_status,estimated_loss,recovered_amount,issue_type")
    for c in cases:
        name = sups.get(c["supplier_id"], "Unknown")
        p = per_provider.setdefault(name, {"open_cases": 0, "estimated_owed": 0.0,
                                           "recovered": 0.0})
        if c.get("workflow_status") in OPEN_STATUSES:
            p["open_cases"] += 1
            p["estimated_owed"] += float(c.get("estimated_loss") or 0)
        p["recovered"] += float(c.get("recovered_amount") or 0)
    for p in per_provider.values():
        p["estimated_owed"] = round(p["estimated_owed"], 2)
        p["recovered"] = round(p["recovered"], 2)

    findings = db.table("audit_findings").select(
        "title,explanation,finding_type,estimated_impact,affected_count,status,billing_month,supplier_id") \
        .in_("status", ["open", "investigating", "disputed"]) \
        .order("estimated_impact", desc=True).limit(5).execute().data or []
    for f in findings:
        f["provider"] = sups.get(f.pop("supplier_id", None), "")
        f["explanation"] = (f.get("explanation") or "")[:280]

    disputes = db.table("disputes").select("status,total_claimed,total_recovered") \
        .limit(500).execute().data or []
    return {
        "providers_owing_us": {k: v for k, v in per_provider.items()
                               if v["open_cases"] or v["recovered"]},
        "top_open_findings": findings,
        "disputes": {
            "draft": sum(1 for d in disputes if d["status"] == "draft"),
            "sent_awaiting_response": sum(1 for d in disputes if d["status"] == "sent"),
            "total_claimed": round(sum(float(d.get("total_claimed") or 0)
                                       for d in disputes), 2),
            "total_recovered": round(sum(float(d.get("total_recovered") or 0)
                                         for d in disputes), 2),
        },
    }


EXEC_SYSTEM = """You are the AI Chief of Staff for Saigon Power LLC — part CEO advisor, part CFO, part operations manager.
The owner (Lance) is scaling this brokerage toward multi-million revenue. You are direct, numerate, and action-oriented.

Rules:
- Use ONLY the numbers in the business context. Never invent figures. Money formatted like $1,234.
- "book_value" numbers come from VERIFIED provider payments — treat them as the financial truth.
- Distinguish verified money (received) from estimates (pipeline est_kwh × adder).
- When something needs doing, name the page in the CRM: Reconciliation, Uploads, My Business, Sales Agents, Commission Management, Renewals.
- Be concise. Bullets over paragraphs. Lead with what matters most to cash."""


def generate_executive_briefing(db) -> str:
    """CEO daily/on-demand briefing from the full business context."""
    import json as _json
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    ctx = executive_context(db)
    if not api_key:
        return _template_summary(get_daily_metrics(db))
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model="claude-sonnet-5",
            max_tokens=900,
            system=EXEC_SYSTEM,
            messages=[{"role": "user", "content":
                "Write today's executive briefing in markdown with exactly these sections:\n"
                "**💰 Money** (received vs expected, dollars being chased, payouts owed to agents)\n"
                "**📈 Growth** (net accounts, book value trend, what's driving it)\n"
                "**⚠️ Risks** (churn/win-back, provider concentration, missing statements)\n"
                "**✅ Do today** (max 3 actions, most valuable first, each with its dollar impact and the CRM page to do it on)\n\n"
                f"Business context JSON:\n{_json.dumps(ctx)}"}]
        )
        return msg.content[0].text.strip()
    except Exception:
        return _template_summary(get_daily_metrics(db))


def generate_ai_summary(metrics: dict) -> str:
    """Short dashboard blurb. Prefers today's stored briefing headline."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    try:
        if not api_key:
            return _template_summary(metrics)
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
        try:
            return _template_summary(metrics)
        except Exception:
            return "All systems operating normally."

# ── Recommendations ────────────────────────────────────────────────────────────

def get_recommendations(db) -> list[dict]:
    recs = []

    # Money first — from verified reconciliation / status data
    try:
        from app.services.business_health import build_business_health
        h = build_business_health(db)
        chasing = h.get("chasing") or {}
        if (chasing.get("total") or 0) > 50:
            recs.append({"icon": "💵", "priority": "high",
                         "text": f"${chasing['total']:,.0f} is owed to you on the latest statements "
                                 f"(${chasing.get('missing_dollars', 0):,.0f} missing + "
                                 f"${chasing.get('underpaid_dollars', 0):,.0f} underpaid) — work the Reconciliation list."})
        wb = h.get("winback") or {}
        if (wb.get("count") or 0) > 0:
            recs.append({"icon": "📉", "priority": "high",
                         "text": f"{wb['count']} account{'s' if wb['count'] != 1 else ''} flagged as leaving "
                                 f"(${wb.get('monthly_value_at_risk', 0):,.0f}/mo at stake) — call them from the Win-Back Queue."})
        for p in h.get("providers", []):
            if p.get("months_not_reporting", 0) >= 2:
                recs.append({"icon": "📄", "priority": "medium",
                             "text": f"{p['name']} has {p['months_not_reporting']} months without a statement — "
                                     f"request the missing reports and upload them."})
    except Exception:
        pass

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
    summary  = generate_executive_briefing(db)
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

    metrics = get_daily_metrics(db)
    # Prefer today's stored executive briefing (rich, already paid for);
    # fall back to a short generated blurb.
    stored = db.table("ai_reports").select("summary").eq("report_type", "daily") \
        .eq("report_date", _today()).limit(1).execute().data
    summary = stored[0]["summary"] if stored else generate_ai_summary(metrics)
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


CHAT_MODEL = "claude-opus-4-8"
MAX_TOOL_TURNS = 8


def _chat_system(ctx_json: str) -> str:
    from app.services.ai_tools import schema_for_prompt
    return (EXEC_SYSTEM + """

You have READ-ONLY database tools (query_crm, aggregate_crm) over the CRM. Use them
to answer with real, current numbers instead of guessing — call aggregate_crm for
every count/total question and query_crm for record lookups.

Critical domain facts:
- The deal book lives in TWO tables. lead_deals (status = 'Active') AND crm_deals
  (deal_status = 'ACTIVE'). Any question about deals, active accounts, or customers
  must check BOTH and combine the results.
- adder is the commission rate in $/kWh (0.008 = 8 mils). Estimated monthly
  commission for a deal = adder x est_kwh.
- actual_commissions is verified money received; everything else is an estimate.
- exception_cases / audit_findings / disputes are the commission audit system —
  money the providers owe us and how we're chasing it.
- billing_month columns are dates like 2026-05-01 (filter with gte/lt for ranges).

Queryable tables:
""" + schema_for_prompt() + """

Business context snapshot (may be minutes old — prefer fresh tool queries for numbers):
""" + ctx_json)


def chat_with_context(message: str, history: list) -> str:
    """Agentic CRM chat: Claude answers with live database queries via
    read-only tools. Falls back to the keyword engine when no API key is
    configured or the API errors."""
    import json as _json
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if api_key:
        try:
            from app.services.ai_tools import TOOL_DEFINITIONS, execute_tool
            db = get_client()
            ctx = executive_context(db)
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)

            msgs = []
            for h in (history or [])[-10:]:
                role = h.get("role")
                content = str(h.get("content") or "")[:4000]
                if role in ("user", "assistant") and content:
                    msgs.append({"role": role, "content": content})
            msgs.append({"role": "user", "content": message[:4000]})

            system = _chat_system(_json.dumps(ctx))

            # Manual tool-use loop: run queries until Claude stops asking.
            response = None
            for _ in range(MAX_TOOL_TURNS):
                response = client.messages.create(
                    model=CHAT_MODEL,
                    max_tokens=8000,
                    system=system,
                    tools=TOOL_DEFINITIONS,
                    messages=msgs,
                )
                if response.stop_reason != "tool_use":
                    break
                msgs.append({"role": "assistant", "content": response.content})
                results = []
                for block in response.content:
                    if block.type == "tool_use":
                        results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": execute_tool(db, block.name, block.input),
                        })
                msgs.append({"role": "user", "content": results})

            if response is not None:
                text = "\n\n".join(b.text for b in response.content
                                   if b.type == "text").strip()
                if text:
                    return text
        except Exception:
            pass
    return _keyword_chat(message, history)


def _full_deal_book(db) -> list:
    """BOTH deal tables normalized to one shape — lead_deals is only a fraction
    of the book; crm_deals holds most of it. Every deal count must use this."""
    from app.services.reconciliation_v2 import fetch_all
    book = []
    for d in fetch_all(db, "lead_deals", "status,supplier,sales_agent,est_kwh,adder,end_date"):
        book.append({"active": (d.get("status") or "").lower() == "active",
                     "future": (d.get("status") or "").lower() == "future",
                     "supplier": d.get("supplier") or "Unknown",
                     "agent": d.get("sales_agent") or "",
                     "est_kwh": float(d.get("est_kwh") or 0),
                     "adder": float(d.get("adder") or 0),
                     "end_date": d.get("end_date")})
    for d in fetch_all(db, "crm_deals", "deal_status,provider,sales_agent,meter_type,adder,contract_end_date"):
        book.append({"active": (d.get("deal_status") or "").upper() == "ACTIVE",
                     "future": (d.get("deal_status") or "").upper() in ("FUTURE", "PENDING"),
                     "supplier": d.get("provider") or "Unknown",
                     "agent": d.get("sales_agent") or "",
                     "est_kwh": 2500.0 if d.get("meter_type") == "Commercial" else 1100.0,
                     "adder": float(d.get("adder") or 0),
                     "end_date": d.get("contract_end_date")})
    return book


_MONTH_NAMES = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
                "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}


def _provider_match(names, q: str):
    """Best provider-name match in the question: full name, or its first
    distinctive word ('chariot' -> 'Chariot Energy'). Longest match wins."""
    best = None
    for name in names:
        n = (name or "").strip()
        if len(n) < 3:
            continue
        first = n.split()[0].lower()
        if n.lower() in q or (len(first) >= 3 and first in q.split()) or (len(first) >= 4 and first in q):
            if best is None or len(n) > len(best):
                best = n
    return best


def _detect_month(q: str):
    """'may 2026' / '2026-05' / 'last month' -> 'YYYY-MM' (or None)."""
    import re as _re
    q = q.lower()
    m = _re.search(r"\b(20\d{2})-(\d{2})\b", q)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    m = _re.search(r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?,?\s+(20\d{2})\b", q)
    if m:
        return f"{m.group(2)}-{_MONTH_NAMES[m.group(1)]:02d}"
    today = _now().date()
    if "last month" in q:
        y, mo = (today.year, today.month - 1) if today.month > 1 else (today.year - 1, 12)
        return f"{y}-{mo:02d}"
    if "this month" in q:
        return f"{today.year}-{today.month:02d}"
    return None


def _extract_esiid(q: str):
    import re as _re
    m = _re.search(r"\b(\d{15,22})\b", q)
    return m.group(1) if m else None


_SEARCH_TRIGGERS = ("find ", "look up ", "lookup ", "search for ", "search ",
                    "phone number for ", "phone for ", "email for ")
_SEARCH_STOPWORDS = {"customer", "customers", "client", "account", "the", "a", "an",
                     "for", "named", "name", "called", "info", "details", "please",
                     "number", "phone", "email", "of", "me", "up"}


def _extract_search_term(message: str):
    """'find customer Julie Vu' -> 'Julie Vu' (or None when not a lookup)."""
    low = message.lower()
    for t in _SEARCH_TRIGGERS:
        idx = low.find(t)
        if idx >= 0:
            rest = message[idx + len(t):]
            words = [w.strip("?.,!\"'") for w in rest.split()]
            keep = [w for w in words if w.lower() not in _SEARCH_STOPWORDS and w]
            term = " ".join(keep[:4]).strip()
            return term if len(term) >= 3 else None
    return None


def _keyword_chat(message: str, history: list) -> str:
    db = get_client()
    q = message.lower()

    # ── helpers ──
    def _want(*keywords):
        return any(k in q for k in keywords)

    parts = []

    # ── Account lookup by ESI ID (a long digit string in the question) ──
    esiid = _extract_esiid(q)
    if esiid:
        lines = []
        deal = None
        ld = db.table("lead_deals").select(
            "status,supplier,adder,est_kwh,end_date,sales_agent,leads(first_name,last_name,phone)") \
            .eq("esiid", esiid).limit(1).execute().data or []
        if ld:
            d, lead = ld[0], (ld[0].get("leads") or {})
            deal = (f"{lead.get('first_name','')} {lead.get('last_name','')}".strip() or "Unknown",
                    d.get("supplier"), d.get("status"), d.get("adder"), d.get("end_date"),
                    d.get("sales_agent"), lead.get("phone"))
        else:
            cd = db.table("crm_deals").select(
                "deal_status,provider,adder,contract_end_date,sales_agent,crm_customers(full_name,phone)") \
                .eq("esiid", esiid).limit(1).execute().data or []
            if cd:
                d, cust = cd[0], (cd[0].get("crm_customers") or {})
                deal = (cust.get("full_name") or "Unknown", d.get("provider"), d.get("deal_status"),
                        d.get("adder"), d.get("contract_end_date"), d.get("sales_agent"),
                        cust.get("phone"))
        if deal:
            name, prov, status, adder, end, agent, phone = deal
            lines.append(f"Customer: {name}" + (f" · {phone}" if phone else ""))
            lines.append(f"Provider: {prov or '—'} · Status: {status or '—'} · "
                         f"Adder: {adder if adder is not None else '—'} $/kWh")
            lines.append(f"Contract ends: {str(end)[:10] if end else 'open'} · Agent: {agent or '—'}")
        else:
            lines.append("Not linked to any deal in the CRM.")

        pays = db.table("actual_commissions").select("billing_month,raw_amount,raw_rate") \
            .eq("raw_esiid", esiid).order("billing_month", desc=True).limit(400).execute().data or []
        if pays:
            by_m: dict = {}
            rate_by_m: dict = {}
            for p in pays:
                m = str(p.get("billing_month") or "")[:7]
                by_m[m] = by_m.get(m, 0.0) + float(p.get("raw_amount") or 0)
                if p.get("raw_rate") is not None:
                    rate_by_m.setdefault(m, p["raw_rate"])
            lines.append("")
            lines.append(f"Payments: {_fmt_money(sum(by_m.values()))} lifetime across {len(by_m)} months. Recent:")
            for m in sorted(by_m, reverse=True)[:6]:
                rate = rate_by_m.get(m)
                lines.append(f"  {m}: {_fmt_money(by_m[m])}" + (f" @ {rate:g} $/kWh" if rate else ""))
        else:
            lines.append("No commission payments recorded for this ESI ID.")

        try:
            cases = db.table("exception_cases").select("issue_type,workflow_status,estimated_loss,billing_month") \
                .eq("esiid", esiid).in_("workflow_status", ["open", "investigating", "waiting_on_provider"]) \
                .limit(10).execute().data or []
            if cases:
                lines.append("")
                lines.append("Open audit cases:")
                for c in cases:
                    lines.append(f"  {str(c['billing_month'])[:7]}: {c['issue_type'].replace('_', ' ')} — "
                                 f"est. {_fmt_money(c.get('estimated_loss') or 0)} ({c['workflow_status'].replace('_', ' ')})")
        except Exception:
            pass
        return _section(f"Account {esiid}", lines)

    # ── Customer / lead lookup by name: "find Julie Vu" ──
    term = _extract_search_term(message)
    if term:
        words = term.split()
        hits = []

        # customers: every word must appear somewhere in full_name
        cq = db.table("crm_customers").select("id,full_name,phone,email,city")
        for w in words:
            cq = cq.ilike("full_name", f"%{w}%")
        for c in (cq.limit(6).execute().data or []):
            hits.append(("Customer", c.get("full_name"), c.get("phone"), c.get("email"),
                         c.get("city"), c.get("id")))

        # leads: try first+last split, then each word on either column
        lead_queries = []
        if len(words) >= 2:
            lead_queries.append(db.table("leads").select("id,first_name,last_name,status,phone,email")
                                .ilike("first_name", f"%{words[0]}%")
                                .ilike("last_name", f"%{words[-1]}%"))
        for col in ("first_name", "last_name"):
            lead_queries.append(db.table("leads").select("id,first_name,last_name,status,phone,email")
                                .ilike(col, f"%{term}%"))
        seen_ids = set()
        for lq in lead_queries:
            for l in (lq.limit(6).execute().data or []):
                if l.get("id") in seen_ids:
                    continue
                seen_ids.add(l.get("id"))
                nm = f"{l.get('first_name','')} {l.get('last_name','')}".strip()
                hits.append((f"Lead ({(l.get('status') or '').title()})", nm, l.get("phone"),
                             l.get("email"), None, l.get("id")))
        if hits:
            lines = []
            cust_ids = [h[5] for h in hits if h[0] == "Customer"]
            deals_by_cust: dict = {}
            if cust_ids:
                for d in (db.table("crm_deals").select("customer_id,provider,deal_status")
                          .in_("customer_id", cust_ids[:20]).limit(60).execute().data or []):
                    deals_by_cust.setdefault(d["customer_id"], []).append(
                        f"{d.get('provider') or '?'} ({(d.get('deal_status') or '').title()})")
            for kind, nm, phone, email, city, _id in hits[:8]:
                bits = [b for b in (phone, email, city) if b]
                line = f"{nm} — {kind}" + (f" · {' · '.join(bits)}" if bits else "")
                if _id in deals_by_cust:
                    line += f" · Deals: {', '.join(deals_by_cust[_id][:3])}"
                lines.append(line)
            return _section(f"Search results for \"{term}\" — {len(hits)} match(es)", lines)
        # no matches — fall through so other branches can still answer

    # ── Commission audit: "how much does NRG owe us?", disputes, shortfalls ──
    if _want("owe", "owed", "owes", "dispute", "disputes", "shortfall", "underpaid",
             "under paid", "wrong mil", "wrong rate", "missing commission", "clawback"):
        try:
            audit_ctx = _commission_audit_context(db)
            owing = audit_ctx.get("providers_owing_us") or {}
            named = [n for n in owing if n.lower() in q]  # e.g. "how much does NRG owe us"
            show = named or list(owing)
            lines = []
            for name in sorted(show, key=lambda n: -owing[n]["estimated_owed"]):
                p = owing[name]
                lines.append(f"{name}: {_fmt_money(p['estimated_owed'])} across "
                             f"{p['open_cases']} open case(s)"
                             + (f" · {_fmt_money(p['recovered'])} recovered" if p["recovered"] else ""))
            total = sum(owing[n]["estimated_owed"] for n in show)
            parts.append(_section(f"Estimated owed to Saigon — {_fmt_money(total)} total",
                                  lines or ["No open exception cases. All statements reconcile clean."]))
            fs = audit_ctx.get("top_open_findings") or []
            if fs:
                parts.append(_section("Biggest open findings", [
                    f"{f['title']} — est. {_fmt_money(f.get('estimated_impact') or 0)} ({f.get('status')})"
                    for f in fs[:5]]))
            d = audit_ctx.get("disputes") or {}
            parts.append(_section("Disputes", [
                f"Drafts awaiting your review: {d.get('draft', 0)}",
                f"Sent, awaiting provider: {d.get('sent_awaiting_response', 0)}",
                f"Claimed {_fmt_money(d.get('total_claimed') or 0)} · Recovered {_fmt_money(d.get('total_recovered') or 0)}",
            ]))
            parts.append("Manage these on the Reconciliation and Disputes pages.")
            return "\n\n".join(parts)
        except Exception:
            pass  # audit tables not available — fall through to generic answers

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

        # Deals (both deal tables — lead_deals + crm_deals)
        book = _full_deal_book(db)
        active = [d for d in book if d["active"]]
        future = [d for d in book if d["future"]]
        est_rev = sum(d["est_kwh"] * d["adder"] for d in active)
        expiring_30 = [d for d in active if (_days_until(d.get("end_date")) or 999) <= 30]
        deal_lines = [
            f"Active: {len(active)}",
            f"Future/Pending: {len(future)}",
            f"Est. monthly commission: {_fmt_money(est_rev)}",
            f"Expiring within 30 days: {len(expiring_30)}",
        ]
        parts.append(_section("Deals", deal_lines))

        # Commissions (from reconciliation-run totals — full months, not a row sample)
        runs = db.table("reconciliation_runs").select("billing_month,total_actual") \
            .like("notes", '%"engine": "v2"%').order("billing_month", desc=True) \
            .limit(1000).execute().data or []
        by_month: dict = {}
        for r in runs:
            m = str(r.get("billing_month"))[:7]
            by_month[m] = by_month.get(m, 0.0) + (r.get("total_actual") or 0)
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
        book = _full_deal_book(db)
        agent_map: dict = {}
        for d in book:
            name = d["agent"].strip() or "Unassigned"
            key = name.upper()
            if key not in agent_map:
                agent_map[key] = {"name": name, "active": 0, "total": 0, "est": 0.0}
            agent_map[key]["total"] += 1
            if d["active"]:
                agent_map[key]["active"] += 1
                agent_map[key]["est"] += d["est_kwh"] * d["adder"]

        ranked = sorted(agent_map.values(), key=lambda x: -x["active"])
        lines = []
        for i, stats in enumerate(ranked[:15], 1):
            lines.append(f"#{i} {stats['name']}: {stats['active']} active, {stats['total']} total deals, "
                         f"est. {_fmt_money(stats['est'])}/mo")
        return _section(f"Sales Team Performance — whole book ({len(book)} deals)",
                        lines or ["No agent data found."])

    # ── Expiring / renewals (must come before generic "deals" check) ──
    if _want("expir", "renew", "renewal", "ending", "expire", "soon"):
        deals = [d for d in _full_deal_book(db) if d["active"]]
        buckets = {"30 days": [], "60 days": [], "90 days": []}
        for d in deals:
            days = _days_until(d.get("end_date"))
            if days is None:
                continue
            sup = d["supplier"] or "Unknown"
            agent = d["agent"].strip() or "Unassigned"
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

    # ── Deals / active / pipeline (BOTH deal tables) ──
    if _want("deal", "deals", "active", "pipeline", "contract", "accounts"):
        book = _full_deal_book(db)
        # provider scoping: "how many active deals with direct energy?"
        matched = _provider_match({d["supplier"].strip() for d in book if d["supplier"]}, q)
        scoped_to = matched.upper() if matched else None
        if scoped_to:
            first_word = scoped_to.split()[0]
            book = [d for d in book if first_word in d["supplier"].strip().upper()]
        active = [d for d in book if d["active"]]
        future = [d for d in book if d["future"]]
        est_rev = sum(d["est_kwh"] * d["adder"] for d in active)

        by_supplier: dict = {}
        display: dict = {}
        for d in active:
            key = d["supplier"].strip().upper()
            display.setdefault(key, d["supplier"].strip())
            by_supplier[key] = by_supplier.get(key, 0) + 1
        sup_lines = [f"{display[s]}: {c} deals"
                     for s, c in sorted(by_supplier.items(), key=lambda x: -x[1])[:8]]

        lines = [
            f"Active deals: {len(active)}",
            f"Future/Pending: {len(future)}",
            f"Est. monthly commission from active: {_fmt_money(est_rev)}",
        ]
        if not scoped_to:
            lines += ["", "Active deals by provider:"] + sup_lines
        title = (f"{display.get(scoped_to, scoped_to.title())} deals — {len(book)} total"
                 if scoped_to else f"Deals — {len(book)} total across the whole book")
        return _section(title, lines)

    # ── Commissions / money / revenue — provider- and month-aware ──
    if _want("commission", "commissions", "money", "revenue", "payment", "paid", "earn", "financ", "income", "received", "receive"):
        runs = db.table("reconciliation_runs").select(
            "billing_month,total_actual,supplier_id,suppliers(name)") \
            .like("notes", '%"engine": "v2"%').order("billing_month", desc=True) \
            .limit(1000).execute().data or []

        month = _detect_month(q)
        provider = _provider_match(
            {(r.get("suppliers") or {}).get("name") or "" for r in runs}, q)
        scoped = [r for r in runs
                  if (not provider or (r.get("suppliers") or {}).get("name") == provider)
                  and (not month or str(r.get("billing_month"))[:7] == month)]

        title_bits = [b for b in (provider, month) if b]
        title = "Commission Received" + (f" — {' · '.join(title_bits)}" if title_bits else "")
        lines = []
        if month:
            total = sum(r.get("total_actual") or 0 for r in scoped)
            lines.append(f"Received in {month}: {_fmt_money(total)}")
            for r in sorted(scoped, key=lambda r: -(r.get("total_actual") or 0)):
                lines.append(f"  {(r.get('suppliers') or {}).get('name', '?')}: "
                             f"{_fmt_money(r.get('total_actual') or 0)}")
        else:
            by_month: dict = {}
            for r in scoped:
                m = str(r.get("billing_month"))[:7]
                by_month[m] = by_month.get(m, 0.0) + (r.get("total_actual") or 0)
            for m in sorted(by_month, reverse=True)[:8]:
                lines.append(f"{m}: {_fmt_money(by_month[m])}")
            lines.append("")
            lines.append(f"All-time verified received{' from ' + provider if provider else ''}: "
                         f"{_fmt_money(sum(by_month.values()))} across {len(by_month)} months")
        if not provider:
            est = sum(d["est_kwh"] * d["adder"] for d in _full_deal_book(db) if d["active"])
            lines.append(f"Est. monthly commission (active deals): {_fmt_money(est)}")
        return _section(title, lines if scoped else
                        [f"No statements found{' for ' + provider if provider else ''}"
                         f"{' in ' + month if month else ''}."])


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
        "  • **Active deals** — whole-book counts, or scoped: \"active deals with Direct Energy\"\n"
        "  • **Commissions** — \"commission received in May 2026\", \"revenue from Chariot\"\n"
        "  • **Who owes us** — \"how much does Discount Power owe us?\", disputes, shortfalls\n"
        "  • **Find someone** — \"find Julie Vu\" (customers & leads by name)\n"
        "  • **An ESI ID** — paste it to see the account, payment history, and open cases\n"
        "  • **Expiring deals** — what's up for renewal\n"
        "  • **Agents** — sales team performance across the whole book\n"
        "  • **Alerts** / **Uploads** — open issues, recent statements"
    )
