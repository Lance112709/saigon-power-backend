"""SGP Agent tier-based commission structure.

Approved SGP Agents (classification SGP_AGENT + agreement APPROVED) earn a
permanent split of the gross profit their book generates — 50/50 at approval,
rising in 5% steps to a hard cap of 70/30. A tier is unlocked permanently by
reaching its monthly-GP threshold in three separate calendar months (not
necessarily consecutive). Agents are NEVER automatically demoted.

GP definitions (matching the payout engine's philosophy — agents are paid
only from dollars providers actually paid):
  PROVIDER_PAID_GP (default) — sum of actual_commissions.raw_amount on the
      agent's deal ESIIDs for a statement month (clawbacks are negative rows
      and net out naturally).
  FINALIZED_GP — provider-paid minus amounts on accounts with an OPEN
      exception case (disputed/under-investigation money doesn't qualify).

Everything here is opt-in per agent and idempotent: re-running an evaluation
never duplicates qualifying months (DB unique key), promotions, history rows,
or alerts.
"""
from datetime import date, datetime, timezone
from typing import Optional

from app.services.audit import audit
from app.services.reconciliation_v2 import fetch_all

OPEN_CASE_STATUSES = ("open", "investigating", "waiting_on_provider")

DEFAULT_SETTINGS = {"qualification_basis": "PROVIDER_PAID_GP",
                    "promotion_effective_rule": "NEXT_COMMISSION_PERIOD"}


# ---- config / reference data ------------------------------------------------

def get_settings(db) -> dict:
    try:
        rows = db.table("sgp_settings").select("*").eq("id", 1).limit(1).execute().data
        if rows:
            return {**DEFAULT_SETTINGS, **{k: v for k, v in rows[0].items() if v is not None}}
    except Exception:
        pass
    return dict(DEFAULT_SETTINGS)


def load_tiers(db) -> list:
    tiers = db.table("sgp_tiers").select("*").eq("active", True) \
        .order("tier_order").limit(20).execute().data or []
    # hard cap enforced in code as well as in the DB constraint
    return [t for t in tiers if float(t.get("agent_split") or 0) <= 70]


def is_eligible(agent: dict) -> tuple:
    """(eligible, reason). The gate for tier progress AND tier payouts."""
    if (agent.get("classification") or "") != "SGP_AGENT":
        return False, f"classification is {agent.get('classification') or 'unset'} — not an SGP Agent"
    if (agent.get("agreement_status") or "") != "APPROVED":
        return False, ("not eligible for SGP commission tiers because the SGP Agent "
                       f"Agreement has not been approved (status: {agent.get('agreement_status') or 'NOT_SENT'})")
    if agent.get("sgp_suspended"):
        return False, "SGP commission eligibility is suspended"
    if agent.get("agreement_terminated_at"):
        return False, "agent agreement is terminated"
    return True, "eligible"


# ---- GP math ----------------------------------------------------------------

def agent_esiids(book: dict, agent_name: str) -> set:
    from app.services.agent_commission_engine import norm_name
    me = norm_name(agent_name)
    return {es for es, d in book.items() if norm_name(d.get("agent")) == me}


def statement_months(db) -> list:
    rows = db.table("reconciliation_runs").select("billing_month") \
        .like("notes", '%"engine": "v2"%').order("billing_month", desc=True) \
        .limit(1000).execute().data or []
    return sorted({str(r["billing_month"])[:7] for r in rows})


def _disputed_esiids(db, esiids: list, label: str) -> set:
    out = set()
    for i in range(0, len(esiids), 100):
        rows = fetch_all(db, "exception_cases", "esiid",
                         filters=[("eq", ("billing_month", f"{label}-01")),
                                  ("in_", ("workflow_status", list(OPEN_CASE_STATUSES))),
                                  ("in_", ("esiid", esiids[i:i + 100]))])
        out.update(r["esiid"] for r in rows)
    return out


def monthly_gp(db, esiids: set, label: str, basis: str) -> float:
    """Eligible GP for one agent, one statement month."""
    if not esiids:
        return 0.0
    es_list = sorted(esiids)
    skip = _disputed_esiids(db, es_list, label) if basis == "FINALIZED_GP" else set()
    total = 0.0
    for i in range(0, len(es_list), 100):
        rows = fetch_all(db, "actual_commissions", "raw_esiid,raw_amount",
                         filters=[("eq", ("billing_month", f"{label}-01")),
                                  ("in_", ("raw_esiid", es_list[i:i + 100]))])
        for r in rows:
            if r["raw_esiid"] in skip:
                continue
            total += float(r.get("raw_amount") or 0)
    return round(total, 2)


# ---- promotion mechanics ----------------------------------------------------

def _next_month(label: str) -> str:
    y, m = int(label[:4]), int(label[5:7])
    m += 1
    if m > 12:
        y, m = y + 1, 1
    return f"{y}-{m:02d}"


def effective_from_for_promotion(rule: str, last_qual_label: str, today: date) -> str:
    """ISO date the new split starts applying (see plan assumption #4)."""
    if rule == "IMMEDIATE":
        return f"{last_qual_label}-01"
    if rule == "NEXT_CALENDAR_MONTH":
        nxt = _next_month(f"{today.year}-{today.month:02d}")
        return f"{nxt}-01"
    if rule == "NEXT_DEAL":
        return today.isoformat()
    # NEXT_COMMISSION_PERIOD (default)
    return f"{_next_month(last_qual_label)}-01"


def _alert(db, alert_type: str, entity_id: str, message: str,
           severity: str = "info", metadata: dict = None) -> bool:
    """ai_alerts insert with open-alert dedupe (audit_notifications pattern)."""
    try:
        existing = db.table("ai_alerts").select("id").eq("type", alert_type) \
            .eq("entity_id", str(entity_id)).eq("status", "open").limit(1).execute().data
        if existing:
            return False
        db.table("ai_alerts").insert({
            "type": alert_type, "entity_type": "sgp_agent", "entity_id": str(entity_id),
            "message": message[:500], "severity": severity, "status": "open",
            "metadata": metadata or {},
        }).execute()
        return True
    except Exception:
        return False


def evaluate_agent(db, agent: dict, tiers: list, settings: dict, book: dict,
                   today: Optional[date] = None, backfill_from: Optional[str] = None,
                   actor: str = "system") -> dict:
    """Idempotent: record qualifying months + apply any earned promotions."""
    today = today or date.today()
    name = agent.get("name") or ""
    out = {"agent_id": agent["id"], "name": name, "tier_before": agent.get("current_tier"),
           "tier_after": agent.get("current_tier"), "new_qualifying": [], "warnings": []}

    ok, reason = is_eligible(agent)
    if not ok:
        out["skipped"] = reason
        return out

    esiids = agent_esiids(book, name)
    if not esiids:
        out["warnings"].append(f"{name}: SGP Agent with no deals carrying their name — no GP to evaluate.")
        return out

    # evaluation window: complete statement months from the agreement's
    # effective month (or the audited backfill floor) up to last month
    eff = agent.get("agreement_effective_at") or agent.get("agreement_approved_at")
    if not eff:
        out["warnings"].append(f"{name}: approved but agreement_effective_at is missing — cannot evaluate.")
        return out
    floor = (backfill_from or str(eff)[:7])
    current_label = f"{today.year}-{today.month:02d}"
    window = [m for m in statement_months(db) if floor <= m < current_label]
    if not window:
        return out

    existing = {(r["tier_order"], str(r["qualifying_month"])[:7])
                for r in fetch_all(db, "sgp_tier_progress", "tier_order,qualifying_month",
                                   filters=[("eq", ("agent_id", agent["id"]))])}
    milestone_tiers = [t for t in tiers if t["tier_order"] > 1]

    gp_cache: dict = {}
    for label in window:
        missing = [t for t in milestone_tiers if (t["tier_order"], label) not in existing]
        if not missing:
            continue
        gp = gp_cache.setdefault(
            label, monthly_gp(db, esiids, label, settings["qualification_basis"]))
        for t in missing:
            if gp >= float(t["monthly_gp_threshold"]):
                db.table("sgp_tier_progress").insert({
                    "agent_id": agent["id"], "tier_order": t["tier_order"],
                    "qualifying_month": f"{label}-01", "eligible_gp": gp,
                    "basis": settings["qualification_basis"],
                }).execute()
                existing.add((t["tier_order"], label))
                out["new_qualifying"].append({"month": label, "tier": t["tier_order"], "gp": gp})
        if any(q["month"] == label for q in out["new_qualifying"]):
            best = max(q["tier"] for q in out["new_qualifying"] if q["month"] == label)
            _alert(db, "sgp_qualifying_month", f"{agent['id']}:{label}",
                   f"{name} earned a qualifying month for {label}: ${gp:,.2f} eligible GP "
                   f"(counts toward the {next(t['name'] for t in tiers if t['tier_order'] == best)} tier).",
                   metadata={"agent": name, "month": label, "gp": gp})

    # sequential promotion — one month can complete several tiers, so keep
    # stepping while the NEXT tier has enough qualifying months. Never step down.
    by_order = {t["tier_order"]: t for t in tiers}
    cur = int(agent.get("current_tier") or 1)
    promoted = []
    while True:
        nxt = by_order.get(cur + 1)
        if not nxt:
            break
        quals = sorted(m for (o, m) in existing if o == nxt["tier_order"])
        if len(quals) < int(nxt["required_qualifying_months"]):
            break
        eff_date = effective_from_for_promotion(
            settings["promotion_effective_rule"], quals[int(nxt["required_qualifying_months"]) - 1], today)
        db.table("sales_agents").update({
            "current_tier": nxt["tier_order"], "tier_effective_from": eff_date,
        }).eq("id", agent["id"]).execute()
        db.table("sgp_tier_history").insert({
            "agent_id": agent["id"], "previous_tier": cur, "new_tier": nxt["tier_order"],
            "reason": (f"Reached ${float(nxt['monthly_gp_threshold']):,.0f} monthly GP in "
                       f"{nxt['required_qualifying_months']} months: {', '.join(quals[:int(nxt['required_qualifying_months'])])}"),
            "effective_from": eff_date, "promoted_by": actor, "automatic": actor == "system",
        }).execute()
        audit(db, "sales_agents", agent["id"], "sgp_tier_promotion",
              {"current_tier": cur}, {"current_tier": nxt["tier_order"], "effective_from": eff_date},
              reason=f"Permanent promotion to {nxt['name']} ({nxt['agent_split']:g}%)", actor=actor)
        _alert(db, "sgp_tier_unlocked", f"{agent['id']}:{nxt['tier_order']}",
               f"{name} permanently unlocked the {nxt['name']} tier — "
               f"{float(nxt['agent_split']):g}/{float(nxt['company_split']):g} split, effective {eff_date}.",
               severity="info", metadata={"agent": name, "tier": nxt["tier_order"]})
        promoted.append(nxt["tier_order"])
        cur = nxt["tier_order"]
        agent["current_tier"] = cur

    # "2 of 3" nudge for the next tier
    nxt = by_order.get(cur + 1)
    if nxt:
        have = sum(1 for (o, _m) in existing if o == nxt["tier_order"])
        need = int(nxt["required_qualifying_months"])
        if have == need - 1:
            _alert(db, "sgp_two_of_three", f"{agent['id']}:{nxt['tier_order']}:{have}",
                   f"{name} has completed {have} of {need} qualifying months for the "
                   f"{float(nxt['agent_split']):g}% tier — one more ${float(nxt['monthly_gp_threshold']):,.0f} "
                   f"month unlocks it permanently.")

    out["tier_after"] = cur
    out["promoted_to"] = promoted
    return out


def evaluate_all(db, agent_id: Optional[str] = None, backfill_from: Optional[str] = None,
                 actor: str = "system", today: Optional[date] = None) -> dict:
    """Evaluate every SGP-classified agent (or one). Safe to run repeatedly."""
    from app.services.agent_commission_engine import load_deal_book
    try:
        agents = fetch_all(db, "sales_agents", "*")
    except Exception as e:
        return {"error": f"sales_agents unavailable: {str(e)[:120]}"}
    if agent_id:
        agents = [a for a in agents if a["id"] == agent_id]
    sgp_agents = [a for a in agents if (a.get("classification") or "") == "SGP_AGENT"]

    tiers = load_tiers(db)
    settings = get_settings(db)
    book = load_deal_book(db)

    results, warnings = [], []
    for a in sgp_agents:
        r = evaluate_agent(db, a, tiers, settings, book,
                           today=today, backfill_from=backfill_from, actor=actor)
        results.append(r)
        warnings.extend(r.get("warnings", []))

    # validation sweep: misclassification / identity problems
    for a in agents:
        if (a.get("classification") or "") == "REFERRAL_PARTNER" and a.get("current_tier"):
            warnings.append(f"{a.get('name')}: REFERRAL_PARTNER has an SGP tier set — "
                            f"referral partners must not hold tiers; review the classification.")
    return {"evaluated": len(sgp_agents), "results": results, "warnings": warnings,
            "settings": settings}


def run_monthly_evaluation() -> dict:
    from app.db.client import get_client
    return evaluate_all(get_client())


# ---- payout-engine hook -----------------------------------------------------

def apply_sgp_overrides(db, plans: dict, payout_label: str) -> dict:
    """Replace eligible SGP agents' plan components with their tier split for
    one payout month. The tier that applies is resolved from the PERMANENT
    history (last promotion effective on/before the payout month), so a
    NEXT_COMMISSION_PERIOD promotion correctly pays the old split for its own
    qualifying month. Agents not classified SGP_AGENT are returned untouched."""
    from app.services.agent_commission_engine import norm_name
    try:
        agents = fetch_all(
            db, "sales_agents",
            "id,name,classification,agreement_status,sgp_suspended,agreement_terminated_at,"
            "current_tier,tier_effective_from,agreement_effective_at,agreement_approved_at")
    except Exception:
        return plans  # migration not applied — legacy behavior everywhere
    sgp = [a for a in agents if (a.get("classification") or "") == "SGP_AGENT"]
    if not sgp:
        return plans

    tiers = {t["tier_order"]: t for t in load_tiers(db)}
    settings = get_settings(db)
    label_end = f"{payout_label}-31"

    history = {}
    try:
        for h in fetch_all(db, "sgp_tier_history", "agent_id,new_tier,effective_from",
                           filters=[("in_", ("agent_id", [a["id"] for a in sgp]))]):
            history.setdefault(h["agent_id"], []).append(h)
    except Exception:
        pass
    for rows in history.values():
        rows.sort(key=lambda h: (str(h["effective_from"]), h["new_tier"]))

    for a in sgp:
        plan = plans.get(norm_name(a.get("name")))
        if plan is None:
            continue
        ok, reason = is_eligible(a)
        if not ok:
            plan["sgp_warning"] = (f"{a.get('name')} is classified SGP Agent but is {reason} — "
                                   f"paying their legacy plan instead of an SGP tier.")
            continue
        applicable = [h for h in history.get(a["id"], [])
                      if str(h["effective_from"])[:10] <= label_end]
        if not applicable:
            plan["sgp_warning"] = (f"{a.get('name')}: no SGP tier was effective yet in "
                                   f"{payout_label} — paying their legacy plan for this month.")
            continue
        tier_order = applicable[-1]["new_tier"]
        tier = tiers.get(tier_order)
        if not tier:
            continue
        split = float(tier["agent_split"])
        plan["components"] = [{"type": "percent_of_commission", "percent": split, "supplier": None}]
        plan["sgp_tier"] = tier_order
        plan["sgp_split"] = split
        plan["sgp_tier_name"] = tier["name"]
        if settings["promotion_effective_rule"] == "NEXT_DEAL":
            plan["sgp_history"] = [
                {"effective_from": str(h["effective_from"])[:10],
                 "split": float(tiers[h["new_tier"]]["agent_split"]) if h["new_tier"] in tiers else split}
                for h in history.get(a["id"], [])]
    return plans
