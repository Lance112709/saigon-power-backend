"""SGP Agent tier structure: eligibility, GP, permanent promotions, payouts."""
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from fakedb import FakeDB
from app.services.sgp_tiers import (
    apply_sgp_overrides, effective_from_for_promotion, evaluate_all,
    is_eligible, monthly_gp,
)
from app.services.agent_commission_engine import calculate_month, load_agent_plans

TODAY = date(2026, 7, 11)
AGENT_ID = "00000000-0000-0000-0000-0000000000a1"


def tiers_seed():
    return [
        {"tier_order": 1, "name": "Partner", "monthly_gp_threshold": 0, "required_qualifying_months": 0,
         "agent_split": 50, "company_split": 50, "is_max": False, "active": True},
        {"tier_order": 2, "name": "Growth Partner", "monthly_gp_threshold": 5000, "required_qualifying_months": 3,
         "agent_split": 55, "company_split": 45, "is_max": False, "active": True},
        {"tier_order": 3, "name": "Senior Partner", "monthly_gp_threshold": 10000, "required_qualifying_months": 3,
         "agent_split": 60, "company_split": 40, "is_max": False, "active": True},
        {"tier_order": 4, "name": "Premier Partner", "monthly_gp_threshold": 15000, "required_qualifying_months": 3,
         "agent_split": 65, "company_split": 35, "is_max": False, "active": True},
        {"tier_order": 5, "name": "Elite Partner", "monthly_gp_threshold": 20000, "required_qualifying_months": 3,
         "agent_split": 70, "company_split": 30, "is_max": True, "active": True},
    ]


def sgp_agent(**kw):
    return {"id": AGENT_ID, "name": "Test Agent", "email": None, "phone": None,
            "commission_rules": {"components": [{"type": "flat_per_deal", "amount": 20, "supplier": None}]},
            "classification": "SGP_AGENT", "agreement_status": "APPROVED",
            "agreement_effective_at": "2026-01-01", "agreement_approved_at": "2026-01-01T00:00:00Z",
            "agreement_terminated_at": None, "sgp_suspended": False,
            "current_tier": 1, "tier_effective_from": "2026-01-01", **kw}


def es(n):
    return f"10089010000000000{n:05d}"


def make_db(agent=None, months_gp=None, extra_agents=None):
    """months_gp: {'2026-03': 6000.0} — creates one commission row per month
    on the agent's single deal ESIID, plus the statement-month markers."""
    db = FakeDB()
    db.tables["sgp_tiers"] = tiers_seed()
    db.tables["sgp_settings"] = [{"id": 1, "qualification_basis": "PROVIDER_PAID_GP",
                                  "promotion_effective_rule": "NEXT_COMMISSION_PERIOD"}]
    agents = [agent or sgp_agent()] + (extra_agents or [])
    db.tables["sales_agents"] = agents
    db.tables["crm_deals"] = [{
        "id": f"deal-{a['id']}", "deal_status": "ACTIVE", "provider": "Discount Power",
        "esiid": es(i), "adder": 0.008, "product_type": "Fixed Rate", "contract_term": "36",
        "sales_agent": a["name"], "business_name": None, "provider_status": None,
        "contract_start_date": "2026-01-05", "crm_customers": None, "customer_id": None,
    } for i, a in enumerate(agents)]
    db.tables["actual_commissions"] = []
    db.tables["reconciliation_runs"] = []
    for i, a in enumerate(agents):
        for m, gp in (months_gp or {}).items():
            db.tables["actual_commissions"].append({
                "raw_esiid": es(i), "raw_amount": gp, "raw_kwh": 1000.0,
                "billing_month": f"{m}-01", "supplier_id": "sup1", "suppliers": None})
            db.tables["reconciliation_runs"].append(
                {"billing_month": f"{m}-01", "notes": '{"engine": "v2"}'})
    return db


def promotions(db):
    return [(h["previous_tier"], h["new_tier"]) for h in db.tables.get("sgp_tier_history", [])]


# ── eligibility gates ─────────────────────────────────────────────────────────

def test_eligibility_gates():
    assert is_eligible(sgp_agent())[0] is True
    for bad in (
        sgp_agent(classification=None),
        sgp_agent(classification="REFERRAL_PARTNER"),
        sgp_agent(classification="INTERNAL_EMPLOYEE"),
        sgp_agent(agreement_status="SIGNED"),
        sgp_agent(agreement_status="PENDING_SIGNATURE"),
        sgp_agent(sgp_suspended=True),
        sgp_agent(agreement_terminated_at="2026-06-01T00:00:00Z"),
    ):
        assert is_eligible(bad)[0] is False


def test_referral_partner_fully_excluded():
    ref = sgp_agent(classification="REFERRAL_PARTNER", name="Ref Partner",
                    id="00000000-0000-0000-0000-0000000000r1", current_tier=None)
    db = make_db(agent=ref, months_gp={"2026-03": 50000.0, "2026-04": 50000.0, "2026-05": 50000.0})
    out = evaluate_all(db, today=TODAY)
    assert out["evaluated"] == 0                      # not even evaluated
    assert db.tables.get("sgp_tier_progress", []) == []
    assert promotions(db) == []


# ── GP math ───────────────────────────────────────────────────────────────────

def test_provider_paid_gp_scopes_to_agent_esiids_and_month():
    db = make_db(months_gp={"2026-03": 6000.0})
    db.tables["actual_commissions"] += [
        {"raw_esiid": es(99), "raw_amount": 9999.0, "billing_month": "2026-03-01"},  # not my account
        {"raw_esiid": es(0), "raw_amount": 500.0, "billing_month": "2026-04-01"},    # other month
    ]
    assert monthly_gp(db, {es(0)}, "2026-03", "PROVIDER_PAID_GP") == 6000.0


def test_finalized_gp_excludes_open_disputes():
    db = make_db(months_gp={"2026-03": 6000.0})
    db.tables["exception_cases"] = [{"esiid": es(0), "billing_month": "2026-03-01",
                                     "workflow_status": "open"}]
    assert monthly_gp(db, {es(0)}, "2026-03", "FINALIZED_GP") == 0.0
    db.tables["exception_cases"][0]["workflow_status"] = "resolved"
    assert monthly_gp(db, {es(0)}, "2026-03", "FINALIZED_GP") == 6000.0


# ── qualifying window ─────────────────────────────────────────────────────────

def test_months_before_agreement_and_current_month_never_count():
    db = make_db(agent=sgp_agent(agreement_effective_at="2026-04-01"),
                 months_gp={"2026-02": 8000.0,   # before agreement
                            "2026-07": 8000.0})  # current month (incomplete)
    evaluate_all(db, today=TODAY)
    assert db.tables.get("sgp_tier_progress", []) == []


def test_backfill_from_widens_window_for_migrated_history():
    db = make_db(agent=sgp_agent(agreement_effective_at="2026-06-01"),
                 months_gp={"2026-02": 8000.0, "2026-03": 8000.0, "2026-04": 8000.0})
    evaluate_all(db, today=TODAY)
    assert db.tables.get("sgp_tier_progress", []) == []          # outside window
    evaluate_all(db, backfill_from="2026-01", actor="admin@x", today=TODAY)
    assert len(db.tables["sgp_tier_progress"]) == 3              # tier-2 rows
    assert promotions(db) == [(1, 2)]


# ── qualifying months & promotion ────────────────────────────────────────────

def test_nonconsecutive_months_promote_permanently():
    db = make_db(months_gp={"2026-01": 5200.0, "2026-02": 900.0, "2026-03": 6100.0,
                            "2026-04": 800.0, "2026-06": 5000.0})
    out = evaluate_all(db, today=TODAY)
    assert promotions(db) == [(1, 2)]
    agent = db.tables["sales_agents"][0]
    assert agent["current_tier"] == 2
    # NEXT_COMMISSION_PERIOD: 3rd qualifying month = 2026-06 → effective 2026-07-01
    assert agent["tier_effective_from"] == "2026-07-01"
    assert out["results"][0]["promoted_to"] == [2]


def test_idempotent_reevaluation_no_duplicates():
    db = make_db(months_gp={"2026-01": 5200.0, "2026-02": 6000.0, "2026-03": 6100.0})
    evaluate_all(db, today=TODAY)
    p1 = len(db.tables["sgp_tier_progress"])
    h1 = len(db.tables["sgp_tier_history"])
    a1 = len(db.tables.get("ai_alerts", []))
    evaluate_all(db, today=TODAY)
    evaluate_all(db, today=TODAY)
    assert len(db.tables["sgp_tier_progress"]) == p1
    assert len(db.tables["sgp_tier_history"]) == h1
    assert len(db.tables.get("ai_alerts", [])) == a1
    assert db.tables["sales_agents"][0]["current_tier"] == 2


def test_one_month_counts_toward_multiple_tiers():
    db = make_db(months_gp={"2026-03": 12000.0})
    evaluate_all(db, today=TODAY)
    rows = db.tables["sgp_tier_progress"]
    assert {r["tier_order"] for r in rows} == {2, 3}   # $12k satisfies $5k AND $10k
    assert all(str(r["qualifying_month"])[:7] == "2026-03" for r in rows)


def test_sequential_multistep_promotion_in_one_run():
    db = make_db(months_gp={"2026-01": 11000.0, "2026-02": 10500.0, "2026-03": 12000.0})
    evaluate_all(db, today=TODAY)
    assert promotions(db) == [(1, 2), (2, 3)]          # steps through, never skips
    assert db.tables["sales_agents"][0]["current_tier"] == 3


def test_hard_cap_at_tier_five():
    db = make_db(agent=sgp_agent(current_tier=5),
                 months_gp={"2026-01": 90000.0, "2026-02": 90000.0, "2026-03": 90000.0})
    evaluate_all(db, today=TODAY)
    assert db.tables["sales_agents"][0]["current_tier"] == 5
    assert promotions(db) == []


def test_never_demoted():
    db = make_db(agent=sgp_agent(current_tier=3, tier_effective_from="2026-01-01"),
                 months_gp={"2026-04": 0.0, "2026-05": 12.0, "2026-06": 0.0})
    evaluate_all(db, today=TODAY)
    assert db.tables["sales_agents"][0]["current_tier"] == 3


def test_two_of_three_alert_fires_once():
    db = make_db(months_gp={"2026-01": 5200.0, "2026-03": 6100.0})
    evaluate_all(db, today=TODAY)
    evaluate_all(db, today=TODAY)
    nudges = [a for a in db.tables.get("ai_alerts", []) if a["type"] == "sgp_two_of_three"]
    assert len(nudges) == 1
    assert "2 of 3" in nudges[0]["message"]


# ── effective-date rules ──────────────────────────────────────────────────────

def test_effective_rules():
    assert effective_from_for_promotion("IMMEDIATE", "2026-06", TODAY) == "2026-06-01"
    assert effective_from_for_promotion("NEXT_COMMISSION_PERIOD", "2026-06", TODAY) == "2026-07-01"
    assert effective_from_for_promotion("NEXT_CALENDAR_MONTH", "2026-06", TODAY) == "2026-08-01"
    assert effective_from_for_promotion("NEXT_DEAL", "2026-06", TODAY) == "2026-07-11"
    assert effective_from_for_promotion("NEXT_COMMISSION_PERIOD", "2026-12", TODAY) == "2027-01-01"


# ── payout-engine integration ────────────────────────────────────────────────

def with_history(db, entries):
    db.tables["sgp_tier_history"] = [
        {"agent_id": AGENT_ID, "previous_tier": p, "new_tier": n, "effective_from": eff,
         "reason": "", "promoted_by": "system", "automatic": True}
        for p, n, eff in entries]


def test_engine_pays_tier_split_with_provenance():
    db = make_db(agent=sgp_agent(current_tier=2, tier_effective_from="2026-03-01"),
                 months_gp={"2026-03": 1000.0})
    with_history(db, [(None, 1, "2026-01-01"), (1, 2, "2026-03-01")])
    result = calculate_month(db, 2026, 3)
    bucket = result["agents"]["Test Agent"]
    assert bucket["total"] == 550.0                     # 55% of $1,000
    assert "SGP Tier 2 (55%)" in bucket["deals"][0]["applied"]
    # legacy flat_per_deal component must NOT also pay (components replaced)
    assert bucket["bonuses"] == 0.0


def test_promotion_effective_next_period_pays_old_split_for_qualifying_month():
    # promoted to tier 2 effective 2026-04-01: March pays 50%, April pays 55%
    db = make_db(agent=sgp_agent(current_tier=2, tier_effective_from="2026-04-01"),
                 months_gp={"2026-03": 1000.0, "2026-04": 1000.0})
    with_history(db, [(None, 1, "2026-01-01"), (1, 2, "2026-04-01")])
    assert calculate_month(db, 2026, 3)["agents"]["Test Agent"]["total"] == 500.0
    assert calculate_month(db, 2026, 4)["agents"]["Test Agent"]["total"] == 550.0


def test_zero_behavior_change_for_unclassified_agents():
    db = make_db(agent=sgp_agent(classification=None, current_tier=None),
                 months_gp={"2026-03": 1000.0})
    plain = load_agent_plans(db)
    labeled = load_agent_plans(db, payout_label="2026-03")
    assert plain == labeled                              # deep equal — byte-identical
    # legacy plan still pays: flat_per_deal $20 on the first payment
    assert calculate_month(db, 2026, 3)["agents"]["Test Agent"]["total"] == 20.0


def test_not_approved_sgp_agent_pays_legacy_with_warning():
    db = make_db(agent=sgp_agent(agreement_status="SENT", current_tier=None),
                 months_gp={"2026-03": 1000.0})
    result = calculate_month(db, 2026, 3)
    assert result["agents"]["Test Agent"]["total"] == 20.0    # legacy flat_per_deal
    assert any("has not been approved" in w for w in result["warnings"])


def test_next_deal_rule_pays_per_deal_start():
    db = make_db(agent=sgp_agent(current_tier=2, tier_effective_from="2026-03-10"),
                 months_gp={"2026-03": 1000.0})
    db.tables["sgp_settings"][0]["promotion_effective_rule"] = "NEXT_DEAL"
    with_history(db, [(None, 1, "2026-01-01"), (1, 2, "2026-03-10")])
    # existing deal started 2026-01-05 (before the promotion) → keeps 50%
    assert calculate_month(db, 2026, 3)["agents"]["Test Agent"]["total"] == 500.0
    # a deal started after the promotion date → 55%
    db.tables["crm_deals"][0]["contract_start_date"] = "2026-03-15"
    assert calculate_month(db, 2026, 3)["agents"]["Test Agent"]["total"] == 550.0


def test_apply_overrides_missing_tables_falls_back():
    db = FakeDB()  # no sales_agents/sgp tables at all
    plans = {"x": {"id": "1", "name": "X", "rules": {}, "components": []}}
    assert apply_sgp_overrides(db, dict(plans), "2026-03") == plans
