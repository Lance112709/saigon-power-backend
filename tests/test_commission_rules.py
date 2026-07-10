"""Commission rules engine: evaluation math + versioning."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from fakedb import FakeDB
from app.services.commission_rules import (
    create_rule_version, evaluate_rule, get_rule_for_month, rule_rate,
)

SUP = "00000000-0000-0000-0000-000000000001"


def r(rule_type, config):
    return {"rule_type": rule_type, "config": config}


# -- evaluate_rule ------------------------------------------------------------

def test_fixed_rate_per_kwh():
    amount, rate = evaluate_rule(r("rate_per_kwh", {"rate": 0.008, "rate_source": "fixed"}),
                                 1000, 0.005)
    assert amount == 8.0 and rate == 0.008


def test_deal_adder_rate_source():
    amount, rate = evaluate_rule(r("rate_per_kwh", {"rate_source": "deal_adder"}), 1000, 0.007)
    assert amount == 7.0 and rate == 0.007


def test_deal_adder_source_without_adder_falls_back():
    assert evaluate_rule(r("rate_per_kwh", {"rate_source": "deal_adder"}), 1000, None) is None


def test_rate_needs_kwh():
    assert evaluate_rule(r("rate_per_kwh", {"rate": 0.008}), None, None) is None


def test_flat_fee_ignores_kwh():
    amount, rate = evaluate_rule(r("flat_fee", {"flat_amount": 50}), None, None)
    assert amount == 50.0 and rate is None


def test_tiered_boundaries():
    tiers = [{"min_kwh": 0, "max_kwh": 1000, "rate": 0.005},
             {"min_kwh": 1000, "max_kwh": None, "rate": 0.008}]
    assert evaluate_rule(r("tiered", {"tiers": tiers}), 999, None)[1] == 0.005
    assert evaluate_rule(r("tiered", {"tiers": tiers}), 1000, None)[1] == 0.008
    assert evaluate_rule(r("tiered", {"tiers": tiers}), 50000, None)[1] == 0.008


def test_hybrid_flat_plus_rate():
    amount, rate = evaluate_rule(
        r("hybrid", {"flat_amount": 25, "rate": 0.004, "rate_source": "fixed"}), 1000, None)
    assert amount == 29.0 and rate == 0.004


def test_unknown_rule_type():
    assert evaluate_rule(r("percentage", {"rate": 5}), 1000, 0.008) is None


def test_rule_rate_flat_fee_is_none():
    assert rule_rate(r("flat_fee", {"flat_amount": 50}), 1000, 0.008) is None


# -- versioning ---------------------------------------------------------------

def _payload(effective_from, rate=0.008, notes=None):
    return {"name": "DP residential", "rule_type": "rate_per_kwh",
            "config": {"rate": rate, "rate_source": "fixed"},
            "effective_from": effective_from, "notes": notes}


def test_new_version_closes_previous_and_never_deletes():
    db = FakeDB()
    v1 = create_rule_version(db, SUP, _payload("2024-01-01", rate=0.008), "test")
    v2 = create_rule_version(db, SUP, _payload("2026-06-01", rate=0.009), "test")
    rules = db.tables["commission_rules"]
    assert len(rules) == 2  # nothing deleted
    closed = next(x for x in rules if x["id"] == v1["id"])
    assert closed["effective_to"] == "2026-06-01"
    assert closed["superseded_by"] == v2["id"]
    assert v2["version"] == 2


def test_get_rule_for_month_picks_by_effective_date():
    db = FakeDB()
    create_rule_version(db, SUP, _payload("2024-01-01", rate=0.008), "test")
    create_rule_version(db, SUP, _payload("2026-06-01", rate=0.009), "test")
    assert get_rule_for_month(db, SUP, "2026-03")["config"]["rate"] == 0.008
    assert get_rule_for_month(db, SUP, "2026-07")["config"]["rate"] == 0.009
    assert get_rule_for_month(db, SUP, "2023-12") is None
