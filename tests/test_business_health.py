"""Pure-math tests for the Business Health panel."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.business_health import month_over_month, ltv_math, confirmed_flow


def test_month_over_month_counts():
    prev = {"a", "b", "c"}
    cur = {"b", "c", "d", "e"}
    mm = month_over_month(prev, cur)
    assert mm == {"gained": 2, "lost": 1, "net": 1}


def test_month_over_month_no_change():
    s = {"a", "b"}
    assert month_over_month(s, set(s)) == {"gained": 0, "lost": 0, "net": 0}


def test_ltv_math_basic():
    # $100k over 10k account-months => $10 ARPA; 2%/mo churn => 50-mo lifetime
    r = ltv_math(100_000, 10_000, 0.02)
    assert r["arpa"] == 10.0
    assert r["expected_lifetime_months"] == 50.0
    assert r["ltv_per_account"] == 500.0
    assert r["monthly_churn_pct"] == 2.0


def test_ltv_lifetime_clamped():
    assert ltv_math(1000, 100, 0.0)["expected_lifetime_months"] == 72.0   # no churn observed
    assert ltv_math(1000, 100, 0.5)["expected_lifetime_months"] == 6.0    # absurd churn month


def test_ltv_zero_account_months():
    assert ltv_math(0, 0, 0.02)["arpa"] == 0.0


def test_confirmed_flow_ignores_billing_bounce():
    # 'b' skips cur but returns in nxt -> NOT lost; 'x' new in cur -> gained
    prev2 = {"a", "b"}
    prev = {"a", "b", "c"}
    cur = {"a", "c", "x"}
    nxt = {"a", "b", "c", "x"}
    cf = confirmed_flow(prev2, prev, cur, nxt)
    assert cf["lost"] == 0          # b bounced, c stayed
    assert cf["gained"] == 1        # x
    assert cf["net"] == 1


def test_confirmed_flow_real_loss():
    cf = confirmed_flow(set(), {"a", "b"}, {"a"}, {"a"})
    assert cf["lost"] == 1 and cf["gained"] == 0
