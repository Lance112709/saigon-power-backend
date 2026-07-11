"""Deterministic commission forecast math."""
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.commission_forecast import _add_months, build_forecast

TODAY = date(2026, 7, 10)


def deal(end_date, adder=0.008, est_kwh=1000.0, active=True):
    return {"active": active, "adder": adder, "est_kwh": est_kwh,
            "end_date": end_date, "supplier": "X"}


def test_add_months_wraps_years():
    assert _add_months("2026-11", 3) == "2027-02"
    assert _add_months("2026-01", -1) == "2025-12"


def test_base_is_trailing_average_of_complete_months():
    received = {"2026-04": 900.0, "2026-05": 1000.0, "2026-06": 1100.0,
                "2026-07": 50.0}  # current month (partial) must be excluded
    f = build_forecast(received, [], 0.0, today=TODAY)
    assert f["base_monthly"] == 1000.0
    assert f["trailing_months"] == ["2026-04", "2026-05", "2026-06"]


def test_rolloffs_reduce_projection_cumulatively():
    received = {"2026-04": 100.0, "2026-05": 100.0, "2026-06": 100.0}
    deals = [deal("2026-08-15", adder=0.01, est_kwh=2000.0),   # $20/mo ends Aug
             deal("2026-10-01", adder=0.008, est_kwh=1250.0)]  # $10/mo ends Oct
    f = build_forecast(received, deals, 0.0, today=TODAY)
    by_month = {m["month"]: m for m in f["months"]}
    assert by_month["2026-08"]["projected"] == 100.0     # still under contract in Aug
    assert by_month["2026-09"]["projected"] == 80.0      # Aug roll-off applied
    assert by_month["2026-11"]["projected"] == 70.0      # both rolled off
    assert f["renewals_at_stake_12mo"] == 30.0
    assert f["renewal_accounts_12mo"] == 2


def test_projection_never_negative_and_totals_consistent():
    received = {"2026-06": 10.0, "2026-05": 10.0, "2026-04": 10.0}
    deals = [deal("2026-08-01", adder=0.05, est_kwh=1000.0)]  # $50/mo > base
    f = build_forecast(received, deals, 0.0, today=TODAY)
    assert all(m["projected"] >= 0 for m in f["months"])
    assert f["projected_12mo_all_renewed"] == 120.0


def test_inactive_and_dateless_deals_ignored():
    received = {"2026-06": 100.0}
    deals = [deal("2026-09-01", active=False),
             deal(None)]  # month-to-month: no end date, no roll-off
    f = build_forecast(received, deals, 0.0, today=TODAY)
    assert f["renewals_at_stake_12mo"] == 0.0


def test_clawback_exposure_passthrough():
    f = build_forecast({"2026-06": 100.0}, [], 123.456, today=TODAY)
    assert f["clawback_exposure"] == 123.46
