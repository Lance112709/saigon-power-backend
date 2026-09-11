"""Pure-logic tests for the commission payments API helpers."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.api.v1.commission_payments import month_status, norm_es


def test_norm_es_strips_formatting():
    assert norm_es(" 1008901-0231234567890 ") == "10089010231234567890"
    assert norm_es(None) == ""


def test_month_status_maps_recon_verdicts():
    items = [
        {"esiid": "10443720008297350", "billing_month": "2026-05-01", "status": "matched"},
        {"esiid": "10443720008297350", "billing_month": "2026-04-01", "status": "short_paid"},
        {"esiid": "10443720008297350", "billing_month": "2026-03-01", "status": "missing"},
    ]
    st = month_status(items)
    assert st[("10443720008297350", "2026-05")] == "paid"
    assert st[("10443720008297350", "2026-04")] == "partial"
    assert st[("10443720008297350", "2026-03")] == "unpaid"


def test_month_status_worst_verdict_wins():
    items = [
        {"esiid": "10443720008297350", "billing_month": "2026-05-01", "status": "matched"},
        {"esiid": "10443720008297350", "billing_month": "2026-05-01", "status": "missing"},
    ]
    assert month_status(items)[("10443720008297350", "2026-05")] == "unpaid"


def test_paid_summary_groups_by_month_and_agent(monkeypatch):
    from app.api.v1 import agent_commissions as mod
    from tests.fakedb import FakeDB
    rows = [
        {"id": "1", "agent_name": "Nga Nguyen", "year": 2026, "month": 5, "status": "paid", "total_commission": 85, "total_deals": 17, "paid_at": "2026-06-10T12:00:00+00:00", "paid_by": "Lance"},
        {"id": "2", "agent_name": "Nga Nguyen", "year": 2026, "month": 6, "status": "closed_out", "total_commission": 150, "total_deals": 31, "paid_at": None, "paid_by": None},
        {"id": "3", "agent_name": "Jennie Duong", "year": 2026, "month": 5, "status": "paid", "total_commission": 335, "total_deals": 67, "paid_at": "2026-06-11T12:00:00+00:00", "paid_by": "Lance"},
        {"id": "4", "agent_name": "Jennie Duong", "year": 2025, "month": 12, "status": "paid", "total_commission": 40, "total_deals": 8, "paid_at": "2026-01-05T12:00:00+00:00", "paid_by": "Lance"},
        {"id": "5", "agent_name": "Tai", "year": 2026, "month": 7, "status": "calculated", "total_commission": 20, "total_deals": 4, "paid_at": None, "paid_by": None},
    ]
    db = FakeDB(); db.tables["agent_commissions"] = rows
    monkeypatch.setattr(mod, "get_client", lambda: db)
    user = type("U", (), {"name": "Lance", "email": "l@x"})()
    ytd = mod.paid_summary("2026-01", "2026-12", user)
    assert ytd["records"] == 4
    assert ytd["totals"] == {"paid": 420.0, "owed": 150.0, "pending": 20.0}
    assert [(m["month"], m["paid"], m["owed"]) for m in ytd["by_month"]] == [(5, 420.0, 0.0), (6, 0.0, 150.0), (7, 0.0, 0.0)]
    assert ytd["by_agent"][0]["agent_name"] == "Jennie Duong" and ytd["by_agent"][0]["paid"] == 335.0
    assert ytd["by_agent"][1]["months_paid"] == 1 and ytd["by_agent"][1]["last_paid_at"].startswith("2026-06-10")
    everything = mod.paid_summary(None, None, user)
    assert everything["totals"]["paid"] == 460.0
    one = mod.paid_summary("2026-05", "2026-05", user)
    assert one["totals"] == {"paid": 420.0, "owed": 0.0, "pending": 0.0}
