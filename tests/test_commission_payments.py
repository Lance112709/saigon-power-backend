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


def test_close_month_pays_everything_except_held_and_zero(monkeypatch):
    from app.api.v1 import agent_commissions as mod
    from tests.fakedb import FakeDB
    import json as _json
    db = FakeDB()
    db.tables["agent_commissions"] = [
        {"id": "a", "agent_name": "Nga Nguyen", "year": 2026, "month": 8, "status": "calculated", "total_commission": 340,
         "notes": _json.dumps({"held": 0}), "approved_at": None, "closed_out_at": None},
        {"id": "b", "agent_name": "Jennie Duong", "year": 2026, "month": 8, "status": "approved", "total_commission": 145,
         "notes": _json.dumps({"held": 1}), "approved_at": "2026-09-01", "closed_out_at": None},
        {"id": "c", "agent_name": "Tai", "year": 2026, "month": 8, "status": "calculated", "total_commission": 0,
         "notes": _json.dumps({"held": 0}), "approved_at": None, "closed_out_at": None},
        {"id": "d", "agent_name": "Old", "year": 2026, "month": 7, "status": "calculated", "total_commission": 50,
         "notes": "{}", "approved_at": None, "closed_out_at": None},
    ]
    db.tables["commission_logs"] = []
    monkeypatch.setattr(mod, "get_client", lambda: db)
    user = type("U", (), {"name": "Lance", "email": "l@x"})()
    r = mod.close_month({"year": 2026, "month": 8, "paid_at": "2026-09-10", "notes": "Zelle"}, user)
    assert [p["id"] for p in r["paid"]] == ["a"] and r["paid_total"] == 340.0
    assert {s["agent_name"] for s in r["skipped"]} == {"Jennie Duong", "Tai"}
    a = next(x for x in db.tables["agent_commissions"] if x["id"] == "a")
    assert a["status"] == "paid" and a["paid_at"].startswith("2026-09-10") and a["approved_by"] == "Lance"
    assert db.tables["commission_logs"][0]["action"] == "mark_paid" and "Zelle" in db.tables["commission_logs"][0]["notes"]
    # held rows can be forced through explicitly
    r2 = mod.close_month({"year": 2026, "month": 8, "skip_held": False}, user)
    assert [p["id"] for p in r2["paid"]] == ["b"]


def test_autorun_months_and_digest_items():
    from datetime import date
    from app.services.commission_autorun import months_to_run
    assert months_to_run(date(2026, 1, 15)) == [(2026, 1), (2025, 12), (2025, 11)]
    from app.services import commission_digest as dg
    from tests.fakedb import FakeDB
    db = FakeDB()
    db.tables["agent_commissions"] = [
        {"agent_name": "Nga Nguyen", "year": 2026, "month": 5, "status": "paid", "total_commission": 85},
        {"agent_name": "Nga Nguyen", "year": 2026, "month": 6, "status": "calculated", "total_commission": 150},
        {"agent_name": "Nga Nguyen", "year": 2026, "month": 7, "status": "paid", "total_commission": 345},
    ]
    db.tables["sales_agents"] = [{"name": "Nga Nguyen"}]
    db.tables["crm_deals"] = [{"id": "x", "sales_agent": "NGA NGUYEN", "contract_start_date": "2026-08-02"},
                              {"id": "y", "sales_agent": "Nga Nguyen", "contract_start_date": "2026-08-03"},
                              {"id": "z", "sales_agent": "Long Nguyen", "contract_start_date": "2026-08-04"},
                              {"id": "w", "sales_agent": "Old Referral", "contract_start_date": "2025-01-04"}]
    db.tables["lead_deals"] = []
    result = {"agents": {"Nga Nguyen": {"total": 600.0, "deals": [
                  {"held": True, "hold_reason": "same service address, contract also started this month", "customer": "Lanh Hoang"},
                  {"kind": "clawback", "customer": "Bad Cust", "cancelled": "2026-08-14", "commission": -5.0}]}},
              "unassigned": {"agent_not_registered": {"LONG NGUYEN": 42.5}, "no_agent_on_deal": {"esiids": 3, "gross": 12.0}},
              "warnings": ["Tai has paid deals but NO commission plan configured — payout is $0 until you set their plan."]}
    d = dg.build_digest(2026, 8, db=db, result=result)
    it = d["items"]
    assert len(it["held"]) == 1 and "Lanh Hoang" in it["held"][0]["text"]
    assert len(it["clawbacks"]) == 1 and "-5.00" in it["clawbacks"][0]["text"]
    assert it["unregistered_agents"][0]["agent"] == "LONG NGUYEN" and it["no_agent"][0]["text"].startswith("3 paid accounts")
    assert it["no_plan"][0]["agent"] == "Tai"
    assert len(it["swings"]) == 1 and "+" in it["swings"][0]["text"]       # 600 vs avg(85,150,345)=193 → big swing
    assert any("'NGA NGUYEN'" in x["text"] for x in it["name_fixes"]) and any("'Long Nguyen'" in x["text"] for x in it["name_fixes"])
    assert not any("Old Referral" in x["text"] for x in it["name_fixes"])  # legacy name outside the month is not nagged
    assert it["unpaid_older"][0]["text"].startswith("Jun 2026 $150.00")
    assert d["count"] == 9 and "Lanh Hoang" in dg.digest_html(d)


def test_agent_name_suggestions_and_explicit_renames():
    from app.services import agent_names as an
    from tests.fakedb import FakeDB
    db = FakeDB()
    db.tables["sales_agents"] = [{"name": "Tai Dinh"}, {"name": "Nga Nguyen"}, {"name": "Minh Ngo"}]
    db.tables["crm_deals"] = [{"id": "1", "sales_agent": "Tai Dinh Realtor", "contract_start_date": "2026-08-01"},
                              {"id": "2", "sales_agent": "Minh Ngo ( Arlington )", "contract_start_date": "2026-08-01"},
                              {"id": "3", "sales_agent": "NGA  NGUYEN", "contract_start_date": "2026-08-01"},
                              {"id": "4", "sales_agent": "Somebody Else", "contract_start_date": "2026-08-01"}]
    db.tables["lead_deals"] = []
    rep = an.agent_name_report(db)
    assert rep["fixable"]["NGA  NGUYEN"]["canonical"] == "Nga Nguyen"
    assert rep["unknown"]["Tai Dinh Realtor"]["suggested"] == "Tai Dinh"
    assert rep["unknown"]["Minh Ngo ( Arlington )"]["suggested"] == "Minh Ngo"
    assert rep["unknown"]["Somebody Else"]["suggested"] is None
    dry = an.normalize_deal_agent_names(db, dry_run=True, renames={"Tai Dinh Realtor": "Tai Dinh"})
    assert dry["changed"] == 2 and db.tables["crm_deals"][0]["sales_agent"] == "Tai Dinh Realtor"
    real = an.normalize_deal_agent_names(db, dry_run=False, renames={"Tai Dinh Realtor": "Tai Dinh"})
    assert real["changed"] == 2 and db.tables["crm_deals"][0]["sales_agent"] == "Tai Dinh" and db.tables["crm_deals"][2]["sales_agent"] == "Nga Nguyen"
    import pytest
    with pytest.raises(ValueError):
        an.normalize_deal_agent_names(db, dry_run=True, renames={"Somebody Else": "Nobody"})
    assert an.canonical_agent(db, "  nga   nguyen ") == "Nga Nguyen" and an.canonical_agent(db, "") is None
