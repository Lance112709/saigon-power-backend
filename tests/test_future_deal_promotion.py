"""FUTURE -> ACTIVE promotion for crm_deals once the start date passes.

Regression for HERMOSA NAILS / PLAY ZONE (2026-10-29): the flip was silently
blocked by the duplicate-active-meter guard while the predecessor deal on the
same ESI ID was still ACTIVE.
"""
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.api.v1 import crm
from tests.fakedb import FakeDB

ESI = "1008901023814325250103"
PAST = (date.today() - timedelta(days=14)).isoformat()
OLDER = (date.today() - timedelta(days=5 * 365)).isoformat()
LATER = (date.today() - timedelta(days=3)).isoformat()
FUTURE = (date.today() + timedelta(days=30)).isoformat()


def _deal(db, **kw):
    row = {"esiid": ESI, "deal_status": "FUTURE", "contract_start_date": PAST, "customer_id": "c1"}
    row.update(kw)
    return db.table("crm_deals").insert(row).execute().data[0]


def _audits(db, rec):
    return [a["action"] for a in db.tables.get("audit_log", []) if a["record_id"] == rec]


def test_promotes_when_start_date_passed():
    db = FakeDB()
    d = _deal(db)
    deals = [dict(d)]
    crm._promote_future_deals(db, deals)
    assert deals[0]["deal_status"] == "ACTIVE"
    assert db.tables["crm_deals"][0]["deal_status"] == "ACTIVE"
    assert "deal_updated" in _audits(db, d["id"])


def test_leaves_not_yet_started_alone():
    db = FakeDB()
    d = _deal(db, contract_start_date=FUTURE)
    deals = [dict(d)]
    crm._promote_future_deals(db, deals)
    assert deals[0]["deal_status"] == "FUTURE"


def test_retires_active_predecessor_on_same_esi():
    """Hermosa Nails: Tara deal re-activated by a statement import stayed ACTIVE
    past the NRG renewal's start date and blocked the flip."""
    db = FakeDB()
    old = _deal(db, deal_status="ACTIVE", contract_start_date=OLDER, provider="Tara Energy")
    new = _deal(db, provider="NRG")
    deals = [dict(old), dict(new)]
    crm._promote_future_deals(db, deals)
    by_id = {r["id"]: r for r in db.tables["crm_deals"]}
    assert by_id[old["id"]]["deal_status"] == "RENEWED"
    assert by_id[new["id"]]["deal_status"] == "ACTIVE"
    # in-memory list the caller renders is updated too
    assert {d["id"]: d["deal_status"] for d in deals} == {old["id"]: "RENEWED", new["id"]: "ACTIVE"}
    assert "deal_renewed" in _audits(db, old["id"])


def test_retires_predecessor_on_another_customer():
    """Play Zone: old contract lived as a shell deal on a duplicate customer."""
    db = FakeDB()
    old = _deal(db, deal_status="ACTIVE", contract_start_date=OLDER, customer_id="shell")
    new = _deal(db, customer_id="real")
    deals = [dict(new)]  # customer page only lists its own deals
    crm._promote_future_deals(db, deals)
    by_id = {r["id"]: r for r in db.tables["crm_deals"]}
    assert by_id[old["id"]]["deal_status"] == "RENEWED"
    assert by_id[new["id"]]["deal_status"] == "ACTIVE"


def test_does_not_retire_newer_active_deal():
    """An ACTIVE deal that started AFTER this one is not a predecessor; leave both."""
    db = FakeDB()
    newer = _deal(db, deal_status="ACTIVE", contract_start_date=LATER)
    stale = _deal(db)
    deals = [dict(stale)]
    crm._promote_future_deals(db, deals)
    by_id = {r["id"]: r for r in db.tables["crm_deals"]}
    assert by_id[newer["id"]]["deal_status"] == "ACTIVE"
    assert by_id[stale["id"]]["deal_status"] == "FUTURE"
