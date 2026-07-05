"""Status-sync tests: provider statement statuses -> CRM deal statuses."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.status_sync import map_status, sync_statuses


class FQ:
    def __init__(self, db, table):
        self.db, self.t = db, table
        self._u = None
        self._id = None

    def update(self, payload):
        self._u = payload
        return self

    def eq(self, col, val):
        self._id = val
        return self

    def select(self, *a, **k): return self
    def limit(self, n): return self
    def insert(self, *a): return self

    def execute(self):
        if self._u is not None:
            self.db.updates.append((self.t, self._id, self._u))
        class R: data = []; count = 0
        return R()


class FakeDB:
    def __init__(self):
        self.updates = []

    def table(self, name):
        return FQ(self, name)


E1, E2, E3 = "1008901000000000000001", "1008901000000000000002", "1008901000000000000003"


def deal(esiid, source="lead_deals", active=True, provider_status=None):
    return {"source": source, "id": f"d-{esiid}", "active": active,
            "provider_status": provider_status, "esiid": esiid}


def row(esiid, status, label="2026-06"):
    return {"esiid": esiid, "provider_status": status, "statement_label": label}


def deals_of(*ds):
    return {"by_esiid": {d["esiid"]: d for d in ds}}


def test_map_status_variants():
    assert map_status("Active") == "active"
    assert map_status("Inactive") == "inactive"           # must not match 'active'
    assert map_status("Going Final") == "going_final"
    assert map_status("Inactive Collections") == "inactive"
    assert map_status("Switch Back") is None              # billing code, not a status
    assert map_status("") is None


def test_inactive_deactivates_deal():
    db = FakeDB()
    s = sync_statuses(db, [row(E1, "Inactive"), row(E2, "Active"), row(E3, "Active")],
                      deals_of(deal(E1), deal(E2), deal(E3)), "test", "tester")
    assert s["deactivated"] == 1 and s["pending"] is False
    tables = [(t, u.get("status")) for t, _, u in db.updates if "status" in u]
    assert ("lead_deals", "Inactive") in tables


def test_active_reactivates_deal():
    db = FakeDB()
    s = sync_statuses(db, [row(E1, "Active"), row(E2, "Active"), row(E3, "Active")],
                      deals_of(deal(E1, source="crm_deals", active=False), deal(E2), deal(E3)),
                      "test", "tester")
    assert s["reactivated"] == 1
    assert any(u.get("deal_status") == "ACTIVE" for _, _, u in db.updates)


def test_going_final_flags_but_keeps_active():
    db = FakeDB()
    s = sync_statuses(db, [row(E1, "Going Final"), row(E2, "Active"), row(E3, "Active")],
                      deals_of(deal(E1), deal(E2), deal(E3)), "test", "tester")
    assert s["going_final"] == 1
    # provider_status stamped but no status/deal_status flip
    flips = [u for _, _, u in db.updates if "status" in u or "deal_status" in u]
    assert flips == []


def test_mass_churn_held_for_review():
    db = FakeDB()
    s = sync_statuses(db, [row(E1, "Inactive"), row(E2, "Inactive"), row(E3, "Active")],
                      deals_of(deal(E1), deal(E2), deal(E3)), "test", "tester")
    assert s["pending"] is True
    assert s["deactivated"] == 2      # reported, not applied
    assert db.updates == []           # nothing touched


def test_force_applies_despite_mass_churn():
    db = FakeDB()
    s = sync_statuses(db, [row(E1, "Inactive"), row(E2, "Inactive"), row(E3, "Active")],
                      deals_of(deal(E1), deal(E2), deal(E3)), "test", "tester", force=True)
    assert s["pending"] is False
    assert s["deactivated"] == 2
    assert len([u for _, _, u in db.updates if u.get("status") == "Inactive"]) == 2


def test_unchanged_status_skips_write():
    db = FakeDB()
    s = sync_statuses(db, [row(E1, "Active")],
                      deals_of(deal(E1, provider_status="Active")), "test", "tester")
    assert db.updates == []
    assert s["confirmed_active"] == 1
