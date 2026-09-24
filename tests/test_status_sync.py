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
    # NRG Business LDC Status values
    assert map_status("Enrolled") == "active"
    assert map_status("New Account") == "active"
    assert map_status("Enrollment Pending") == "active"
    assert map_status("Dropped") == "inactive"
    assert map_status("Cancelled") == "inactive"
    assert map_status("Drop Pending") == "going_final"    # win-back signal, not yet gone


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


def test_older_statement_does_not_overwrite_newer_status():
    """Back-filling Nov 2025 after Jun 2026 was already applied must not flip
    a deal Jun 2026 marked Inactive back to Active (and vice versa)."""
    db = FakeDB()
    d1 = dict(deal(E1, active=False, provider_status="Inactive"), provider_status_date="2026-06-01")
    d2 = dict(deal(E2, active=True, provider_status="Active"), provider_status_date="2026-06-01")
    d3 = dict(deal(E3, active=True, provider_status="Active"), provider_status_date="2026-06-01")
    res = sync_statuses(db, [row(E1, "Active", "2025-11"), row(E2, "Inactive", "2025-11"),
                             row(E3, "Inactive", "2026-07")],
                        deals_of(d1, d2, d3), "Heritage — old.xls", "test", force=True)
    assert res["stale_skipped"] == 2
    assert res["deactivated"] == 1
    assert [u[1] for u in db.updates] == [f"d-{E3}"]
    assert d1["active"] is False and d2["active"] is True


def test_guard_rejection_does_not_abort_sync(monkeypatch):
    """crm_deals' duplicate-ESIID trigger rejecting a reactivation must be
    recorded and skipped, not raise out of the whole import."""
    class BoomDB(FakeDB):
        def table(self, name):
            q = FQ(self, name)
            if name == "crm_deals":
                def boom():
                    raise RuntimeError("ESI ID already has an active deal in crm_deals")
                q.execute = boom
            return q
    monkeypatch.setattr("app.services.status_sync.audit", lambda *a, **k: None)
    db = BoomDB()
    blocked = deal(E1, source="crm_deals", active=False, provider_status="Inactive")
    fine = deal(E2, active=False, provider_status="Inactive")
    res = sync_statuses(db, [row(E1, "Active"), row(E2, "Active")], deals_of(blocked, fine), "src", "test")
    assert res["blocked"] == 1
    assert res["reactivated"] == 1
    assert blocked["active"] is False and fine["active"] is True


# --- absence rule (all providers) -------------------------------------------
from fakedb import FakeDB as RealFakeDB  # noqa: E402
from app.services.status_sync import (  # noqa: E402
    absence_sync, ABSENCE_SYNC_GROUPS, TRUSTED_STATUS_GROUPS, _full_statement_months)

SUP, OTHER = "sup-1", "sup-2"
FILL = [f"10089019999999999{i:05d}" for i in range(10)]   # 10 padding meters = a "full" month


def _db():
    db = RealFakeDB()
    db.table("suppliers").insert([{"id": SUP, "name": "Iron Horse"},
                                  {"id": OTHER, "name": "Budget Power"}]).execute()
    return db


def _ledger(db, months_esiids, supplier=SUP, pad=True):
    """months_esiids: {"2026-06": [E1, E2], ...} -> actual_commissions rows."""
    for ym, esiids in months_esiids.items():
        for es in list(esiids) + (FILL if pad else []):
            db.table("actual_commissions").insert({"supplier_id": supplier, "billing_month": f"{ym}-01",
                                                   "raw_esiid": es}).execute()


def _deals(db, *specs):
    """specs: (esiid, source, start) -> seeded deal rows + load_deals-shaped dict."""
    by = {}
    for es, source, start in specs:
        did = f"d-{es}"
        db.table(source).insert({"id": did, "esiid": es,
                                 ("status" if source == "lead_deals" else "deal_status"):
                                 ("Active" if source == "lead_deals" else "ACTIVE")}).execute()
        by[es] = {"source": source, "id": did, "active": True, "esiid": es, "start": start}
    return {"by_esiid": by}


def test_absence_rule_covers_every_provider():
    for g in ("Discount Power/Cirro", "Iron Horse", "CleanSky", "Chariot", "Hudson Energy",
              "Budget Power", "Heritage Power", "Tara Energy", "NRG Commercial", "Reliant Energy", "APG&E"):
        assert g in ABSENCE_SYNC_GROUPS


def test_absence_deactivates_after_three_missing_months():
    db = _db()
    _ledger(db, {"2026-06": [E1, E2], "2026-07": [E1], "2026-08": [E1]})
    deals = _deals(db, (E1, "crm_deals", "2025-01-01"), (E2, "lead_deals", "2025-01-01"),
                   (E3, "crm_deals", "2025-01-01"))
    out = absence_sync(db, SUP, "Iron Horse", deals, "tester", current_esiids={E1})
    assert out["deactivated"] == 1 and out["churned"] == 1 and not out["pending"]
    assert out["window"] == "2026-06" and out["latest"] == "2026-08"
    assert db.tables["crm_deals"][0]["deal_status"] == "ACTIVE"          # E1 still paid
    assert db.tables["lead_deals"][0]["status"] == "Active"              # E2 paid in June (in window)
    assert db.tables["crm_deals"][1]["deal_status"] == "INACTIVE"        # E3 never paid
    assert db.tables["crm_deals"][1]["provider_status"] == "Inactive"
    assert any(a["action"] == "status_deactivated" for a in db.tables["audit_log"])


def test_absence_grace_period_for_new_contracts():
    db = _db()
    _ledger(db, {"2026-06": [E1], "2026-07": [E1], "2026-08": [E1]})
    deals = _deals(db, (E2, "crm_deals", "2026-07-15"),   # started 1 month ago: first payment not due yet
                   (E3, "crm_deals", "2025-12-01"))      # old contract, unpaid for 3 months
    out = absence_sync(db, SUP, "CleanSky", deals, "tester")
    assert out["in_grace"] == 1 and out["deactivated"] == 1
    assert db.tables["crm_deals"][0]["deal_status"] == "ACTIVE"
    assert db.tables["crm_deals"][1]["deal_status"] == "INACTIVE"


def test_absence_needs_three_full_statement_months_on_file():
    db = _db()
    _ledger(db, {"2026-07": [E1], "2026-08": [E1]})
    deals = _deals(db, (E2, "crm_deals", "2025-01-01"))
    out = absence_sync(db, SUP, "Chariot", deals, "tester")
    assert out["deactivated"] == 0 and "skipped" in out
    assert db.tables["crm_deals"][0]["deal_status"] == "ACTIVE"


def test_stub_statement_month_does_not_count_toward_window():
    db = _db()
    _ledger(db, {"2026-05": [E1, E2], "2026-06": [E1], "2026-07": [E1]})
    _ledger(db, {"2026-08": [E3]}, pad=False)            # 1-row stub vs 11-row months
    assert _full_statement_months(db, SUP) == ["2026-07-01", "2026-06-01", "2026-05-01"]
    deals = _deals(db, (E2, "crm_deals", "2025-01-01"), (E3, "crm_deals", "2025-01-01"))
    out = absence_sync(db, SUP, "Iron Horse", deals, "tester")
    # window is May-Jul (stub ignored): E2 paid in May -> kept; E3 paid on the stub -> still "seen"
    assert out["window"] == "2026-05" and out["deactivated"] == 0
    assert all(d["deal_status"] == "ACTIVE" for d in db.tables["crm_deals"])


def test_meter_paid_by_another_rep_is_not_dropped_without_a_twin():
    db = _db()
    _ledger(db, {"2026-06": [E1], "2026-07": [E1], "2026-08": [E1]})
    _ledger(db, {"2026-07": [E2]}, supplier=OTHER, pad=False)   # Budget Power now pays E2
    deals = _deals(db, (E1, "crm_deals", "2025-01-01"), (E2, "crm_deals", "2025-01-01"))
    out = absence_sync(db, SUP, "Iron Horse", deals, "tester")
    assert out["deactivated"] == 0 and out["candidates"] == 0
    assert [(x["esiid"], x["paid_by"]) for x in out["switched"]] == [(E2, "Budget Power")]
    assert db.tables["crm_deals"][1]["deal_status"] == "ACTIVE"
    assert "audit_log" not in db.tables


def test_stale_twin_is_dropped_when_other_provider_pays_and_crm_has_that_deal():
    db = _db()
    _ledger(db, {"2026-06": [E1], "2026-07": [E1], "2026-08": [E1]})
    _ledger(db, {"2026-07": [E2]}, supplier=OTHER, pad=False)
    deals = _deals(db, (E1, "crm_deals", "2025-01-01"),
                   (E2, "lead_deals", "2025-01-01"))          # old Budget-era pipeline deal
    db.table("crm_deals").insert({"id": "twin", "esiid": E2, "deal_status": "ACTIVE",
                                  "provider": "Budget Power"}).execute()
    out = absence_sync(db, SUP, "Iron Horse", deals, "tester")
    assert out["deactivated"] == 1 and out["stale_twins"] == 1 and out["switched"] == []
    assert db.tables["lead_deals"][0]["status"] == "Inactive"
    assert "now paid by Budget Power" in db.tables["lead_deals"][0]["provider_status_source"]
    assert [d["deal_status"] for d in db.tables["crm_deals"]] == ["ACTIVE", "ACTIVE"]  # E1 + twin untouched


def test_absence_mass_churn_is_held_until_forced():
    db = _db()
    _ledger(db, {"2026-06": [E1], "2026-07": [E1], "2026-08": [E1]})
    deals = _deals(db, (E1, "crm_deals", "2025-01-01"), (E2, "crm_deals", "2025-01-01"),
                   (E3, "crm_deals", "2025-01-01"))
    out = absence_sync(db, SUP, "Discount Power/Cirro", deals, "tester")
    assert out["pending"] and out["held"] == 2 and out["deactivated"] == 0
    assert all(d["deal_status"] == "ACTIVE" for d in db.tables["crm_deals"])
    forced = absence_sync(db, SUP, "Discount Power/Cirro", deals, "tester", force=True)
    assert forced["deactivated"] == 2
    assert [d["deal_status"] for d in db.tables["crm_deals"]] == ["ACTIVE", "INACTIVE", "INACTIVE"]


def test_absence_dry_run_writes_nothing():
    db = _db()
    _ledger(db, {"2026-06": [E1], "2026-07": [E1], "2026-08": [E1]})
    deals = _deals(db, (E1, "crm_deals", "2025-01-01"), (E2, "crm_deals", "2025-01-01"))
    out = absence_sync(db, SUP, "Tara Energy", deals, "tester", dry_run=True)
    assert [w["esiid"] for w in out["would_deactivate"]] == [E2]
    assert all(d["deal_status"] == "ACTIVE" for d in db.tables["crm_deals"])
    assert "audit_log" not in db.tables


def test_hudson_drop_flag_is_trusted():
    assert "Hudson Energy" in TRUSTED_STATUS_GROUPS
    assert map_status("drop 2026-07-13 00:00:00") == "inactive"
    assert map_status("Payment") is None
    assert map_status("Residential") is None and map_status("Switch Back") is None


def test_deals_of_another_provider_group_are_not_judged_by_this_statement():
    """Budget Power's load_deals() also carries the Direct Energy deals of the
    transferred book; Budget's statements must not deactivate them."""
    db = _db()
    _ledger(db, {"2026-06": [E1], "2026-07": [E1], "2026-08": [E1]})
    deals = _deals(db, (E1, "lead_deals", "2025-01-01"), (E2, "lead_deals", "2025-01-01"))
    deals["by_esiid"][E1]["provider"] = "budget power"
    deals["by_esiid"][E2]["provider"] = "direct energy"       # home group: Discount Power/Cirro
    out = absence_sync(db, SUP, "Budget Power", deals, "tester")
    assert out["other_group"] == 1 and out["active"] == 1 and out["deactivated"] == 0
    assert db.tables["lead_deals"][1]["status"] == "Active"
