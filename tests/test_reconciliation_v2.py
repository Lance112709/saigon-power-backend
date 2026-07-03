"""Reconciliation engine v2 tests with an in-memory fake Supabase client."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.reconciliation_v2 import run_reconciliation_v2, norm_addr, _in_window

SUP = "00000000-0000-0000-0000-000000000001"


class FakeQuery:
    def __init__(self, db, table):
        self.db, self.tname = db, table
        self._rows = None

    # chainable no-op filters that are good enough for these tests
    def select(self, *a, **k): return self
    def eq(self, *a): return self
    def in_(self, *a): return self
    def limit(self, *a): return self
    def range(self, *a): return self

    def insert(self, rows):
        rows = rows if isinstance(rows, list) else [rows]
        stored = []
        for r in rows:
            r = dict(r)
            r.setdefault("id", f"{self.tname}-{len(self.db.tables.setdefault(self.tname, []))}")
            self.db.tables.setdefault(self.tname, []).append(r)
            stored.append(r)
        self._rows = stored
        return self

    def execute(self):
        class R:
            pass
        r = R()
        r.data = self._rows if self._rows is not None else list(self.db.tables.get(self.tname, []))
        r.count = len(r.data)
        return r


class FakeDB:
    def __init__(self):
        self.tables = {}

    def table(self, name):
        return FakeQuery(self, name)


def deal(esiid, adder=0.008, active=True, start="2025-01-01", end="2027-01-01",
         name="Test Customer", est_kwh=1000.0):
    return {"source": "lead_deals", "id": f"deal-{esiid}", "lead_id": None,
            "active": active, "status": "Active" if active else "Inactive",
            "adder": adder, "est_kwh": est_kwh, "start": start, "end": end,
            "esiid": esiid, "name": name, "phone": None, "agent": "Lance",
            "addr_n": "", "zip5": ""}


def row(esiid, amount, rate=0.008, usage=1000.0, ss="2026-04-01", se="2026-05-01",
        status="", label="2026-05"):
    return {"esiid": esiid, "customer_name": "Test", "address": "", "city": "", "zip": "",
            "usage_kwh": usage, "rate": rate, "amount": amount,
            "service_start": ss, "service_end": se, "provider_status": status,
            "row_type": "commission", "statement_label": label, "raw": {}}


def run(deals_list, rows):
    db = FakeDB()
    deals = {"by_esiid": {d["esiid"]: d for d in deals_list}, "no_esiid": [], "addr_index": {}}
    out = run_reconciliation_v2(db, SUP, "Budget Power", "2026-05", rows, deals=deals)
    return out, db


E1 = "1008901000000000000001"
E2 = "1008901000000000000002"


def test_correct_payment_is_matched():
    out, db = run([deal(E1)], [row(E1, 8.0)])
    assert out["matched"] == 1
    assert out["missing"] == out["short_paid"] == out["over_paid"] == out["unexpected"] == 0


def test_wrong_rate_detected_with_dollars_lost():
    # contract says 8 mills, provider paid 5 mills on 1000 kWh → $3 short
    out, db = run([deal(E1, adder=0.008)], [row(E1, 5.0, rate=0.005)])
    assert out["short_paid"] == 1
    item = next(i for i in db.tables["reconciliation_items"] if i["status"] == "short_paid")
    assert abs(item["expected_amount"] - 8.0) < 0.01
    assert "0.005" in item["resolution_notes"] and "0.008" in item["resolution_notes"]


def test_missing_active_deal_flagged():
    out, db = run([deal(E1), deal(E2)], [row(E1, 8.0)])
    assert out["missing"] == 1
    item = next(i for i in db.tables["reconciliation_items"] if i["status"] == "missing")
    assert item["esiid"] == E2
    assert item["expected_amount"] == 8.0  # adder * est_kwh


def test_contract_window_respected():
    # contract ended well before the statement month → not "missing"
    ended = deal(E2, end="2025-06-30")
    out, _ = run([deal(E1), ended], [row(E1, 8.0)])
    assert out["missing"] == 0


def test_future_contract_not_expected():
    future = deal(E2, start="2026-06-15")
    out, _ = run([deal(E1), future], [row(E1, 8.0)])
    assert out["missing"] == 0


def test_duplicate_identical_rows_flagged_as_overpaid():
    out, db = run([deal(E1)], [row(E1, 8.0), row(E1, 8.0)])
    assert out["over_paid"] == 1
    item = next(i for i in db.tables["reconciliation_items"] if i["status"] == "over_paid")
    assert "duplicate" in item["resolution_notes"].lower()


def test_split_invoice_rows_are_not_duplicates():
    # same period but different amounts = split line items (normal for Iron Horse)
    out, _ = run([deal(E1, adder=0.007)],
                 [row(E1, 4.18, rate=0.007, usage=597.6), row(E1, 0.46, rate=0.007, usage=66.4)])
    assert out["over_paid"] == 0
    assert out["matched"] == 1


def test_unknown_esiid_flagged_unexpected():
    out, db = run([], [row(E1, 8.0)])
    assert out["unexpected"] == 1
    item = db.tables["reconciliation_items"][0]
    assert "not found in CRM" in item["resolution_notes"]


def test_provider_churn_status_conflict():
    out, db = run([deal(E1)], [row(E1, 8.0, status="Inactive")])
    assert out["unexpected"] == 1
    item = db.tables["reconciliation_items"][0]
    assert "Inactive" in item["resolution_notes"]


def test_run_totals_add_up():
    out, _ = run([deal(E1), deal(E2, adder=0.008)],
                 [row(E1, 8.0), row(E2, 5.0, rate=0.005)])
    assert abs(out["total_actual"] - 13.0) < 0.01
    assert abs(out["total_expected"] - 16.0) < 0.01   # 8 correct + 8 rate-corrected
    assert abs(out["total_discrepancy"] + 3.0) < 0.01


def test_addr_normalization():
    assert norm_addr("3532 Omeara Drive") == norm_addr("3532 OMEARA DR")
    assert norm_addr("4601 Avenue H, Apt 3") == norm_addr("4601 AVENUE H #3")
    assert norm_addr("123 North Main Street") == norm_addr("123 N MAIN ST")
