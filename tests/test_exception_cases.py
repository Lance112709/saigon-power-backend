"""Exception-case workflow: durable across reconciliation re-runs."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from fakedb import FakeDB
from app.services.exception_cases import (
    _priority, list_cases, update_case, upsert_cases_from_run,
)

SUP = "00000000-0000-0000-0000-000000000001"
LABEL = "2026-04"
E1 = "1008901000000000000001"
E2 = "1008901000000000000002"


def item(run_id, esiid, status, severity="high", expected=8.0, actual=5.0,
         notes="ROOT CAUSE: wrong commission rate"):
    return {"reconciliation_run_id": run_id, "esiid": esiid, "status": status,
            "severity": severity, "expected_amount": expected, "actual_amount": actual,
            "discrepancy_amount": (actual or 0) - (expected or 0),
            "resolution_notes": notes, "is_resolved": False}


def deals_for(*esiids):
    return {"by_esiid": {e: {"name": f"Customer {e[-2:]}", "agent": "Lance"} for e in esiids}}


def seed(db, run_id, items):
    for it in items:
        db.table("reconciliation_items").insert(it).execute()


def test_new_discrepancy_opens_case_with_priority_and_action():
    db = FakeDB()
    seed(db, "run1", [item("run1", E1, "short_paid")])
    out = upsert_cases_from_run(db, "run1", SUP, LABEL, deals=deals_for(E1), actor="t")
    assert out["created"] == 1
    case = db.tables["exception_cases"][0]
    assert case["workflow_status"] == "open"
    assert case["estimated_loss"] == 3.0          # 8 expected - 5 paid
    assert case["priority_score"] == _priority("high", 3.0)
    assert "true-up" in case["recommended_action"]
    assert case["customer_name"] == f"Customer {E1[-2:]}"
    # item back-linked to its case
    assert db.tables["reconciliation_items"][0]["case_id"] == case["id"]


def test_rerun_preserves_workflow_state_and_refreshes_numbers():
    db = FakeDB()
    seed(db, "run1", [item("run1", E1, "short_paid")])
    upsert_cases_from_run(db, "run1", SUP, LABEL, deals=deals_for(E1), actor="t")
    case_id = db.tables["exception_cases"][0]["id"]
    update_case(db, case_id, {"workflow_status": "waiting_on_provider",
                              "notes": "emailed rep"}, actor="lance")

    # month re-reconciled: new run, same discrepancy, bigger loss
    seed(db, "run2", [item("run2", E1, "short_paid", expected=10.0, actual=5.0)])
    out = upsert_cases_from_run(db, "run2", SUP, LABEL, deals=deals_for(E1), actor="t")
    assert out["updated"] == 1 and out["created"] == 0
    case = db.tables["exception_cases"][0]
    assert case["workflow_status"] == "waiting_on_provider"   # survived
    assert case["notes"] == "emailed rep"                     # survived
    assert case["estimated_loss"] == 5.0                      # refreshed
    assert case["last_seen_run_id"] == "run2"


def test_disappeared_discrepancy_auto_resolves():
    db = FakeDB()
    seed(db, "run1", [item("run1", E1, "short_paid")])
    upsert_cases_from_run(db, "run1", SUP, LABEL, deals=deals_for(E1), actor="t")
    # next run: E1 now matched (only a matched item, which is ignored)
    seed(db, "run2", [item("run2", E1, "matched")])
    out = upsert_cases_from_run(db, "run2", SUP, LABEL, deals=deals_for(E1), actor="t")
    assert out["auto_resolved"] == 1
    case = db.tables["exception_cases"][0]
    assert case["workflow_status"] == "resolved"
    assert "Self-corrected" in case["notes"]


def test_manually_closed_case_not_reopened_by_auto_resolve():
    db = FakeDB()
    seed(db, "run1", [item("run1", E1, "short_paid")])
    upsert_cases_from_run(db, "run1", SUP, LABEL, deals=deals_for(E1), actor="t")
    case_id = db.tables["exception_cases"][0]["id"]
    update_case(db, case_id, {"workflow_status": "recovered",
                              "recovered_amount": 3.0}, actor="lance")
    seed(db, "run2", [item("run2", E2, "missing", expected=8.0, actual=None)])
    upsert_cases_from_run(db, "run2", SUP, LABEL, deals=deals_for(E1, E2), actor="t")
    case = next(c for c in db.tables["exception_cases"] if c["esiid"] == E1)
    assert case["workflow_status"] == "recovered"
    assert case["recovered_amount"] == 3.0


def test_update_case_resolves_linked_items():
    db = FakeDB()
    seed(db, "run1", [item("run1", E1, "short_paid")])
    upsert_cases_from_run(db, "run1", SUP, LABEL, deals=deals_for(E1), actor="t")
    case_id = db.tables["exception_cases"][0]["id"]
    update_case(db, case_id, {"workflow_status": "ignored"}, actor="lance")
    assert db.tables["reconciliation_items"][0]["is_resolved"] is True


def test_list_cases_priority_sorted_and_filtered():
    db = FakeDB()
    seed(db, "run1", [item("run1", E1, "short_paid", severity="low", expected=6.0, actual=5.0),
                      item("run1", E2, "missing", severity="critical", expected=100.0, actual=None)])
    upsert_cases_from_run(db, "run1", SUP, LABEL, deals=deals_for(E1, E2), actor="t")
    rows = list_cases(db, workflow_status="any_open")
    assert rows[0]["esiid"] == E2  # critical $100 outranks low $1
    assert list_cases(db, min_loss=50)[0]["esiid"] == E2
    assert len(list_cases(db, min_loss=50)) == 1


def test_invalid_workflow_status_rejected():
    db = FakeDB()
    seed(db, "run1", [item("run1", E1, "short_paid")])
    upsert_cases_from_run(db, "run1", SUP, LABEL, deals=deals_for(E1), actor="t")
    case_id = db.tables["exception_cases"][0]["id"]
    try:
        update_case(db, case_id, {"workflow_status": "bogus"}, actor="t")
        assert False, "should have raised"
    except ValueError:
        pass
