"""Call-list month-to-month exclusion."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.api.v1.calllist import _is_month_to_month


def test_month_to_month_markers_detected():
    assert _is_month_to_month({"rate_type": "Month-Month"})
    assert _is_month_to_month({"contract_term": "Month to Month"})
    assert _is_month_to_month({"plan_name": "IH Month to Month"})
    assert _is_month_to_month({"rate_type": "month_to_month"})


def test_fixed_plans_kept():
    assert not _is_month_to_month({"rate_type": "Fixed Rate", "contract_term": "36 Months",
                                   "plan_name": "No Gimmicks 36"})
    assert not _is_month_to_month({"contract_term": "12 Months"})
    assert not _is_month_to_month({"plan_name": None, "rate_type": None})


from app.api.v1.calllist import _cycle_key, _drop_resolved


def test_resolved_rows_hidden_for_same_cycle_only():
    rows = [
        {"entity_key": "lead:abc", "end_date": "2026-10-01", "name": "A"},
        {"entity_key": "crm:xyz",  "end_date": "2026-11-15", "name": "B"},
        {"entity_key": "crm:xyz",  "end_date": None,         "name": "C"},
    ]
    resolved = {_cycle_key("lead:abc", "2026-10-01T00:00:00"), _cycle_key("crm:xyz", None)}
    kept = [r["name"] for r in _drop_resolved(rows, resolved)]
    # A resolved for its cycle; C resolved (no end date); B is a different cycle → stays
    assert kept == ["B"]


def test_renewed_customer_returns_next_cycle():
    rows = [{"entity_key": "lead:abc", "end_date": "2027-10-01"}]
    resolved = {_cycle_key("lead:abc", "2026-10-01")}
    assert _drop_resolved(rows, resolved) == rows
