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


from app.api.v1.calllist import _month_counts, _filter_month, NO_MONTH


def test_month_counts_sorted_with_no_date_last():
    rows = [
        {"end_date": "2026-11-03"}, {"end_date": "2026-09-30"},
        {"end_date": "2026-11-20T00:00:00"}, {"end_date": None}, {"end_date": ""},
    ]
    assert _month_counts(rows) == [
        {"month": "2026-09", "label": "Sep 2026", "count": 1},
        {"month": "2026-11", "label": "Nov 2026", "count": 2},
        {"month": NO_MONTH, "label": "No end date", "count": 2},
    ]


def test_filter_month():
    rows = [{"end_date": "2026-11-03", "n": 1}, {"end_date": "2026-09-30", "n": 2}, {"end_date": None, "n": 3}]
    assert [r["n"] for r in _filter_month(rows, "2026-11")] == [1]
    assert [r["n"] for r in _filter_month(rows, NO_MONTH)] == [3]
    assert [r["n"] for r in _filter_month(rows, None)] == [1, 2, 3]
    assert [r["n"] for r in _filter_month(rows, "")] == [1, 2, 3]
