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
