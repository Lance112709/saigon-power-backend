"""Outcome learning: recovery stats, hints, and priority boost."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from fakedb import FakeDB
from app.services.recovery_stats import (
    priority_multiplier, provider_recovery_stats, recovery_hint,
)

SUP_GOOD = "00000000-0000-0000-0000-000000000001"   # pays disputes
SUP_BAD = "00000000-0000-0000-0000-000000000002"    # rejects them
SUP_NEW = "00000000-0000-0000-0000-000000000003"    # no history


def seeded():
    db = FakeDB()
    db.tables["disputes"] = [
        {"supplier_id": SUP_GOOD, "status": "recovered", "total_claimed": 1000.0, "total_recovered": 900.0},
        {"supplier_id": SUP_GOOD, "status": "recovered", "total_claimed": 500.0, "total_recovered": 450.0},
        {"supplier_id": SUP_GOOD, "status": "sent", "total_claimed": 200.0, "total_recovered": 0.0},
        {"supplier_id": SUP_BAD, "status": "rejected", "total_claimed": 800.0, "total_recovered": 0.0},
        {"supplier_id": SUP_NEW, "status": "draft", "total_claimed": 100.0, "total_recovered": 0.0},
    ]
    return provider_recovery_stats(db)


def test_stats_only_count_closed_outcomes():
    stats = seeded()
    good = stats[SUP_GOOD]
    assert good["disputes_sent"] == 3       # sent includes open ones
    assert good["disputes_closed"] == 2     # closed = recovered/rejected only
    assert good["recovery_rate"] == 0.9     # 1350 / 1500
    assert stats[SUP_BAD]["recovery_rate"] == 0.0
    # drafts don't create history
    assert stats[SUP_NEW]["disputes_closed"] == 0


def test_hints_reflect_history():
    stats = seeded()
    assert "90% of claimed dollars recovered" in recovery_hint(stats, SUP_GOOD)
    assert "almost always pays" in recovery_hint(stats, SUP_GOOD)
    assert "rejected past disputes" in recovery_hint(stats, SUP_BAD)
    assert recovery_hint(stats, SUP_NEW) is None       # no closed history yet
    assert recovery_hint(stats, "unknown-supplier") is None


def test_priority_boost_for_paying_providers():
    stats = seeded()
    assert priority_multiplier(stats, SUP_GOOD) == 1.45    # 1 + 0.9*0.5
    assert priority_multiplier(stats, SUP_BAD) == 1.0      # 0% recovery, no boost
    assert priority_multiplier(stats, SUP_NEW) == 1.0      # no history, neutral
