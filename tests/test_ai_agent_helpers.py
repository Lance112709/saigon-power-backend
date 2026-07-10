"""Pure parsing helpers behind the free keyword chat engine."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.ai_agent import _detect_month, _extract_esiid, _extract_search_term


def test_month_name_and_year():
    assert _detect_month("commission in may 2026") == "2026-05"
    assert _detect_month("what did we get in january 2025?") == "2025-01"
    assert _detect_month("revenue for Sept 2025") == "2025-09"


def test_iso_month():
    assert _detect_month("show 2026-04 payments") == "2026-04"


def test_no_month():
    assert _detect_month("how many active deals") is None
    assert _detect_month("we may want to check") is None  # 'may' without a year


def test_esiid_extraction():
    q = "what happened with 1008901023810634620100 last month"
    assert _extract_esiid(q) == "1008901023810634620100"
    assert _extract_esiid("17-digit 10443720009905279 ok") == "10443720009905279"
    assert _extract_esiid("no ids here, just 12345") is None
    assert _extract_esiid("phone 8329379999 is short") is None


def test_search_term_extraction():
    assert _extract_search_term("find customer Julie Vu") == "Julie Vu"
    assert _extract_search_term("look up Teressa Evans please") == "Teressa Evans"
    assert _extract_search_term("phone number for Henry Nguyen") == "Henry Nguyen"
    assert _extract_search_term("search for the customer named Ngoc Doan") == "Ngoc Doan"


def test_search_term_absent_or_too_short():
    assert _extract_search_term("how many active deals do we have") is None
    assert _extract_search_term("find it") is None
