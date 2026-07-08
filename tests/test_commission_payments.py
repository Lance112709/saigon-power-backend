"""Pure-logic tests for the commission payments API helpers."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.api.v1.commission_payments import month_status, norm_es


def test_norm_es_strips_formatting():
    assert norm_es(" 1008901-0231234567890 ") == "10089010231234567890"
    assert norm_es(None) == ""


def test_month_status_maps_recon_verdicts():
    items = [
        {"esiid": "10443720008297350", "billing_month": "2026-05-01", "status": "matched"},
        {"esiid": "10443720008297350", "billing_month": "2026-04-01", "status": "short_paid"},
        {"esiid": "10443720008297350", "billing_month": "2026-03-01", "status": "missing"},
    ]
    st = month_status(items)
    assert st[("10443720008297350", "2026-05")] == "paid"
    assert st[("10443720008297350", "2026-04")] == "partial"
    assert st[("10443720008297350", "2026-03")] == "unpaid"


def test_month_status_worst_verdict_wins():
    items = [
        {"esiid": "10443720008297350", "billing_month": "2026-05-01", "status": "matched"},
        {"esiid": "10443720008297350", "billing_month": "2026-05-01", "status": "missing"},
    ]
    assert month_status(items)[("10443720008297350", "2026-05")] == "unpaid"
