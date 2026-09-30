"""GiaDienRe daily contract monitor: one renewal task per contract end date."""
import sys
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.api.v1 import giadienre
from tests.fakedb import FakeDB


def _run(db, monkeypatch):
    monkeypatch.setenv("GDR_CRON_KEY", "k")
    monkeypatch.setattr(giadienre, "get_client", lambda: db)
    monkeypatch.setattr("app.services.sms.send_sms", lambda *a, **k: None)
    return giadienre.monitor_run(x_cron_key="k")


def _sub(db, days, **kw):
    end = (giadienre._now().date() + timedelta(days=days)).isoformat()
    row = {"id": f"sub-{len(db.tables.get('giadienre_subscriptions', []))}",
           "full_name": "A", "phone": "1", "status": "NEW",
           "contract_end_date": end, "extra": {"bills": [1]}, **kw}
    db.table("giadienre_subscriptions").insert(row).execute()
    return row["id"], end


def _renewals(db):
    return [t for t in db.tables.get("tasks", []) if "GiaDienRe renewal" in t["title"]]


def test_deleted_or_completed_task_is_not_recreated(monkeypatch):
    db = FakeDB()
    sid, end = _sub(db, 10)
    assert _run(db, monkeypatch)["tasks_created"] == 1
    sub = db.tables["giadienre_subscriptions"][0]
    assert sub["extra"] == {"bills": [1], "renewal_alert_end": end}

    db.tables["tasks"][0]["status"] = "completed"
    assert _run(db, monkeypatch)["tasks_created"] == 0

    db.tables["tasks"] = []   # deleted from the Tasks page
    assert _run(db, monkeypatch)["tasks_created"] == 0
    assert _renewals(db) == []


def test_new_end_date_alerts_again(monkeypatch):
    db = FakeDB()
    _sub(db, 10)
    _run(db, monkeypatch)
    db.tables["tasks"] = []
    new_end = (giadienre._now().date() + timedelta(days=60)).isoformat()
    db.tables["giadienre_subscriptions"][0]["contract_end_date"] = new_end
    assert _run(db, monkeypatch)["tasks_created"] == 1
    assert db.tables["giadienre_subscriptions"][0]["extra"]["renewal_alert_end"] == new_end


def test_long_expired_contract_is_skipped(monkeypatch):
    db = FakeDB()
    _sub(db, -(giadienre.MONITOR_EXPIRED_GRACE_DAYS + 1))
    _sub(db, -giadienre.MONITOR_EXPIRED_GRACE_DAYS)
    res = _run(db, monkeypatch)
    assert res["tasks_created"] == 1 and res["expired"] == 1
    assert "sub-1" in _renewals(db)[0]["description"]
