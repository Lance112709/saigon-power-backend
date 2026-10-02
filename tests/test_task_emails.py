"""Task emails: who gets told about which task."""
from datetime import date

from app.services.email_reminders import (
    build_digests, resolve_assignee, task_recipients, _build_email_html,
)


def _user(uid, first, last, role="manager"):
    return {"id": uid, "first_name": first, "last_name": last, "role": role,
            "email": f"{first.lower()}@example.com", "name": f"{first} {last}"}

USERS = [_user("u-lance", "Lance", "Nguyen", "admin"),
         _user("u-nga", "Nga", "Nguyen"),
         _user("u-jennie", "Jennie", "Duong")]


def _task(**kw):
    return {"id": "t1", "title": "Call back", "status": "pending", "priority": "medium",
            "due_date": "2026-10-02T14:00:00+00:00", **kw}


def test_resolve_assignee_full_name_email_and_first_name():
    assert resolve_assignee("Jennie Duong", USERS)["id"] == "u-jennie"
    assert resolve_assignee("  jennie ", USERS)["id"] == "u-jennie"
    assert resolve_assignee("NGA@example.com", USERS)["id"] == "u-nga"
    assert resolve_assignee("", USERS) is None
    assert resolve_assignee("Somebody Else", USERS) is None


def test_resolve_assignee_ambiguous_first_name_matches_nobody():
    users = USERS + [_user("u-nga2", "Nga", "Tran")]
    assert resolve_assignee("nga", users) is None
    assert resolve_assignee("Nga Tran", users)["id"] == "u-nga2"


def test_recipients_are_creator_and_assignee():
    t = _task(created_by_id="u-nga", assigned_to="Jennie Duong")
    assert {u["id"] for u in task_recipients(t, USERS)} == {"u-nga", "u-jennie"}
    mine = _task(created_by_id="u-nga", assigned_to="Nga Nguyen")
    assert [u["id"] for u in task_recipients(mine, USERS)] == ["u-nga"]


def test_unowned_task_goes_to_admins():
    assert [u["id"] for u in task_recipients(_task(), USERS)] == ["u-lance"]
    assert [u["id"] for u in task_recipients(_task(assigned_to="Bob"), USERS)] == ["u-lance"]


def test_digest_buckets_by_calendar_date_and_owner():
    today = date(2026, 10, 2)
    tasks = [
        _task(id="old", due_date="2026-09-30T23:30:00+00:00", assigned_to="jennie"),
        _task(id="now", due_date="2026-10-02T23:30:00+00:00", created_by_id="u-nga", assigned_to="Jennie Duong"),
        _task(id="tmrw", due_date="2026-10-03T01:00:00+00:00", created_by_id="u-nga"),
        _task(id="later", due_date="2026-10-09T01:00:00+00:00", created_by_id="u-nga"),
        _task(id="done", status="completed", assigned_to="jennie"),
    ]
    d = build_digests(tasks, USERS, today)
    ids = lambda uid: [[t["id"] for t in b] for b in d[uid]]
    assert ids("u-jennie") == [["old"], ["now"], []]
    assert ids("u-nga") == [[], ["now"], ["tmrw"]]
    assert "u-lance" not in d


def test_digest_html_escapes_titles_and_explains_ownership():
    t = _task(title="<script>x</script>", created_by="Nga Nguyen", created_by_id="u-nga", assigned_to="jennie")
    html = _build_email_html(USERS[2], USERS, [], [t], [])
    assert "<script>" not in html and "&lt;script&gt;" in html
    assert "Assigned to you by Nga Nguyen" in html
