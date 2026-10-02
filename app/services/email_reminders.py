"""Task emails for CRM staff.

Each user is emailed only about tasks they created or are assigned to:
  * a daily digest (overdue / due today / due tomorrow),
  * an instant notice when someone else assigns them a task,
  * an instant notice when someone else completes a task they created.
Tasks nobody owns (system-generated, or assigned to a name that matches no
user) go to the admins' digest so they are never silently dropped.
"""
import os
from datetime import datetime, timedelta
from html import escape

import pytz

from app.db.client import get_client

# saigonpowertx.com is the domain verified in Resend.
FROM_EMAIL = (os.environ.get("REMINDER_FROM_EMAIL")
              or os.environ.get("CUSTOMER_FROM_EMAIL")
              or "admin@saigonpowertx.com")
CRM_URL = os.environ.get("CRM_URL", "https://saigonpowertx.com").rstrip("/")
CENTRAL = pytz.timezone("America/Chicago")


def _dt(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

def _priority_color(p: str) -> str:
    return {"high": "#dc2626", "medium": "#d97706", "low": "#6b7280"}.get(p, "#6b7280")

def _task_url(task: dict) -> str:
    if task.get("lead_id"):
        return f"{CRM_URL}/crm/leads/{task['lead_id']}"
    if task.get("customer_id"):
        return f"{CRM_URL}/crm/customers/{task['customer_id']}"
    return f"{CRM_URL}/tasks"

def _due_label(task: dict) -> str:
    try:
        return _dt(task["due_date"]).strftime("%b %d")
    except Exception:
        return ""

def _send(to: str, subject: str, html: str) -> dict:
    try:
        import resend
        if not getattr(resend, "api_key", None):
            resend.api_key = os.environ.get("RESEND_API_KEY", "")
        if not resend.api_key:
            return {"ok": False, "error": "RESEND_API_KEY not set"}
        resend.Emails.send({
            "from": f"Saigon Power CRM <{FROM_EMAIL}>",
            "to": [to],
            "subject": subject,
            "html": html,
        })
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


# ── Who owns a task ───────────────────────────────────────────────────────────

def active_users(db) -> list:
    rows = (db.table("users")
            .select("id, first_name, last_name, email, role, status")
            .eq("status", "active").execute().data or [])
    for u in rows:
        u["name"] = f"{u.get('first_name') or ''} {u.get('last_name') or ''}".strip()
    return rows

def resolve_assignee(assigned_to, users: list):
    """Match the free-text tasks.assigned_to to a user: full name or email
    first, then a first name when exactly one user has it."""
    key = str(assigned_to or "").strip().lower()
    if not key:
        return None
    for u in users:
        if key in (u["name"].lower(), str(u.get("email") or "").lower()):
            return u
    first = [u for u in users if str(u.get("first_name") or "").strip().lower() == key]
    return first[0] if len(first) == 1 else None

def task_recipients(task: dict, users: list) -> list:
    """Users to email about a task: its creator and its assignee. A task with
    neither goes to the admins."""
    by_id = {u["id"]: u for u in users}
    out = {}
    creator = by_id.get(task.get("created_by_id"))
    if creator:
        out[creator["id"]] = creator
    assignee = resolve_assignee(task.get("assigned_to"), users)
    if assignee:
        out[assignee["id"]] = assignee
    if not out:
        out = {u["id"]: u for u in users if u.get("role") == "admin"}
    return list(out.values())

def _role_note(task: dict, user: dict, users: list) -> str:
    """Why this user is seeing the task, e.g. 'Assigned to you by Lance Nguyen'."""
    assignee = resolve_assignee(task.get("assigned_to"), users)
    mine_assigned = bool(assignee and assignee["id"] == user["id"])
    mine_created = task.get("created_by_id") == user["id"]
    if mine_assigned and mine_created:
        return "Your task"
    if mine_assigned:
        by = task.get("created_by")
        return f"Assigned to you by {by}" if by else "Assigned to you"
    if mine_created:
        who = task.get("assigned_to")
        return f"You created · assigned to {who}" if who else "You created · unassigned"
    who = task.get("assigned_to")
    return f"No owner · assigned to {who}" if who else "Unassigned"


# ── HTML ──────────────────────────────────────────────────────────────────────

def _shell(heading: str, body: str) -> str:
    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f4f6fa;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
  <div style="max-width:640px;margin:32px auto;background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.08);">
    <div style="background:#0f1d5e;padding:24px 32px;text-align:center;">
      <p style="margin:0;color:#4ade80;font-size:13px;font-weight:600;letter-spacing:.05em;">SAIGON POWER</p>
      <h1 style="margin:4px 0 0;color:#fff;font-size:20px;font-weight:700;">{heading}</h1>
      <p style="margin:6px 0 0;color:#94a3b8;font-size:13px;">{datetime.now(CENTRAL).strftime('%A, %B %d, %Y')}</p>
    </div>
    {body}
    <div style="padding:0 32px 32px;text-align:center;">
      <a href="{CRM_URL}/tasks"
         style="display:inline-block;background:#0f1d5e;color:#fff;padding:12px 32px;border-radius:10px;font-size:14px;font-weight:600;text-decoration:none;">
        View All Tasks →
      </a>
    </div>
    <div style="padding:16px 32px;border-top:1px solid #f1f5f9;text-align:center;">
      <p style="margin:0;font-size:12px;color:#94a3b8;">Saigon Power LLC · You get these for tasks you created or are assigned to</p>
    </div>
  </div>
</body>
</html>"""

def _build_email_html(user: dict, users: list, overdue: list, today: list, tomorrow: list) -> str:
    total = len(overdue) + len(today) + len(tomorrow)

    def task_rows(tasks: list, label_color: str, label: str) -> str:
        rows = ""
        for t in tasks:
            pcolor = _priority_color(t.get("priority", "low"))
            rows += f"""
            <tr>
              <td style="padding:10px 16px;border-bottom:1px solid #f1f5f9;">
                <a href="{_task_url(t)}" style="color:#0f1d5e;font-weight:600;text-decoration:none;">{escape(str(t.get('title') or '—'))}</a>
                <div style="color:#94a3b8;font-size:12px;margin-top:2px;">{escape(_role_note(t, user, users))}</div>
              </td>
              <td style="padding:10px 16px;border-bottom:1px solid #f1f5f9;white-space:nowrap;">
                <span style="background:{label_color}20;color:{label_color};padding:2px 8px;border-radius:99px;font-size:12px;font-weight:600;">{label}</span>
              </td>
              <td style="padding:10px 16px;border-bottom:1px solid #f1f5f9;color:#64748b;font-size:13px;white-space:nowrap;">{_due_label(t)}</td>
              <td style="padding:10px 16px;border-bottom:1px solid #f1f5f9;">
                <span style="color:{pcolor};font-size:12px;font-weight:600;">{escape(str(t.get('priority') or '—')).upper()}</span>
              </td>
            </tr>"""
        return rows

    all_rows = (
        task_rows(overdue, "#dc2626", "Overdue") +
        task_rows(today, "#d97706", "Due Today") +
        task_rows(tomorrow, "#2563eb", "Due Tomorrow")
    )

    def stat(n: int, label: str, color: str, bg: str) -> str:
        return f"""<td style="width:33%;text-align:center;background:{bg};border-radius:10px;padding:12px;">
          <p style="margin:0;font-size:22px;font-weight:700;color:{color};">{n}</p>
          <p style="margin:4px 0 0;font-size:11px;color:{color};font-weight:600;">{label}</p>
        </td>"""

    th = "padding:10px 16px;text-align:left;font-size:11px;color:#64748b;font-weight:600;text-transform:uppercase;letter-spacing:.05em;"
    body = f"""
    <div style="padding:24px 32px 16px;">
      <p style="margin:0;color:#1e293b;font-size:15px;">Hi <strong>{escape(user.get('first_name') or user['name'] or 'there')}</strong>,</p>
      <p style="margin:8px 0 0;color:#64748b;font-size:14px;">
        You have <strong style="color:#0f1d5e;">{total} task{'s' if total != 1 else ''}</strong> that need{'s' if total == 1 else ''} your attention.
      </p>
    </div>
    <div style="padding:0 24px 20px;">
      <table style="width:100%;border-collapse:separate;border-spacing:8px 0;"><tr>
        {stat(len(overdue), "OVERDUE", "#dc2626", "#fef2f2")}
        {stat(len(today), "DUE TODAY", "#d97706", "#fffbeb")}
        {stat(len(tomorrow), "DUE TOMORROW", "#2563eb", "#eff6ff")}
      </tr></table>
    </div>
    <div style="padding:0 16px 24px;">
      <table style="width:100%;border-collapse:collapse;background:#f8fafc;border-radius:12px;overflow:hidden;">
        <thead>
          <tr style="background:#e2e8f0;">
            <th style="{th}">Task</th><th style="{th}">Status</th><th style="{th}">Due</th><th style="{th}">Priority</th>
          </tr>
        </thead>
        <tbody>{all_rows}</tbody>
      </table>
    </div>"""
    return _shell("Daily Task Reminder", body)

def _single_task_html(heading: str, greeting_name: str, intro: str, task: dict, entity_name: str) -> str:
    pcolor = _priority_color(task.get("priority", "low"))
    td_l = "padding:6px 0;color:#64748b;font-size:13px;width:110px;vertical-align:top;"
    td_r = "padding:6px 0;color:#1e293b;font-size:13px;"
    rows = ""
    if entity_name:
        rows += f'<tr><td style="{td_l}">Customer</td><td style="{td_r}">{escape(entity_name)}</td></tr>'
    rows += f'<tr><td style="{td_l}">Due</td><td style="{td_r}">{_due_label(task) or "—"}</td></tr>'
    rows += (f'<tr><td style="{td_l}">Priority</td><td style="{td_r}">'
             f'<span style="color:{pcolor};font-weight:600;">{escape(str(task.get("priority") or "—")).upper()}</span></td></tr>')
    if task.get("description"):
        rows += (f'<tr><td style="{td_l}">Notes</td>'
                 f'<td style="{td_r}white-space:pre-wrap;">{escape(str(task["description"]))}</td></tr>')
    body = f"""
    <div style="padding:24px 32px 8px;">
      <p style="margin:0;color:#1e293b;font-size:15px;">Hi <strong>{escape(greeting_name)}</strong>,</p>
      <p style="margin:8px 0 0;color:#64748b;font-size:14px;">{intro}</p>
    </div>
    <div style="padding:8px 32px 24px;">
      <div style="background:#f8fafc;border-radius:12px;padding:16px 20px;">
        <a href="{_task_url(task)}" style="color:#0f1d5e;font-size:16px;font-weight:700;text-decoration:none;">{escape(str(task.get('title') or '—'))}</a>
        <table style="width:100%;border-collapse:collapse;margin-top:8px;">{rows}</table>
      </div>
    </div>"""
    return _shell(heading, body)

def _entity_name(db, task: dict) -> str:
    try:
        if task.get("lead_id"):
            r = db.table("leads").select("first_name, last_name").eq("id", task["lead_id"]).execute().data
            return f"{r[0].get('first_name') or ''} {r[0].get('last_name') or ''}".strip() if r else ""
        if task.get("customer_id"):
            r = db.table("crm_customers").select("full_name").eq("id", task["customer_id"]).execute().data
            return (r[0].get("full_name") or "") if r else ""
    except Exception:
        pass
    return ""


# ── Instant notices (run as background tasks from the tasks API) ──────────────

def notify_task_assigned(task: dict, actor_id: str, actor_name: str) -> dict:
    """Email the assignee that a task is now theirs. Skipped when they
    assigned it to themselves or the name matches no user."""
    try:
        db = get_client()
        assignee = resolve_assignee(task.get("assigned_to"), active_users(db))
        if not assignee or assignee["id"] == actor_id or not assignee.get("email"):
            return {"sent": 0}
        html = _single_task_html(
            "New Task Assigned", assignee.get("first_name") or assignee["name"],
            f"<strong>{escape(actor_name)}</strong> assigned you a task.",
            task, _entity_name(db, task))
        res = _send(assignee["email"], f"📋 New task from {actor_name}: {task.get('title') or ''}"[:150], html)
        return {"sent": 1 if res["ok"] else 0, **res}
    except Exception as e:
        return {"sent": 0, "error": str(e)[:200]}

def notify_task_completed(task: dict, actor_id: str, actor_name: str) -> dict:
    """Email the creator that someone else completed their task."""
    try:
        creator_id = task.get("created_by_id")
        if not creator_id or creator_id == actor_id:
            return {"sent": 0}
        db = get_client()
        creator = next((u for u in active_users(db) if u["id"] == creator_id), None)
        if not creator or not creator.get("email"):
            return {"sent": 0}
        html = _single_task_html(
            "Task Completed", creator.get("first_name") or creator["name"],
            f"<strong>{escape(actor_name)}</strong> completed a task you created.",
            task, _entity_name(db, task))
        res = _send(creator["email"], f"✅ {actor_name} completed: {task.get('title') or ''}"[:150], html)
        return {"sent": 1 if res["ok"] else 0, **res}
    except Exception as e:
        return {"sent": 0, "error": str(e)[:200]}


# ── Daily digest ──────────────────────────────────────────────────────────────

def build_digests(tasks: list, users: list, today) -> dict:
    """{user_id: (overdue, due_today, due_tomorrow)} for users with something due.
    Due dates are bucketed by calendar date, the way they were entered."""
    digests = {}
    for t in sorted(tasks, key=lambda t: t.get("due_date") or ""):
        if t.get("status") == "completed" or not t.get("due_date"):
            continue
        try:
            due = _dt(t["due_date"]).date()
        except Exception:
            continue
        if due < today:
            bucket = 0
        elif due == today:
            bucket = 1
        elif due == today + timedelta(days=1):
            bucket = 2
        else:
            continue
        for u in task_recipients(t, users):
            digests.setdefault(u["id"], ([], [], []))[bucket].append(t)
    return digests

def send_task_reminders() -> dict:
    db = get_client()
    now = datetime.now(CENTRAL)
    users = active_users(db)
    tasks = db.table("tasks").select("*").neq("status", "completed").execute().data or []
    digests = build_digests(tasks, users, now.date())
    if not digests:
        return {"sent": 0, "message": "No tasks to remind about"}

    sent, errors = 0, []
    for u in users:
        if u["id"] not in digests or not u.get("email"):
            continue
        overdue, today, tomorrow = digests[u["id"]]
        total = len(overdue) + len(today) + len(tomorrow)
        res = _send(
            u["email"],
            f"📋 {total} Task{'s' if total != 1 else ''} Need{'s' if total == 1 else ''} Your Attention — {now.strftime('%b %d')}",
            _build_email_html(u, users, overdue, today, tomorrow),
        )
        if res["ok"]:
            sent += 1
        else:
            errors.append(f"{u['email']}: {res['error']}")
    return {"sent": sent, "errors": errors}
