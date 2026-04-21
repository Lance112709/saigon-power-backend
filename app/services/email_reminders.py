import os
import resend
from datetime import datetime, timezone, timedelta
from app.db.client import get_client

resend.api_key = os.environ.get("RESEND_API_KEY", "")

FROM_EMAIL = os.environ.get("REMINDER_FROM_EMAIL", "reminders@saigonpower.com")

def _dt(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

def _priority_color(p: str) -> str:
    return {"high": "#dc2626", "medium": "#d97706", "low": "#6b7280"}.get(p, "#6b7280")

def _lead_url(task: dict) -> str:
    base = os.environ.get("FRONTEND_URL", "https://saigon-power-frontend.vercel.app").split(",")[0].strip()
    if task.get("lead_id"):
        return f"{base}/crm/leads/{task['lead_id']}"
    if task.get("customer_id"):
        return f"{base}/crm/customers/{task['customer_id']}"
    return f"{base}/tasks"

def _build_email_html(user_name: str, overdue: list, today: list, tomorrow: list) -> str:
    total = len(overdue) + len(today) + len(tomorrow)

    def task_rows(tasks: list, label_color: str, label: str) -> str:
        if not tasks:
            return ""
        rows = ""
        for t in tasks:
            due = ""
            if t.get("due_date"):
                try:
                    due = _dt(t["due_date"]).strftime("%b %d")
                except Exception:
                    pass
            url = _lead_url(t)
            pcolor = _priority_color(t.get("priority", "low"))
            rows += f"""
            <tr>
              <td style="padding:10px 16px;border-bottom:1px solid #f1f5f9;">
                <a href="{url}" style="color:#0f1d5e;font-weight:600;text-decoration:none;">{t.get('title','—')}</a>
              </td>
              <td style="padding:10px 16px;border-bottom:1px solid #f1f5f9;white-space:nowrap;">
                <span style="background:{label_color}20;color:{label_color};padding:2px 8px;border-radius:99px;font-size:12px;font-weight:600;">{label}</span>
              </td>
              <td style="padding:10px 16px;border-bottom:1px solid #f1f5f9;color:#64748b;font-size:13px;white-space:nowrap;">{due}</td>
              <td style="padding:10px 16px;border-bottom:1px solid #f1f5f9;">
                <span style="color:{pcolor};font-size:12px;font-weight:600;">{t.get('priority','—').upper()}</span>
              </td>
            </tr>"""
        return rows

    all_rows = (
        task_rows(overdue, "#dc2626", "Overdue") +
        task_rows(today, "#d97706", "Due Today") +
        task_rows(tomorrow, "#2563eb", "Due Tomorrow")
    )

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f4f6fa;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
  <div style="max-width:640px;margin:32px auto;background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.08);">

    <!-- Header -->
    <div style="background:#0f1d5e;padding:24px 32px;text-align:center;">
      <p style="margin:0;color:#4ade80;font-size:13px;font-weight:600;letter-spacing:.05em;">SAIGON POWER</p>
      <h1 style="margin:4px 0 0;color:#fff;font-size:20px;font-weight:700;">Daily Task Reminder</h1>
      <p style="margin:6px 0 0;color:#94a3b8;font-size:13px;">{datetime.now(timezone.utc).strftime('%A, %B %d, %Y')}</p>
    </div>

    <!-- Greeting -->
    <div style="padding:24px 32px 16px;">
      <p style="margin:0;color:#1e293b;font-size:15px;">Hi <strong>{user_name}</strong>,</p>
      <p style="margin:8px 0 0;color:#64748b;font-size:14px;">
        You have <strong style="color:#0f1d5e;">{total} task{'s' if total != 1 else ''}</strong> that need{'s' if total == 1 else ''} your attention today.
      </p>
    </div>

    <!-- Stats strip -->
    <div style="display:flex;gap:0;padding:0 32px 20px;">
      <div style="flex:1;text-align:center;background:#fef2f2;border-radius:10px;padding:12px;margin-right:8px;">
        <p style="margin:0;font-size:22px;font-weight:700;color:#dc2626;">{len(overdue)}</p>
        <p style="margin:4px 0 0;font-size:11px;color:#dc2626;font-weight:600;">OVERDUE</p>
      </div>
      <div style="flex:1;text-align:center;background:#fffbeb;border-radius:10px;padding:12px;margin-right:8px;">
        <p style="margin:0;font-size:22px;font-weight:700;color:#d97706;">{len(today)}</p>
        <p style="margin:4px 0 0;font-size:11px;color:#d97706;font-weight:600;">DUE TODAY</p>
      </div>
      <div style="flex:1;text-align:center;background:#eff6ff;border-radius:10px;padding:12px;">
        <p style="margin:0;font-size:22px;font-weight:700;color:#2563eb;">{len(tomorrow)}</p>
        <p style="margin:4px 0 0;font-size:11px;color:#2563eb;font-weight:600;">DUE TOMORROW</p>
      </div>
    </div>

    <!-- Task table -->
    <div style="padding:0 16px 24px;">
      <table style="width:100%;border-collapse:collapse;background:#f8fafc;border-radius:12px;overflow:hidden;">
        <thead>
          <tr style="background:#e2e8f0;">
            <th style="padding:10px 16px;text-align:left;font-size:11px;color:#64748b;font-weight:600;text-transform:uppercase;letter-spacing:.05em;">Task</th>
            <th style="padding:10px 16px;text-align:left;font-size:11px;color:#64748b;font-weight:600;text-transform:uppercase;letter-spacing:.05em;">Status</th>
            <th style="padding:10px 16px;text-align:left;font-size:11px;color:#64748b;font-weight:600;text-transform:uppercase;letter-spacing:.05em;">Due</th>
            <th style="padding:10px 16px;text-align:left;font-size:11px;color:#64748b;font-weight:600;text-transform:uppercase;letter-spacing:.05em;">Priority</th>
          </tr>
        </thead>
        <tbody>{all_rows}</tbody>
      </table>
    </div>

    <!-- CTA -->
    <div style="padding:0 32px 32px;text-align:center;">
      <a href="{os.environ.get('FRONTEND_URL','https://saigon-power-frontend.vercel.app').split(',')[0].strip()}/tasks"
         style="display:inline-block;background:#0f1d5e;color:#fff;padding:12px 32px;border-radius:10px;font-size:14px;font-weight:600;text-decoration:none;">
        View All Tasks →
      </a>
    </div>

    <!-- Footer -->
    <div style="padding:16px 32px;border-top:1px solid #f1f5f9;text-align:center;">
      <p style="margin:0;font-size:12px;color:#94a3b8;">Saigon Power LLC · Automated reminder · Do not reply</p>
    </div>
  </div>
</body>
</html>"""


def send_task_reminders() -> dict:
    if not resend.api_key:
        return {"error": "RESEND_API_KEY not set"}

    db = get_client()
    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end   = today_start + timedelta(days=1)
    tomorrow_end = today_start + timedelta(days=2)

    # Fetch all pending tasks
    tasks_res = db.table("tasks").select("*").neq("status", "completed").execute()
    all_tasks = tasks_res.data or []

    overdue  = [t for t in all_tasks if t.get("due_date") and _dt(t["due_date"]) < today_start]
    today    = [t for t in all_tasks if t.get("due_date") and today_start <= _dt(t["due_date"]) < today_end]
    tomorrow = [t for t in all_tasks if t.get("due_date") and today_end <= _dt(t["due_date"]) < tomorrow_end]

    if not overdue and not today and not tomorrow:
        return {"sent": 0, "message": "No tasks to remind about"}

    # Fetch all active users
    users_res = db.table("users").select("name, email").eq("is_active", True).execute()
    users = users_res.data or []

    if not users:
        return {"sent": 0, "message": "No users found"}

    sent = 0
    errors = []
    for u in users:
        email = u.get("email")
        name  = u.get("name") or "Team"
        if not email:
            continue
        html = _build_email_html(name, overdue, today, tomorrow)
        total = len(overdue) + len(today) + len(tomorrow)
        try:
            resend.Emails.send({
                "from": FROM_EMAIL,
                "to": [email],
                "subject": f"📋 {total} Task{'s' if total != 1 else ''} Need Your Attention — {now.strftime('%b %d')}",
                "html": html,
            })
            sent += 1
        except Exception as e:
            errors.append(f"{email}: {str(e)}")

    return {"sent": sent, "errors": errors}
