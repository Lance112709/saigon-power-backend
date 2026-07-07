"""Alert when a provider's monthly commission statement hasn't arrived.

All current providers pay by the 7th of each month. This runs on the 10th
(scheduled in main.py) and emails the admin a list of providers with no
imported statement rows for last month.
"""
import os
from datetime import date

try:
    import resend
except ImportError:  # optional in local dev
    resend = None

from app.db.client import get_client
from app.services.file_parser.provider_parsers import PROVIDER_SUPPLIERS

ADMIN_EMAIL = os.environ.get("ADMIN_ALERT_EMAIL", "lance112709@gmail.com")
FROM_EMAIL = os.environ.get("REMINDER_FROM_EMAIL", "reminders@saigonpower.com")


def last_month_label(today: date = None) -> str:
    d = today or date.today()
    y, m = d.year, d.month - 1
    if m < 1:
        y, m = y - 1, 12
    return f"{y}-{m:02d}"


def check_missing_statements(today: date = None) -> list:
    # Pull any statements sitting in the inbox first, so we never alert about
    # a "missing" statement that already arrived by email.
    try:
        from app.services.email_ingest import poll_inbox
        poll_inbox(actor="watchdog-precheck")
    except Exception:
        pass

    db = get_client()
    label = last_month_label(today)
    missing = []
    for group, sdef in PROVIDER_SUPPLIERS.items():
        sup = db.table("suppliers").select("id").eq("code", sdef["code"]).limit(1).execute().data
        if not sup:
            missing.append(group)
            continue
        rows = db.table("actual_commissions").select("id", count="exact") \
            .eq("supplier_id", sup[0]["id"]).eq("billing_month", f"{label}-01") \
            .limit(1).execute()
        if not (rows.count or 0):
            missing.append(group)

    if resend is not None and not getattr(resend, "api_key", None):
        resend.api_key = os.environ.get("RESEND_API_KEY", "")
    if missing and resend is not None and resend.api_key:
        items = "".join(f"<li><b>{m}</b></li>" for m in missing)
        try:
            resend.Emails.send({
                "from": FROM_EMAIL,
                "to": [ADMIN_EMAIL],
                "subject": f"⚠️ Missing commission statements for {label}",
                "html": f"<p>No commission statement has been imported for <b>{label}</b> from:</p>"
                        f"<ul>{items}</ul>"
                        f"<p>Providers pay by the 7th. Download the statements and upload them to the CRM "
                        f"— matching and reconciliation run automatically.</p>",
            })
        except Exception:
            pass
    return missing
