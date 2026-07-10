"""Alerting for big commission-audit findings.

After every statement import (manual upload or daily email ingest) the
pipeline calls notify_big_findings: material discrepancies become ai_alerts
rows (deduped against open alerts, like ai_agent.run_full_scan) and one
summary email goes to the admin. Thresholds are env-tunable.
"""
import os
from datetime import datetime, timezone

try:
    import resend
except Exception:  # pragma: no cover
    resend = None

ADMIN_EMAIL = os.environ.get("ADMIN_ALERT_EMAIL", "lance112709@gmail.com")
FROM_EMAIL = os.environ.get("REMINDER_FROM_EMAIL", "hello@saigonllc.com")

MISSING_THRESHOLD = float(os.environ.get("AUDIT_ALERT_MISSING_USD", "500"))
SHORTPAID_THRESHOLD = float(os.environ.get("AUDIT_ALERT_SHORTPAID_USD", "250"))


def _alert(db, alert_type: str, entity_id: str, message: str, severity: str,
           metadata: dict = None) -> bool:
    """Insert an ai_alert unless an open one already exists for this entity."""
    try:
        existing = db.table("ai_alerts").select("id").eq("type", alert_type) \
            .eq("entity_id", str(entity_id)).eq("status", "open") \
            .limit(1).execute().data
        if existing:
            return False
        db.table("ai_alerts").insert({
            "type": alert_type, "entity_type": "commission_audit",
            "entity_id": str(entity_id), "message": message[:500],
            "severity": severity, "status": "open",
            "metadata": metadata or {},
        }).execute()
        return True
    except Exception:
        return False


def notify_big_findings(db, provider_name: str, run_results: list,
                        findings: list) -> dict:
    """Write alerts + email the admin when an import surfaces real money."""
    lines = []

    for f in findings or []:
        sev = "critical" if f.get("finding_type") == "systemic_rate_change" else "warning"
        created = _alert(db, f.get("finding_type", "commission_audit"), f.get("id"),
                         f.get("title", ""), sev,
                         {"impact": f.get("estimated_impact"),
                          "affected": f.get("affected_count"),
                          "billing_month": str(f.get("billing_month"))[:10]})
        if created:
            lines.append(f"• {f.get('title')} — est. ${float(f.get('estimated_impact') or 0):,.2f}")

    for r in run_results or []:
        month = r.get("billing_month", "")
        missing_loss = 0.0
        if r.get("missing"):
            missing_loss = max(0.0, float(r.get("total_expected") or 0)
                               - float(r.get("total_actual") or 0))
        if r.get("missing") and missing_loss >= MISSING_THRESHOLD:
            msg = (f"{provider_name} {month}: {r['missing']} active account(s) missing "
                   f"from the statement — up to ${missing_loss:,.2f} unpaid.")
            if _alert(db, "missing_commission", f"{r.get('run_id')}", msg, "critical",
                      {"missing": r["missing"], "loss": round(missing_loss, 2)}):
                lines.append("• " + msg)
        if r.get("short_paid"):
            disc = abs(min(0.0, float(r.get("total_discrepancy") or 0)))
            if disc >= SHORTPAID_THRESHOLD:
                msg = (f"{provider_name} {month}: {r['short_paid']} account(s) paid at the "
                       f"wrong rate — ${disc:,.2f} short this month.")
                if _alert(db, "wrong_rate", f"{r.get('run_id')}", msg, "warning",
                          {"short_paid": r["short_paid"], "loss": round(disc, 2)}):
                    lines.append("• " + msg)

    emailed = False
    if lines and resend is not None:
        if not getattr(resend, "api_key", None):
            resend.api_key = os.environ.get("RESEND_API_KEY", "")
        if resend.api_key:
            try:
                today = datetime.now(timezone.utc).strftime("%b %d")
                resend.Emails.send({
                    "from": f"Saigon CRM <{FROM_EMAIL}>",
                    "to": [ADMIN_EMAIL],
                    "subject": f"Commission audit: {provider_name} needs attention ({today})",
                    "html": ("<p>The latest statement import flagged:</p><p>"
                             + "<br>".join(lines)
                             + "</p><p>Review in the CRM → Reconciliation page.</p>"),
                })
                emailed = True
            except Exception:
                pass

    return {"alerts": len(lines), "emailed": emailed}
