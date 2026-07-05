"""Customer-facing email — branded templates sent via Resend.

Every send is audit-logged. When RESEND_API_KEY isn't configured the
functions return a clear error instead of failing silently (the CRM UI
surfaces it), so a missing key can never look like a sent email.
"""
import base64
import os
from datetime import datetime, timezone

FROM_EMAIL = os.environ.get("CUSTOMER_FROM_EMAIL",
                            os.environ.get("REMINDER_FROM_EMAIL", "hello@saigonllc.com"))
REPLY_TO = os.environ.get("CUSTOMER_REPLY_TO", "info@saigonllc.com")
SITE = "https://saigonpowertx.com"
PHONE_DISPLAY = "(832) 937-9999"
PHONE_TEL = "8329379999"


def send_email(to: str, subject: str, html: str, attachments: list = None) -> dict:
    try:
        import resend
        if not getattr(resend, "api_key", None):
            resend.api_key = os.environ.get("RESEND_API_KEY", "")
        if not resend.api_key:
            return {"ok": False, "error": "Email isn't connected yet — set RESEND_API_KEY in Railway (and verify your domain in Resend)."}
        payload = {
            "from": f"Saigon Power <{FROM_EMAIL}>",
            "to": [to],
            "reply_to": REPLY_TO,
            "subject": subject,
            "html": html,
        }
        if attachments:
            payload["attachments"] = attachments
        result = resend.Emails.send(payload)
        return {"ok": True, "id": (result or {}).get("id")}
    except Exception as e:
        return {"ok": False, "error": f"Email failed: {str(e)[:200]}"}


def _shell(inner: str) -> str:
    """Branded wrapper — table-based so it renders everywhere."""
    year = datetime.now(timezone.utc).year
    return f"""<!DOCTYPE html>
<html><body style="margin:0;padding:0;background:#f4f6fa;font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f6fa;padding:24px 12px;"><tr><td align="center">
<table width="560" cellpadding="0" cellspacing="0" style="max-width:560px;width:100%;">
  <tr><td style="background:#0a1a0e;border-radius:16px 16px 0 0;padding:20px 28px;">
    <table cellpadding="0" cellspacing="0"><tr>
      <td><img src="{SITE}/icon-192.png" width="34" height="34" alt="" style="border-radius:8px;display:block;"></td>
      <td style="padding-left:10px;color:#ffffff;font-weight:800;font-size:17px;">Saigon Power</td>
    </tr></table>
  </td></tr>
  <tr><td style="background:#ffffff;padding:28px;border:1px solid #e6e9f2;border-top:0;">
    {inner}
  </td></tr>
  <tr><td style="background:#ffffff;border:1px solid #e6e9f2;border-top:0;border-radius:0 0 16px 16px;padding:18px 28px;color:#94a3b8;font-size:12px;line-height:1.6;">
    Saigon Power LLC · Texas Broker VID 319010 · {PHONE_DISPLAY}<br>
    Questions? Just reply to this email or call us — we speak English &amp; Tiếng Việt.<br>
    © {year} Saigon Power LLC
  </td></tr>
</table>
</td></tr></table></body></html>"""


def _button(url: str, label: str, color: str = "#16a34a") -> str:
    return (f'<a href="{url}" style="display:inline-block;background:{color};color:#ffffff;'
            f'text-decoration:none;font-weight:800;font-size:15px;padding:14px 28px;'
            f'border-radius:12px;">{label}</a>')


def contract_email_html(p: dict) -> str:
    name = (p.get("customer_name") or "there").split(" ")[0]
    link = f"{SITE}/proposal/{p['token']}"
    rate = f"{float(p['rate']):g}¢/kWh" if p.get("rate") is not None else ""
    term = f"{p['term_months']}-month" if p.get("term_months") else ""
    signed = bool(p.get("signed_contract_url"))
    if signed:
        inner = f"""
    <h1 style="margin:0 0 12px;font-size:22px;color:#0f1d3d;">Your signed contract, {name} 🎉</h1>
    <p style="color:#475569;font-size:15px;line-height:1.6;margin:0 0 18px;">
      Attached is your signed electricity agreement{f" — <b>{p['plan_name']}</b>" if p.get('plan_name') else ""}
      {f"at <b>{rate}</b>" if rate else ""}{f" for a <b>{term}</b> term" if term else ""}.
      Keep it for your records. We'll handle everything with the provider from here.</p>
    <p style="margin:0 0 6px;">{_button(link, "View my contract online")}</p>"""
    else:
        inner = f"""
    <h1 style="margin:0 0 12px;font-size:22px;color:#0f1d3d;">Your contract is ready to sign, {name}</h1>
    <p style="color:#475569;font-size:15px;line-height:1.6;margin:0 0 10px;">
      Here's your electricity agreement{f" — <b>{p['plan_name']}</b>" if p.get('plan_name') else ""}
      {f"at <b>{rate}</b>" if rate else ""}{f", <b>{term}</b> term" if term else ""}.
      Review and sign online — it takes about a minute on your phone.</p>
    <table style="background:#f1f5f9;border-radius:12px;width:100%;margin:0 0 18px;"><tr><td style="padding:14px 18px;font-size:14px;color:#334155;line-height:1.8;">
      {"".join(f"<b>{k}:</b> {v}<br>" for k, v in [
          ("Plan", p.get("plan_name")), ("Rate", rate or None),
          ("Term", term or None), ("Service address", p.get("customer_address")),
      ] if v)}
    </td></tr></table>
    <p style="margin:0 0 6px;">{_button(link, "Review & Sign My Contract ✍️")}</p>
    <p style="color:#94a3b8;font-size:12px;margin:14px 0 0;">
      Prefer to talk it through first? Call or text {PHONE_DISPLAY}.</p>"""
    return _shell(inner)


def renewal_email_html(name: str, provider: str, plan: str, end_date: str,
                       days_left, top_plans: list) -> str:
    first = (name or "there").split(" ")[0]
    urgency = (f"in <b style='color:#dc2626;'>{days_left} days</b>" if days_left is not None and days_left <= 30
               else f"in <b>{days_left} days</b>" if days_left is not None
               else "soon")
    plans_rows = "".join(
        f"""<tr>
        <td style="padding:10px 14px;border-top:1px solid #e6e9f2;font-size:14px;color:#334155;">
          <b>{pl.get('plan_name','Plan')}</b><br><span style="color:#94a3b8;font-size:12px;">{pl.get('term_months','')}-month term</span></td>
        <td style="padding:10px 14px;border-top:1px solid #e6e9f2;text-align:right;font-size:18px;font-weight:800;color:#0f1d3d;white-space:nowrap;">
          {pl.get('rate','')}<span style="font-size:11px;color:#94a3b8;">¢/kWh</span></td></tr>"""
        for pl in (top_plans or [])[:3])
    inner = f"""
    <h1 style="margin:0 0 12px;font-size:22px;color:#0f1d3d;">{first}, your electricity contract ends {urgency}</h1>
    <p style="color:#475569;font-size:15px;line-height:1.6;margin:0 0 10px;">
      Your <b>{plan or 'current plan'}</b> with <b>{provider or 'your provider'}</b> ends
      {f"on <b>{end_date}</b>" if end_date else "soon"}. If it rolls over, providers usually move you to an
      expensive variable rate — often 30–50% higher. Let's lock in a better one before that happens.</p>
    {f'''<p style="margin:16px 0 6px;font-size:12px;font-weight:800;color:#94a3b8;text-transform:uppercase;letter-spacing:1px;">Today's rates</p>
    <table cellpadding="0" cellspacing="0" style="width:100%;background:#f8fafc;border-radius:12px;overflow:hidden;margin:0 0 18px;">{plans_rows}</table>''' if plans_rows else ""}
    <p style="margin:0 0 10px;">{_button(f"{SITE}/enroll", "Lock In My New Rate — 2 Minutes")}</p>
    <p style="color:#475569;font-size:14px;margin:0 0 4px;">
      Or simply reply to this email / call {PHONE_DISPLAY} and we'll handle everything —
      <b>our service is always 100% free to you.</b></p>"""
    return _shell(inner)


def fetch_signed_pdf_attachment(db, storage_path: str) -> dict:
    """Download the signed contract from storage and shape it as a Resend attachment."""
    try:
        blob = db.storage.from_("contracts").download(storage_path)
        if blob and len(blob) < 8_000_000:
            return {"filename": "Saigon-Power-Contract.pdf",
                    "content": base64.b64encode(blob).decode()}
    except Exception:
        pass
    return None
