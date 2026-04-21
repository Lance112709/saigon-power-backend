"""
Send-only SMS service — Telnyx integration.
NO inbound, NO webhooks, NO reply handling.
"""
import os
import re
import httpx
from app.db.client import get_client

TELNYX_API_KEY   = os.environ.get("TELNYX_API_KEY", "")
TELNYX_FROM      = os.environ.get("TELNYX_FROM_NUMBER", "")
TELNYX_URL       = "https://api.telnyx.com/v2/messages"


def _normalize_phone(phone: str) -> str:
    digits = re.sub(r"\D", "", phone or "")
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits[0] == "1":
        return f"+{digits}"
    return f"+{digits}" if digits else ""


def render_template(body: str, variables: dict) -> str:
    for k, v in variables.items():
        body = body.replace(f"{{{{{k}}}}}", str(v or ""))
    return body.strip()


def _is_opted_out(db, lead_id: str = None, customer_id: str = None) -> bool:
    if lead_id:
        res = db.table("leads").select("sms_opt_out").eq("id", lead_id).limit(1).execute()
        return bool(res.data and res.data[0].get("sms_opt_out"))
    if customer_id:
        res = db.table("crm_customers").select("sms_opt_out").eq("id", customer_id).limit(1).execute()
        return bool(res.data and res.data[0].get("sms_opt_out"))
    return False


def send_sms(
    to: str,
    body: str,
    user_id: str = None,
    lead_id: str = None,
    customer_id: str = None,
    deal_id: str = None,
) -> dict:
    db = get_client()

    if _is_opted_out(db, lead_id, customer_id):
        return {"ok": False, "error": "opted_out"}

    if not TELNYX_API_KEY or not TELNYX_FROM:
        return {"ok": False, "error": "SMS not configured — set TELNYX_API_KEY and TELNYX_FROM_NUMBER"}

    to_e164 = _normalize_phone(to)
    if not to_e164:
        return {"ok": False, "error": "Invalid phone number"}

    # Enforce 160-char limit per segment (split at 320 to stay within 2 segments)
    body = body[:320]

    provider_message_id = None
    status = "failed"
    error = None

    try:
        resp = httpx.post(
            TELNYX_URL,
            headers={"Authorization": f"Bearer {TELNYX_API_KEY}", "Content-Type": "application/json"},
            json={"from": TELNYX_FROM, "to": to_e164, "text": body},
            timeout=10,
        )
        resp.raise_for_status()
        provider_message_id = resp.json().get("data", {}).get("id")
        status = "sent"
    except httpx.HTTPStatusError as e:
        error = f"Telnyx error {e.response.status_code}"
    except Exception as e:
        error = str(e)

    db.table("sms_messages").insert({
        "user_id":             user_id,
        "lead_id":             lead_id or None,
        "customer_id":         customer_id or None,
        "deal_id":             deal_id or None,
        "phone_number":        to_e164,
        "message_body":        body,
        "status":              status,
        "provider_message_id": provider_message_id,
    }).execute()

    return {"ok": status == "sent", "status": status, "error": error}


def send_automated(
    trigger_type: str,
    to: str,
    variables: dict,
    user_id: str = None,
    lead_id: str = None,
    customer_id: str = None,
    deal_id: str = None,
) -> dict:
    db = get_client()
    tmpl = db.table("sms_templates").select("message_body").eq("trigger_type", trigger_type).limit(1).execute()
    if not tmpl.data:
        return {"ok": False, "error": f"No template for trigger '{trigger_type}'"}
    body = render_template(tmpl.data[0]["message_body"], variables)
    return send_sms(to, body, user_id=user_id, lead_id=lead_id, customer_id=customer_id, deal_id=deal_id)
