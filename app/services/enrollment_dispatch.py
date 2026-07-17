"""Provider enrollment dispatch — the "plug in the API and it works" layer.

Each provider gets a row in provider_integrations:

  integration_type  'manual' — enrollments queue for you to key into the
                    provider portal yourself, then mark sent.
                    'rest'   — enrollments auto-submit over HTTPS.
  endpoint_url      the provider's enrollment API URL
  auth_type         none | bearer | basic | api_key_header
  auth_credentials  {"token": ...} | {"username":..,"password":..}
                    | {"header_name":"X-Api-Key","api_key":...}
  extra_headers     any additional static headers
  field_mapping     the provider's JSON payload as a template; any string
                    may reference enrollment fields with {{placeholders}}:

    {"customer": {"first": "{{first_name}}", "last": "{{last_name}}"},
     "esiid": "{{esiid}}", "plan_code": "SGP-{{term_months}}",
     "start": "{{requested_start_date}}", "broker_id": "BR200202"}

  test_mode         when true, everything is rendered and logged but the
                    HTTP call is skipped — flip off once the provider
                    confirms the payload looks right.

Every attempt (test or live) is appended to enrollments.submission_log and
audit-logged. A 2xx response moves the enrollment to sent_to_provider and
captures a confirmation number when one is present in the response.
"""
import json
import re
from datetime import datetime, timezone

import httpx

from app.services.audit import audit

TEMPLATE_FIELDS = [
    "id", "first_name", "last_name", "email", "phone", "language",
    "service_address", "service_city", "service_state", "service_zip",
    "esiid", "enrollment_type", "requested_start_date",
    "plan_name", "provider", "rate", "term_months",
]

CONFIRMATION_KEYS = ["confirmation_number", "confirmationNumber", "confirmation",
                     "enrollment_id", "enrollmentId", "reference", "id", "order_id"]


def render_template(node, enrollment: dict):
    """Recursively substitute {{field}} placeholders through the mapping."""
    if isinstance(node, dict):
        return {k: render_template(v, enrollment) for k, v in node.items()}
    if isinstance(node, list):
        return [render_template(v, enrollment) for v in node]
    if isinstance(node, str):
        def sub(m):
            key = m.group(1).strip()
            v = enrollment.get(key)
            return "" if v is None else str(v)
        out = re.sub(r"\{\{([^}]+)\}\}", sub, node)
        # a bare "{{field}}" keeps its native type (numbers stay numbers)
        m = re.fullmatch(r"\{\{([^}]+)\}\}", node.strip())
        if m:
            return enrollment.get(m.group(1).strip())
        return out
    return node


def build_request(integration: dict, enrollment: dict) -> dict:
    payload = render_template(integration.get("field_mapping") or {}, enrollment)
    headers = {"Content-Type": "application/json"}
    headers.update(integration.get("extra_headers") or {})
    creds = integration.get("auth_credentials") or {}
    auth = None
    at = integration.get("auth_type") or "none"
    if at == "bearer" and creds.get("token"):
        headers["Authorization"] = f"Bearer {creds['token']}"
    elif at == "basic" and creds.get("username") is not None:
        auth = (creds.get("username", ""), creds.get("password", ""))
    elif at == "api_key_header" and creds.get("api_key"):
        headers[creds.get("header_name") or "X-Api-Key"] = creds["api_key"]
    return {
        "method": (integration.get("http_method") or "POST").upper(),
        "url": integration.get("endpoint_url") or "",
        "headers": headers,
        "auth": auth,
        "payload": payload,
    }


def _extract_confirmation(body) -> str:
    try:
        data = body if isinstance(body, dict) else json.loads(body)
        for k in CONFIRMATION_KEYS:
            if isinstance(data, dict) and data.get(k):
                return str(data[k])[:100]
    except Exception:
        pass
    return ""


def dispatch_enrollment(db, enrollment_id: str, actor: str = "system",
                        force: bool = False) -> dict:
    """Send one enrollment to its provider. Returns the log entry."""
    enr = db.table("enrollments").select("*").eq("id", enrollment_id).limit(1).execute().data
    if not enr:
        return {"ok": False, "error": "Enrollment not found"}
    enr = enr[0]

    integ = db.table("provider_integrations").select("*") \
        .ilike("provider_name", enr.get("provider") or "").limit(1).execute().data
    integ = integ[0] if integ else None

    now = datetime.now(timezone.utc).isoformat()
    log = list(enr.get("submission_log") or [])

    def finish(entry: dict, new_status: str = None, confirmation: str = None):
        log.append(entry)
        updates = {"submission_log": log, "updated_at": now}
        if new_status:
            updates["status"] = new_status
        if confirmation:
            updates["provider_confirmation"] = confirmation
        db.table("enrollments").update(updates).eq("id", enrollment_id).execute()
        audit(db, "enrollments", enrollment_id, "dispatch_attempt",
              None, {k: v for k, v in entry.items() if k != "payload"},
              reason=f"Provider: {enr.get('provider')}", actor=actor)
        return {"ok": entry.get("success", False), **entry}

    if integ is None or integ.get("integration_type") != "rest" or \
            (not integ.get("is_active") and not force):
        return finish({
            "at": now, "mode": "manual",
            "success": False,
            "message": "No active API integration for this provider — enrollment queued for manual submission.",
        })

    req = build_request(integ, enr)
    if not req["url"]:
        return finish({"at": now, "mode": "rest", "success": False,
                       "message": "Integration has no endpoint URL configured."}, "needs_review")

    if integ.get("test_mode"):
        return finish({
            "at": now, "mode": "test",
            "success": True,
            "message": "TEST MODE — payload rendered but not sent. Flip test mode off to go live.",
            "payload": req["payload"],
        })

    try:
        with httpx.Client(timeout=25) as client:
            resp = client.request(req["method"], req["url"], headers=req["headers"],
                                  auth=req["auth"], json=req["payload"])
        body_text = resp.text[:2000]
        if 200 <= resp.status_code < 300:
            confirmation = _extract_confirmation(body_text)
            db.table("provider_integrations").update(
                {"last_result": {"at": now, "status": resp.status_code, "ok": True}, "updated_at": now}
            ).eq("id", integ["id"]).execute()
            return finish({
                "at": now, "mode": "rest", "success": True,
                "http_status": resp.status_code,
                "message": f"Submitted to {enr.get('provider')} — HTTP {resp.status_code}"
                           + (f", confirmation {confirmation}" if confirmation else ""),
                "response_excerpt": body_text[:500],
            }, "sent_to_provider", confirmation or None)
        db.table("provider_integrations").update(
            {"last_result": {"at": now, "status": resp.status_code, "ok": False,
                             "body": body_text[:500]}, "updated_at": now}
        ).eq("id", integ["id"]).execute()
        return finish({
            "at": now, "mode": "rest", "success": False,
            "http_status": resp.status_code,
            "message": f"Provider rejected the submission (HTTP {resp.status_code}).",
            "response_excerpt": body_text[:500],
        }, "needs_review")
    except Exception as e:
        return finish({
            "at": now, "mode": "rest", "success": False,
            "message": f"Could not reach provider API: {type(e).__name__}: {str(e)[:200]}",
        }, "needs_review")
