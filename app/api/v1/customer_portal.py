"""Customer Portal ("My Saigon Power") — self-service for END CUSTOMERS.

Login is passwordless: phone number + SMS code. The OTP flow is stateless —
the server signs a short-lived challenge JWT containing a hash of the code,
so nothing is stored and restarts can't strand a login.

Portal sessions use a separate JWT role ('customer') that can never access
staff endpoints.
"""
import hashlib
import os
import random
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Body, Depends, Header, HTTPException, Request
from jose import jwt, JWTError

from app.db.client import get_client
from app.auth.core import SECRET_KEY, ALGORITHM
from app.core.security import rate_limit
from app.services.audit import audit
from app.services.sms import send_sms
from app.utils.deals import is_month_to_month

router = APIRouter()

OTP_TTL_MINUTES = 10
SESSION_DAYS = 30


def phone10(v) -> str:
    d = re.sub(r"\D", "", str(v or ""))
    return d[-10:] if len(d) >= 10 else d


def _hash_code(code: str, phone: str) -> str:
    return hashlib.sha256(f"{code}:{phone}:{SECRET_KEY}".encode()).hexdigest()


def _find_customer(db, p10: str) -> Optional[dict]:
    """Match a phone against leads and imported customers (any format)."""
    for l in _fetch(db, "leads", "id, first_name, last_name, phone"):
        if phone10(l.get("phone")) == p10:
            return {"kind": "lead", "id": l["id"],
                    "name": f"{l.get('first_name','')} {l.get('last_name','')}".strip()}
    for c in _fetch(db, "crm_customers", "id, full_name, phone"):
        if phone10(c.get("phone")) == p10:
            return {"kind": "customer", "id": c["id"], "name": c.get("full_name") or ""}
    try:
        for s in _fetch(db, "giadienre_subscriptions", "id, full_name, phone_digits"):
            if phone10(s.get("phone_digits")) == p10:
                return {"kind": "giadienre", "id": s["id"], "name": s.get("full_name") or ""}
    except Exception:
        pass
    return None


def _fetch(db, table, cols):
    out, off = [], 0
    while True:
        page = db.table(table).select(cols).range(off, off + 999).execute().data or []
        out.extend(page)
        if len(page) < 1000:
            break
        off += 1000
    return out


# ── OTP login ─────────────────────────────────────────────────────────────────

@router.post("/request-code")
def request_code(data: dict = Body(...), request: Request = None):
    p10 = phone10(data.get("phone"))
    if len(p10) != 10:
        raise HTTPException(status_code=400, detail="Enter a valid 10-digit phone number.")

    # Throttle code requests: per IP and per phone (SMS cost + enumeration).
    if request is not None:
        rate_limit(request, "otp_request", limit=5, window_seconds=600)
        rate_limit(request, f"otp_request_phone:{p10}", limit=5, window_seconds=3600)

    db = get_client()
    cust = _find_customer(db, p10)
    if cust is None:
        raise HTTPException(status_code=404,
                            detail="We couldn't find an account with that number. Call us at (832) 937-9999 and we'll get you set up.")

    code = f"{random.SystemRandom().randint(0, 999999):06d}"
    sent = send_sms(f"+1{p10}", f"Your Saigon Power login code is {code}. It expires in {OTP_TTL_MINUTES} minutes.")
    if not sent.get("ok"):
        raise HTTPException(status_code=503,
                            detail="We couldn't text you right now — please call us at (832) 937-9999.")

    challenge = jwt.encode({
        "purpose": "portal_otp",
        "phone": p10,
        "code_hash": _hash_code(code, p10),
        "name": cust["name"],
        "exp": datetime.now(timezone.utc) + timedelta(minutes=OTP_TTL_MINUTES),
    }, SECRET_KEY, algorithm=ALGORITHM)

    first = (cust["name"].split() or ["there"])[0]
    return {"ok": True, "challenge": challenge, "hint": f"Code sent to •••-•••-{p10[-4:]}", "first_name": first}


@router.post("/verify-code")
def verify_code(data: dict = Body(...), request: Request = None):
    p10 = phone10(data.get("phone"))
    # Stop online brute force of the 6-digit code (1M space, 10-min window).
    if request is not None:
        rate_limit(request, "otp_verify", limit=8, window_seconds=600)
        rate_limit(request, f"otp_verify_phone:{p10}", limit=8, window_seconds=600)
    code = re.sub(r"\D", "", str(data.get("code") or ""))
    try:
        payload = jwt.decode(str(data.get("challenge") or ""), SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        raise HTTPException(status_code=400, detail="That code expired — request a new one.")
    if payload.get("purpose") != "portal_otp" or payload.get("phone") != p10:
        raise HTTPException(status_code=400, detail="That code expired — request a new one.")
    if _hash_code(code, p10) != payload.get("code_hash"):
        raise HTTPException(status_code=400, detail="Wrong code — check the text message and try again.")

    token = jwt.encode({
        "role": "customer",
        "phone": p10,
        "name": payload.get("name") or "",
        "exp": datetime.now(timezone.utc) + timedelta(days=SESSION_DAYS),
    }, SECRET_KEY, algorithm=ALGORITHM)
    return {"ok": True, "token": token, "name": payload.get("name") or ""}


def portal_user(authorization: str = Header(default="")) -> dict:
    token = authorization.replace("Bearer ", "").strip()
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        raise HTTPException(status_code=401, detail="Please log in again.")
    if payload.get("role") != "customer":
        raise HTTPException(status_code=401, detail="Please log in again.")
    return payload


# ── My account ────────────────────────────────────────────────────────────────

def _days_left(end) -> Optional[int]:
    try:
        return (datetime.strptime(str(end)[:10], "%Y-%m-%d").date() - datetime.now(timezone.utc).date()).days
    except Exception:
        return None


@router.get("/me")
def me(user: dict = Depends(portal_user)):
    db = get_client()
    p10 = user["phone"]

    plans = []
    lead_ids = [l["id"] for l in _fetch(db, "leads", "id, phone") if phone10(l.get("phone")) == p10]
    for i in range(0, len(lead_ids), 50):
        for d in db.table("lead_deals").select("*").in_("lead_id", lead_ids[i:i + 50]).execute().data or []:
            plans.append({
                "source": "lead_deals", "id": d["id"],
                "plan_name": d.get("plan_name") or d.get("rate_type") or "Electricity plan",
                "provider": d.get("supplier"), "rate": d.get("rate"),
                "term": d.get("contract_term"),
                "address": d.get("service_address"),
                "start": d.get("start_date"), "end": d.get("end_date"),
                "active": d.get("status") == "Active", "status": d.get("status"),
                "provider_status": d.get("provider_status"),
                "month_to_month": is_month_to_month(d.get("rate_type"), d.get("plan_name"), d.get("contract_term")),
                "days_left": _days_left(d.get("end_date")),
                "esiid_tail": (d.get("esiid") or "")[-6:] or None,
            })
    cust_ids = [c["id"] for c in _fetch(db, "crm_customers", "id, phone") if phone10(c.get("phone")) == p10]
    for i in range(0, len(cust_ids), 50):
        for d in db.table("crm_deals").select("*").in_("customer_id", cust_ids[i:i + 50]).execute().data or []:
            plans.append({
                "source": "crm_deals", "id": d["id"],
                "plan_name": d.get("product_type") or "Electricity plan",
                "provider": d.get("provider"), "rate": d.get("energy_rate"),
                "term": d.get("contract_term"),
                "address": d.get("service_address"),
                "start": d.get("contract_start_date"), "end": d.get("contract_end_date"),
                "active": d.get("deal_status") == "ACTIVE", "status": d.get("deal_status"),
                "provider_status": d.get("provider_status"),
                "month_to_month": is_month_to_month(d.get("product_type"), d.get("contract_term")),
                "days_left": _days_left(d.get("contract_end_date")),
                "esiid_tail": (d.get("esiid") or "")[-6:] or None,
            })
    plans.sort(key=lambda p: (not p["active"], p["days_left"] if p["days_left"] is not None else 9999))

    enrollments = [e for e in _fetch(db, "enrollments",
                                     "id, status, plan_name, provider, created_at, requested_start_date, phone")
                   if phone10(e.get("phone")) == p10]
    enrollments.sort(key=lambda e: e["created_at"], reverse=True)

    ref_code = p10
    referrals = [e for e in _fetch(db, "enrollments", "id, status, source, created_at")
                 if f"ref:{ref_code}" in (e.get("source") or "")]

    # GiaDienRe membership (website subscription), matched by phone
    membership = None
    try:
        rows = db.table("giadienre_subscriptions").select("*") \
            .or_(f"phone_digits.eq.{p10},phone_digits.eq.1{p10}") \
            .order("created_at", desc=True).limit(1).execute().data
        if rows:
            s = rows[0]
            membership = {
                "id": s["id"],
                "plan_id": s.get("plan_id"),
                "plan_name": s.get("plan_name"),
                "billing_cycle": s.get("billing_cycle"),
                "status": s.get("status"),
                "subscribed_at": s.get("subscribed_at"),
                "card_last4": s.get("card_last4"),
                "card_brand": s.get("card_brand"),
                "card_expiry": s.get("card_expiry"),
                "last_payment_at": s.get("last_payment_at"),
                "next_billing_date": s.get("next_billing_date"),
            }
    except Exception:
        pass  # membership table may predate this feature

    return {
        "name": user.get("name") or "",
        "phone_tail": p10[-4:],
        "membership": membership,
        "plans": plans,
        "enrollments": [{k: v for k, v in e.items() if k != "phone"} for e in enrollments[:10]],
        "referral": {
            "code": ref_code,
            "link": f"https://saigonpowertx.com/enroll?ref={ref_code}",
            "count": len(referrals),
            "active": sum(1 for r in referrals if r["status"] in ("accepted", "active")),
        },
    }


@router.post("/renewal-request")
def renewal_request(data: dict = Body(...), user: dict = Depends(portal_user)):
    """Customer taps 'Renew / get me a better rate' — lands as an urgent task."""
    db = get_client()
    plan = data.get("plan") or {}
    title = f"📱 Portal renewal request: {user.get('name') or user['phone']}"
    db.table("tasks").insert({
        "task_type": "renewal",
        "title": title,
        "description": f"Customer requested renewal from the portal.\n"
                       f"Phone: {user['phone']}\n"
                       f"Plan: {plan.get('plan_name') or '—'} · {plan.get('provider') or '—'} · "
                       f"ends {plan.get('end') or '—'}",
        "due_date": (datetime.now(timezone.utc) + timedelta(days=1)).isoformat(),
        "status": "pending",
        "priority": "high",
        "lead_id": None, "customer_id": None, "deal_id": None,
    }).execute()
    audit(db, "portal", user["phone"], "renewal_request", None, plan,
          reason="Customer self-service renewal request", actor=f"customer:{user['phone']}")
    # let the office know right away (best effort)
    try:
        send_sms(os.environ.get("ADMIN_ALERT_PHONE", "+18329379999"),
                 f"Portal renewal request from {user.get('name') or user['phone']} — check Tasks in the CRM.")
    except Exception:
        pass
    return {"ok": True, "message": "Got it! Our team will call you within 1 business day with your best options."}


# ── Email + password login (giadienre.com portal) ─────────────────────────────
#
# SMS OTP above is retained for compatibility, but giadienre.com now signs
# customers in with email + password. The Argon2id hash lives in
# giadienre_subscriptions.extra["portal_auth"] (no schema change), and the
# session JWT still carries the customer's phone so every existing portal
# endpoint (/me, billing, bills) keeps working unchanged.
#
# Set/forgot password: a 6-digit code is emailed (stateless challenge JWT,
# same pattern as the SMS flow). Outbound mail tries Resend first and falls
# back to Gmail SMTP (GMAIL_USER / GMAIL_APP_PASSWORD) so it works today.

from app.auth.core import hash_password, verify_password

PWD_CODE_TTL_MINUTES = 15
PWD_MIN_LEN = 8


def _find_sub_by_email(db, email: str) -> Optional[dict]:
    if not email:
        return None
    rows = db.table("giadienre_subscriptions").select("*") \
        .ilike("email", email).order("created_at", desc=True).limit(1).execute().data
    return rows[0] if rows else None


def _sub_session_token(sub: dict) -> str:
    p10 = phone10(sub.get("phone_digits") or sub.get("phone"))
    return jwt.encode({
        "role": "customer",
        "phone": p10,
        "email": (sub.get("email") or "").lower(),
        "name": sub.get("full_name") or "",
        "exp": datetime.now(timezone.utc) + timedelta(days=SESSION_DAYS),
    }, SECRET_KEY, algorithm=ALGORITHM)


def _send_portal_email(to: str, subject: str, html: str) -> bool:
    """Resend if configured, else Gmail SMTP — so codes deliver either way."""
    try:
        from app.services.customer_email import send_email
        if send_email(to, subject, html).get("ok"):
            return True
    except Exception:
        pass
    try:
        import smtplib
        from email.mime.text import MIMEText
        user = os.environ.get("GMAIL_USER", "").strip()
        pwd = os.environ.get("GMAIL_APP_PASSWORD", "").replace(" ", "").strip()
        if not user or not pwd:
            return False
        msg = MIMEText(html, "html")
        msg["Subject"] = subject
        msg["From"] = f"Saigon Power <{user}>"
        msg["To"] = to
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=10) as s:
            s.login(user, pwd)
            s.sendmail(user, [to], msg.as_string())
        return True
    except Exception:
        return False


@router.post("/login")
def portal_login(data: dict = Body(...), request: Request = None):
    email = str(data.get("email") or "").strip().lower()
    password = str(data.get("password") or "")
    if request is not None:
        rate_limit(request, "portal_login", limit=10, window_seconds=600)
        if email:
            rate_limit(request, f"portal_login_email:{email}", limit=10, window_seconds=600)
    if not email or not password:
        raise HTTPException(status_code=400, detail="Email and password are required.")

    db = get_client()
    sub = _find_sub_by_email(db, email)
    if sub is None:
        raise HTTPException(status_code=404, detail="account_not_found")

    pa = (sub.get("extra") or {}).get("portal_auth") or {}
    if not pa.get("password_hash"):
        # account exists but has never set a password → guide to create one
        raise HTTPException(status_code=409, detail="no_password")
    if not verify_password(password, pa["password_hash"]):
        raise HTTPException(status_code=401, detail="wrong_password")

    return {"ok": True, "token": _sub_session_token(sub),
            "name": sub.get("full_name") or ""}


@router.post("/password/request-code")
def portal_password_request_code(data: dict = Body(...), request: Request = None,
                                 x_relay_key: str = Header(default="")):
    email = str(data.get("email") or "").strip().lower()
    if request is not None:
        rate_limit(request, "pwd_code_request", limit=5, window_seconds=600)
        if email:
            rate_limit(request, f"pwd_code_email:{email}", limit=5, window_seconds=3600)
    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="Enter a valid email address.")

    db = get_client()
    sub = _find_sub_by_email(db, email)
    if sub is None:
        raise HTTPException(status_code=404, detail="account_not_found")

    code = f"{random.SystemRandom().randint(0, 999999):06d}"
    html = (f"<div style='font-family:sans-serif;max-width:480px'>"
            f"<h2 style='color:#0f172a'>Saigon Power — GiaDienRe</h2>"
            f"<p>Xin chào {sub.get('full_name') or 'bạn'},</p>"
            f"<p>Mã xác nhận để đặt mật khẩu cho tài khoản của bạn là:</p>"
            f"<p style='font-size:32px;font-weight:bold;letter-spacing:6px;"
            f"color:#16a34a'>{code}</p>"
            f"<p>Mã có hiệu lực trong {PWD_CODE_TTL_MINUTES} phút. "
            f"Nếu bạn không yêu cầu, hãy bỏ qua email này.</p>"
            f"<p style='color:#64748b;font-size:13px'>Saigon Power · (832) 937-9999"
            f" · www.giadienre.com</p></div>")
    # Railway blocks outbound SMTP, so when the trusted giadienre.com server
    # calls (shared CRON_SECRET), it delivers the email from its own network.
    relay_secret = os.environ.get("CRON_SECRET", "").strip()
    relayed = bool(relay_secret) and x_relay_key == relay_secret
    if not relayed:
        if not _send_portal_email(email, "Mã xác nhận Saigon Power (GiaDienRe)", html):
            raise HTTPException(status_code=503, detail="email_send_failed")

    challenge = jwt.encode({
        "purpose": "portal_pwd",
        "email": email,
        "sub_id": sub["id"],
        "code_hash": _hash_code(code, email),
        "exp": datetime.now(timezone.utc) + timedelta(minutes=PWD_CODE_TTL_MINUTES),
    }, SECRET_KEY, algorithm=ALGORITHM)

    local, _, domain = email.partition("@")
    hint = f"{local[0]}•••@{domain}" if local else f"•••@{domain}"
    resp = {"ok": True, "challenge": challenge, "hint": hint}
    if relayed:  # server-to-server only — never reaches a browser
        resp["relay_code"] = code
        resp["relay_name"] = sub.get("full_name") or ""
    return resp


@router.post("/password/reset")
def portal_password_reset(data: dict = Body(...), request: Request = None):
    email = str(data.get("email") or "").strip().lower()
    if request is not None:
        rate_limit(request, "pwd_reset", limit=8, window_seconds=600)
        if email:
            rate_limit(request, f"pwd_reset_email:{email}", limit=8, window_seconds=600)
    code = re.sub(r"\D", "", str(data.get("code") or ""))
    password = str(data.get("password") or "")
    if len(password) < PWD_MIN_LEN:
        raise HTTPException(status_code=400, detail="password_too_short")

    try:
        payload = jwt.decode(str(data.get("challenge") or ""), SECRET_KEY,
                             algorithms=[ALGORITHM])
    except JWTError:
        raise HTTPException(status_code=400, detail="code_expired")
    if payload.get("purpose") != "portal_pwd" or payload.get("email") != email:
        raise HTTPException(status_code=400, detail="code_expired")
    if _hash_code(code, email) != payload.get("code_hash"):
        raise HTTPException(status_code=400, detail="wrong_code")

    db = get_client()
    rows = db.table("giadienre_subscriptions").select("*") \
        .eq("id", payload["sub_id"]).limit(1).execute().data
    if not rows:
        raise HTTPException(status_code=404, detail="account_not_found")
    sub = rows[0]

    extra = dict(sub.get("extra") or {})
    extra["portal_auth"] = {
        "password_hash": hash_password(password),
        "set_at": datetime.now(timezone.utc).isoformat(),
    }
    db.table("giadienre_subscriptions").update({
        "extra": extra, "updated_at": datetime.now(timezone.utc).isoformat(),
    }).eq("id", sub["id"]).execute()

    audit(db, "giadienre_subscriptions", sub["id"], "portal_password_set", None,
          {"email": email}, reason="Customer set portal password via email code",
          actor=f"customer:{email}")

    # auto-login after setting the password
    return {"ok": True, "token": _sub_session_token(sub),
            "name": sub.get("full_name") or ""}
