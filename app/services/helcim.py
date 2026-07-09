"""Helcim payments — HelcimPay.js sessions, card-on-file, recurring billing.

Card numbers never touch our servers: HelcimPay.js renders Helcim's hosted
iframe, tokenizes the card, and returns a customerCode/cardToken that we store.
Recurring membership fees are billed by Helcim subscriptions against the
customer's DEFAULT card (verify sessions set the new card as default).

Configuration (Railway env):
    HELCIM_API_TOKEN — API access token (Helcim dashboard → API Access).
When unset, callers get HelcimNotConfigured and endpoints return 503.

API reference: https://devdocs.helcim.com (v2, July 2026).
"""
import hashlib
import json
import os
import re
import secrets
import string
from typing import Optional

import httpx

API_BASE = "https://api.helcim.com/v2"

# GiaDienRe membership pricing (matches lib/plans.ts on the website)
PLAN_PRICES = {
    ("managed", "monthly"): 12.99,
    ("managed", "annual"): 140.29,
    ("managed-plus", "monthly"): 19.99,
    ("managed-plus", "annual"): 215.89,
}
PLAN_LABELS = {
    ("managed", "monthly"): "GiaDienRe Managed — Monthly",
    ("managed", "annual"): "GiaDienRe Managed — Annual",
    ("managed-plus", "monthly"): "GiaDienRe Managed Plus — Monthly",
    ("managed-plus", "annual"): "GiaDienRe Managed Plus — Annual",
}

_plan_id_cache: dict = {}


class HelcimNotConfigured(Exception):
    pass


class HelcimError(Exception):
    pass


def is_configured() -> bool:
    return bool(os.environ.get("HELCIM_API_TOKEN", "").strip())


def _headers() -> dict:
    token = os.environ.get("HELCIM_API_TOKEN", "").strip()
    if not token:
        raise HelcimNotConfigured()
    return {"api-token": token, "accept": "application/json",
            "content-type": "application/json"}


def _post(path: str, body: dict, extra_headers: Optional[dict] = None) -> dict:
    headers = _headers()
    if extra_headers:
        headers.update(extra_headers)
    r = httpx.post(f"{API_BASE}{path}", headers=headers, json=body, timeout=25)
    if r.status_code not in (200, 201):
        raise HelcimError(f"{path} failed ({r.status_code}): {r.text[:300]}")
    return r.json()


def _get(path: str, params: Optional[dict] = None):
    r = httpx.get(f"{API_BASE}{path}", headers=_headers(), params=params or {}, timeout=25)
    if r.status_code != 200:
        raise HelcimError(f"{path} failed ({r.status_code}): {r.text[:300]}")
    return r.json()


# ── HelcimPay.js sessions ─────────────────────────────────────────────────────

def initialize_purchase(amount: float, contact_name: str,
                        email: Optional[str] = None, phone: Optional[str] = None,
                        street: Optional[str] = None, city: Optional[str] = None,
                        postal_code: Optional[str] = None,
                        customer_code: Optional[str] = None,
                        invoice_number: Optional[str] = None) -> dict:
    """HelcimPay purchase session (first membership payment).
    Creates the Helcim customer inline unless customer_code is given.
    Returns {"checkoutToken", "secretToken"} — valid 60 minutes."""
    body: dict = {
        "paymentType": "purchase",
        "amount": round(float(amount), 2),
        "currency": "USD",
        "paymentMethod": "cc",
        "confirmationScreen": True,
    }
    if invoice_number:
        body["invoiceNumber"] = invoice_number[:32]
    if customer_code:
        body["customerCode"] = customer_code
    else:
        billing = {"name": contact_name}
        if street:
            billing["street1"] = street[:100]
        if city:
            billing["city"] = city[:60]
        if postal_code:
            billing["postalCode"] = postal_code[:10]
        if email:
            billing["email"] = email[:200]
        billing["country"] = "USA"
        billing["province"] = "TX"
        req = {"contactName": contact_name[:100], "billingAddress": billing}
        if phone:
            req["cellPhone"] = phone[:30]
        body["customerRequest"] = req
    return _post("/helcim-pay/initialize", body)


def initialize_verify(customer_code: Optional[str] = None,
                      contact_name: Optional[str] = None,
                      email: Optional[str] = None,
                      phone: Optional[str] = None) -> dict:
    """HelcimPay verify session ($0 card save) for add/update card.
    New card becomes the customer's default → recurring bills it."""
    body: dict = {
        "paymentType": "verify",
        "amount": 0,
        "currency": "USD",
        "paymentMethod": "cc",
        "hideExistingPaymentDetails": 1,
        "setAsDefaultPaymentMethod": 1,
    }
    if customer_code:
        body["customerCode"] = customer_code
    elif contact_name:
        req: dict = {"contactName": contact_name[:100],
                     "billingAddress": {"name": contact_name[:100], "country": "USA",
                                        **({"email": email[:200]} if email else {})}}
        if phone:
            req["cellPhone"] = phone[:30]
        body["customerRequest"] = req
    return _post("/helcim-pay/initialize", body)


# ── Response validation ───────────────────────────────────────────────────────

def validate_event(event_message, secret_token: str) -> bool:
    """Verify a HelcimPay SUCCESS eventMessage server-side.

    eventMessage = {"data": <transaction dict or one more nested level>,
                    "hash": sha256(minifiedJson(data) + secretToken)}.
    Python dicts preserve key order from JSON parsing, so a compact re-dump
    reproduces Helcim's minified JSON (per their official sample)."""
    try:
        if isinstance(event_message, str):
            event_message = json.loads(event_message)
        if not isinstance(event_message, dict):
            return False
        given_hash = str(event_message.get("hash") or "")
        data = event_message.get("data")
        if not given_hash or data is None:
            return False
        candidates = []
        if isinstance(data, str):
            candidates.append(data)
            try:
                data = json.loads(data)
            except ValueError:
                pass
        if isinstance(data, dict):
            # some flows nest the transaction one level deeper
            for d in (data, data.get("data") if isinstance(data.get("data"), dict) else None):
                if d is None:
                    continue
                candidates.append(json.dumps(d, separators=(",", ":")))                    # \uXXXX-escaped (PHP style)
                candidates.append(json.dumps(d, separators=(",", ":"), ensure_ascii=False))
        for raw in candidates:
            if hashlib.sha256((raw + secret_token).encode()).hexdigest() == given_hash:
                return True
        return False
    except Exception:
        return False


def extract_card_info(event_message) -> dict:
    """Pull identifiers out of a validated SUCCESS eventMessage."""
    if isinstance(event_message, str):
        try:
            event_message = json.loads(event_message)
        except ValueError:
            event_message = {}
    data = event_message.get("data") if isinstance(event_message, dict) else {}
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except ValueError:
            data = {}
    if isinstance(data, dict) and isinstance(data.get("data"), dict):
        data = data["data"]
    if not isinstance(data, dict):
        data = {}

    card_number = str(data.get("cardNumber") or "")
    status = str(data.get("status") or "").upper()
    return {
        "transaction_id": data.get("transactionId"),
        "type": data.get("type"),
        "customer_code": data.get("customerCode") or None,
        "card_token": data.get("cardToken") or None,
        "card_last4": card_number[-4:] if len(card_number) >= 4 else None,
        "card_brand": data.get("cardType") or None,
        "amount": data.get("amount"),
        "approved": status == "APPROVED",
    }


# ── Recurring (Helcim subscriptions bill the default card) ────────────────────

def _idempotency_key() -> str:
    return "".join(secrets.choice(string.ascii_letters + string.digits) for _ in range(25))


def ensure_payment_plan(plan_id: str, billing_cycle: str) -> Optional[int]:
    """Find-or-create the Helcim payment plan for a membership tier."""
    key = (plan_id, billing_cycle)
    if key not in PLAN_PRICES:
        return None
    if key in _plan_id_cache:
        return _plan_id_cache[key]

    name = PLAN_LABELS[key]
    try:
        existing = _get("/payment-plans")
        plans = existing.get("data", existing) if isinstance(existing, dict) else existing
        for p in plans or []:
            if str(p.get("name") or "").strip() == name:
                _plan_id_cache[key] = int(p["id"])
                return _plan_id_cache[key]
    except HelcimError:
        pass

    created = _post("/payment-plans", {"paymentPlans": [{
        "name": name,
        "type": "subscription",           # bills on each subscriber's anniversary
        "currency": "USD",
        "recurringAmount": PLAN_PRICES[key],
        "billingPeriod": "yearly" if billing_cycle == "annual" else "monthly",
        "billingPeriodIncrements": 1,
        "dateBilling": "Sign-up",
        "termType": "forever",
        "paymentMethod": "card",
        "setupAmount": 0,
    }]})
    rows = created.get("data", []) if isinstance(created, dict) else []
    if rows and rows[0].get("id"):
        _plan_id_cache[key] = int(rows[0]["id"])
        return _plan_id_cache[key]
    raise HelcimError(f"payment plan create returned no id: {str(created)[:200]}")


def create_subscription(customer_code: str, plan_id: str, billing_cycle: str,
                        date_activated: str) -> Optional[str]:
    """Create the auto-renewing Helcim subscription (charges default card on
    each anniversary of date_activated). Pass the NEXT billing date when the
    first payment was already collected via HelcimPay purchase."""
    payment_plan_id = ensure_payment_plan(plan_id, billing_cycle)
    if not payment_plan_id:
        return None
    res = _post("/subscriptions", {"subscriptions": [{
        "paymentPlanId": payment_plan_id,
        "customerCode": customer_code,
        "dateActivated": date_activated[:10],
        "recurringAmount": PLAN_PRICES[(plan_id, billing_cycle)],
        "paymentMethod": "card",
    }]}, extra_headers={"idempotency-key": _idempotency_key()})
    rows = res.get("data", []) if isinstance(res, dict) else []
    return str(rows[0]["id"]) if rows and rows[0].get("id") else None


def cancel_subscription(subscription_id: str) -> bool:
    try:
        _post("/subscriptions", {"subscriptions": [
            {"id": int(subscription_id), "status": "cancelled"}]},
            extra_headers={"idempotency-key": _idempotency_key()})
        return True
    except (HelcimError, ValueError):
        return False


def card_expiry_for(customer_code: str, card_token: str) -> Optional[str]:
    """Best-effort MM/YY display expiry for a saved card."""
    try:
        found = _get("/customers", {"search": customer_code})
        rows = found.get("data", found) if isinstance(found, dict) else found
        cust = next((c for c in rows or [] if c.get("customerCode") == customer_code), None)
        if not cust:
            return None
        cards = _get(f"/customers/{cust['id']}/cards", {"cardToken": card_token})
        cards = cards.get("data", cards) if isinstance(cards, dict) else cards
        exp = str((cards or [{}])[0].get("cardExpiry") or "")
        exp = re.sub(r"\D", "", exp)
        return f"{exp[:2]}/{exp[2:4]}" if len(exp) >= 4 else None
    except Exception:
        return None
