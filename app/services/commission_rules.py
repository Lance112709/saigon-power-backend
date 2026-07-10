"""Versioned per-provider commission rules.

A rule describes how a provider is supposed to pay us for a statement month:

  rate_per_kwh  config: {rate, rate_source: 'fixed'|'deal_adder'}
  flat_fee      config: {flat_amount}
  tiered        config: {tiers: [{min_kwh, max_kwh, rate}], rate_source ignored}
  hybrid        config: {flat_amount, rate, rate_source}

Rules are append-only: editing a rule closes the current version
(effective_to + superseded_by) and inserts version n+1, so the audit engine
can always recompute any historical month with the rule that governed it.
With no rules defined the reconciliation engine behaves exactly as before
(expected rate = the deal's contracted adder).
"""
from datetime import datetime, timezone
from typing import Optional

from app.services.audit import audit

RULE_TYPES = ("rate_per_kwh", "flat_fee", "tiered", "hybrid")


def _month_start(label: str) -> str:
    return f"{label[:7]}-01"


def get_rule_for_month(db, supplier_id: str, label: str) -> Optional[dict]:
    """The rule in force for a supplier during a statement month (or None)."""
    month = _month_start(label)
    rows = db.table("commission_rules").select("*") \
        .eq("supplier_id", supplier_id) \
        .lte("effective_from", month) \
        .order("effective_from", desc=True).order("version", desc=True) \
        .limit(10).execute().data or []
    for r in rows:
        if r.get("effective_to") and str(r["effective_to"])[:10] <= month:
            continue
        return r
    return None


def _tier_rate(tiers: list, kwh: float) -> Optional[float]:
    for t in tiers or []:
        lo = float(t.get("min_kwh") or 0)
        hi = t.get("max_kwh")
        if kwh >= lo and (hi is None or kwh < float(hi)):
            return float(t["rate"]) if t.get("rate") is not None else None
    return None


def rule_rate(rule: dict, kwh: Optional[float], deal_adder: Optional[float]) -> Optional[float]:
    """The $/kWh rate this rule expects, or None when the rule has no rate
    (flat_fee) or the inputs needed to pick one are missing."""
    cfg = rule.get("config") or {}
    rtype = rule.get("rule_type")
    if rtype in ("rate_per_kwh", "hybrid"):
        if cfg.get("rate_source") == "deal_adder":
            return float(deal_adder) if deal_adder is not None else None
        return float(cfg["rate"]) if cfg.get("rate") is not None else None
    if rtype == "tiered":
        if kwh is None:
            return None
        return _tier_rate(cfg.get("tiers"), float(kwh))
    return None  # flat_fee


def evaluate_rule(rule: dict, kwh: Optional[float],
                  deal_adder: Optional[float]) -> Optional[tuple]:
    """(expected_amount, expected_rate) for one account-month, or None when the
    rule cannot be evaluated with the inputs available (caller falls back to
    the plain adder x kwh math)."""
    cfg = rule.get("config") or {}
    rtype = rule.get("rule_type")
    if rtype not in RULE_TYPES:
        return None
    if rtype == "flat_fee":
        if cfg.get("flat_amount") is None:
            return None
        return round(float(cfg["flat_amount"]), 4), None
    rate = rule_rate(rule, kwh, deal_adder)
    if rate is None or kwh is None:
        return None
    amount = rate * float(kwh)
    if rtype == "hybrid" and cfg.get("flat_amount") is not None:
        amount += float(cfg["flat_amount"])
    return round(amount, 4), rate


def create_rule_version(db, supplier_id: str, payload: dict, actor: str) -> dict:
    """Insert a new rule version; close the current one (never deleted)."""
    effective_from = str(payload["effective_from"])[:10]
    current = db.table("commission_rules").select("*") \
        .eq("supplier_id", supplier_id).is_("effective_to", "null") \
        .order("version", desc=True).limit(1).execute().data or []
    version = (current[0]["version"] + 1) if current else 1

    new = db.table("commission_rules").insert({
        "supplier_id": supplier_id,
        "name": payload.get("name") or f"Rule v{version}",
        "rule_type": payload["rule_type"],
        "config": payload.get("config") or {},
        "effective_from": effective_from,
        "version": version,
        "notes": payload.get("notes"),
        "created_by": actor,
    }).execute().data[0]

    if current:
        db.table("commission_rules").update({
            "effective_to": effective_from,
            "superseded_by": new["id"],
        }).eq("id", current[0]["id"]).execute()

    audit(db, "commission_rules", new["id"], "rule_version_created",
          {"previous": current[0]["id"] if current else None},
          {"rule_type": new["rule_type"], "config": new["config"],
           "effective_from": effective_from, "version": version},
          reason=payload.get("notes") or "", actor=actor)
    return new


def rule_history(db, supplier_id: str) -> list:
    return db.table("commission_rules").select("*") \
        .eq("supplier_id", supplier_id) \
        .order("effective_from", desc=True).order("version", desc=True) \
        .limit(200).execute().data or []
