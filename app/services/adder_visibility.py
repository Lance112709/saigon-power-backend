"""Who may see a deal's adder (the broker margin, $/kWh).

Rule (Lance, 2026-11-03): admins see every adder; a salesperson sees the
adder only on deals where they are the sales agent; nobody else sees it.
Deal rows leaving the API are stripped of `adder` when the caller may not see
it, and `adder` is dropped from writes by such callers, so the forms can hide
the field without the value leaking through the network tab.
"""
from typing import Iterable


def _own_names(user) -> set:
    return {s for s in (
        (getattr(user, "sales_agent_name", None) or "").strip().lower(),
        (getattr(user, "name", None) or "").strip().lower(),
    ) if s}


def can_see_adder(user, sales_agent) -> bool:
    if getattr(user, "is_admin", False):
        return True
    return str(sales_agent or "").strip().lower() in _own_names(user)


def redact_adder(user, deals: Iterable[dict]) -> None:
    """Remove `adder` in place from every deal the caller may not see it on."""
    if getattr(user, "is_admin", False):
        return
    own = _own_names(user)
    for d in deals or []:
        if isinstance(d, dict) and str(d.get("sales_agent") or "").strip().lower() not in own:
            d.pop("adder", None)


def redact_adder_one(user, deal):
    if isinstance(deal, dict):
        redact_adder(user, [deal])
    return deal
