"""Who may see a deal's adder (the broker margin, $/kWh).

Rule (Lance, 2026-11-03): admins see every adder; a salesperson sees the
adder only on deals where they are the sales agent; nobody else sees it.
Deal rows leaving the API are stripped of `adder` when the caller may not see
it, and `adder` is dropped from writes by such callers, so the forms can hide
the field without the value leaking through the network tab.
"""
from typing import Iterable


def _own_names(user) -> set:
    """Only a login LINKED to a sales agent record (users.sales_agent_name)
    counts as that salesperson. A CSR whose display name happens to be on a
    deal as sales_agent is not a salesperson and sees nothing."""
    s = (getattr(user, "sales_agent_name", None) or "").strip().lower()
    return {s} if s else set()


def can_see_adder(user, sales_agent) -> bool:
    if getattr(user, "is_admin", False):
        return True
    return str(sales_agent or "").strip().lower() in _own_names(user)


ENTER_ADDER_PERMISSION = "enter_adder"


def role_has_permission(db, role: str, permission: str) -> bool:
    """role_permissions.granted for (role, permission); False when unset."""
    try:
        res = db.table("role_permissions").select("granted").eq("role", role).eq("permission", permission).limit(1).execute()
    except Exception:
        return False
    return bool(res.data and res.data[0].get("granted"))


def can_enter_adder(db, user, sales_agent) -> bool:
    """May this caller TYPE an adder on a deal being added or renewed? Anyone
    who may see it, plus roles granted the `enter_adder` permission (CSRs and
    managers entering Commercial deals for an agent). Seeing it afterwards is
    still governed by can_see_adder."""
    if can_see_adder(user, sales_agent):
        return True
    return role_has_permission(db, getattr(user, "role", ""), ENTER_ADDER_PERMISSION)


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
