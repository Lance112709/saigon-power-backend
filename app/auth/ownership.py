"""Record-level ownership checks for sales-agent scoping.

Reads were already scoped to the owning agent in crm.py / leads.py, but writes
and note/attachment sub-resources were not — any authenticated sales_agent could
mutate or read another agent's records (IDOR). These helpers centralize the
rule so every write path enforces it identically:

  - admin / manager / csr  → full access (no per-record restriction)
  - sales_agent            → only records whose owning agent name matches theirs

A sales_agent with no `sales_agent_name` on their token owns nothing and is
denied, which is the safe default.
"""
from typing import Optional

from fastapi import HTTPException

from app.auth.deps import UserContext


def _agent_name(user: UserContext) -> Optional[str]:
    if not user.is_sales_agent:
        return None  # not restricted
    return (user.sales_agent_name or "").lower() or "\x00"  # unknown agent → owns nothing


def assert_lead_access(db, user: UserContext, lead_id: str) -> dict:
    """Return the lead row, or raise 404/403. Enforces agent ownership."""
    res = db.table("leads").select("*").eq("id", lead_id).limit(1).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Lead not found")
    lead = res.data[0]
    name = _agent_name(user)
    if name is not None and (lead.get("sales_agent") or "").lower() != name:
        raise HTTPException(status_code=403, detail="Access denied")
    return lead


def assert_customer_access(db, user: UserContext, customer_id: str) -> None:
    """Raise 404 if the customer doesn't exist, 403 if a sales_agent has no
    owned deal for them."""
    exists = db.table("crm_customers").select("id").eq("id", customer_id).limit(1).execute()
    if not exists.data:
        raise HTTPException(status_code=404, detail="Customer not found")
    name = _agent_name(user)
    if name is None:
        return
    deals = db.table("crm_deals").select("sales_agent").eq("customer_id", customer_id).execute().data or []
    if not any((d.get("sales_agent") or "").lower() == name for d in deals):
        raise HTTPException(status_code=403, detail="Access denied")


def assert_crm_deal_access(db, user: UserContext, deal_id: str) -> dict:
    res = db.table("crm_deals").select("*").eq("id", deal_id).limit(1).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Deal not found")
    deal = res.data[0]
    name = _agent_name(user)
    if name is not None and (deal.get("sales_agent") or "").lower() != name:
        raise HTTPException(status_code=403, detail="Access denied")
    return deal


def assert_lead_deal_access(db, user: UserContext, lead_id: str) -> None:
    """A lead's deals inherit the lead's ownership."""
    assert_lead_access(db, user, lead_id)
