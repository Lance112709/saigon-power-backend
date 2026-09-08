from fastapi import APIRouter, Query, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional
import logging
from datetime import datetime, timezone, timedelta, date
from app.db.client import get_client
from app.auth.deps import get_current_user, UserContext

router = APIRouter()
log = logging.getLogger(__name__)

def _days_until(date_str: Optional[str]) -> Optional[int]:
    if not date_str:
        return None
    try:
        d = date.fromisoformat(date_str[:10])
        return (d - datetime.now(timezone.utc).date()).days
    except Exception:
        return None


def _is_month_to_month(deal: dict) -> bool:
    """Month-to-month plans have no renewal date to chase — keep them off the
    call list. The marker varies by import: rate_type 'Month-Month',
    contract_term 'Month to Month', or a plan name containing it."""
    for field in ("rate_type", "contract_term", "plan_name", "product_type"):
        v = str(deal.get(field) or "").lower().replace("-", " ").replace("_", " ")
        if "month to month" in v or "month month" in v:
            return True
    return False


def _fetch_all(db, table: str, cols: str, filters: list) -> list:
    out, off = [], 0
    while True:
        q = db.table(table).select(cols)
        for fn, args in filters:
            q = getattr(q, fn)(*args)
        page = q.range(off, off + 999).execute().data or []
        out.extend(page)
        if len(page) < 1000:
            break
        off += 1000
    return out


def _cycle_key(entity_key: str, end_date: Optional[str]) -> tuple[str, str]:
    """A resolution is tied to the customer and the contract end date that put
    them on the list, so a renewed customer comes back for the next cycle."""
    return (entity_key, (end_date or "")[:10])


def _load_resolved(db) -> set[tuple[str, str]]:
    """Set of (entity_key, end_date) cycles already marked RESOLVED. Best-effort:
    if migration 015 has not been applied yet the list still renders."""
    try:
        rows = _fetch_all(db, "call_list_resolutions", "entity_key, end_date", filters=[])
    except Exception as e:  # pragma: no cover - table missing / transient
        log.warning("call_list_resolutions unavailable: %s", e)
        return set()
    return {_cycle_key(r.get("entity_key") or "", r.get("end_date")) for r in rows}


def _drop_resolved(results: list, resolved: set[tuple[str, str]]) -> list:
    return [r for r in results
            if _cycle_key(r.get("entity_key") or "", r.get("end_date")) not in resolved]


def _score_customer(lead: dict, deals: list) -> tuple[int, list[str], str]:
    score = 0
    reasons = []
    action = "Check in with customer"

    active_deals = [d for d in deals if d.get("status") == "Active"]
    if not active_deals:
        return 0, [], action

    for deal in active_deals:
        days = _days_until(deal.get("end_date"))

        if days is not None:
            if days < 0:
                score += 100
                reasons.append(f"Contract EXPIRED {-days} day{'s' if days != -1 else ''} ago — "
                               f"customer is on holdover pricing")
                action = "Call now — contract already expired"
            elif days <= 7:
                score += 100
                reasons.append(f"Contract expires in {days} day{'s' if days != 1 else ''} — URGENT")
                action = "Renew NOW — contract expiring"
            elif days <= 30:
                score += 80
                reasons.append(f"Contract expires in {days} days")
                action = "Call for renewal — URGENT"
            elif days <= 60:
                score += 50
                reasons.append(f"Contract expires in {days} days")
                action = "Call for renewal"
            elif days <= 90:
                score += 25
                reasons.append(f"Renewal window opens soon ({days} days)")
                action = "Start renewal conversation"

        # Boost commercial accounts
        if str(deal.get("product_type") or "").lower() == "commercial":
            score += 15

    return min(score, 100), reasons, action


def _build_entries(db, user: UserContext) -> list:
    """Every customer that scores > 0, scoped to the caller. Resolved cycles are
    NOT removed here so the Resolved tab can reuse the same rows."""
    results = []

    # Fetch all converted customers via lead_customers
    customers = db.table("lead_customers").select(
        "id, lead_id, leads(first_name, last_name, phone, sgp_customer_id, sales_agent)"
    ).range(0, 499).execute().data or []

    if not customers:
        return []

    lead_ids = [c["lead_id"] for c in customers if c.get("lead_id")]

    # Batch fetch all active deals; month-to-month plans are excluded — there
    # is no contract end date to call about, and customers whose only plan is
    # month-to-month drop off the list entirely (they score 0 below).
    all_deals = db.table("lead_deals").select(
        "id, lead_id, status, end_date, supplier, plan_name, product_type, est_kwh, adder, "
        "rate_type, contract_term"
    ).in_("lead_id", lead_ids).eq("status", "Active").execute().data or []
    all_deals = [d for d in all_deals if not _is_month_to_month(d)]

    deals_by_lead: dict = {}
    for d in all_deals:
        deals_by_lead.setdefault(d["lead_id"], []).append(d)

    # Sales agents only see their own customers — if no agent name mapped, return nothing
    if user.is_sales_agent and not user.sales_agent_name:
        return []
    agent_name = user.sales_agent_name if user.is_sales_agent else None

    for c in customers:
        lead_id = c.get("lead_id")
        lead = c.get("leads") or {}
        deals = deals_by_lead.get(lead_id, [])

        # Sales agents only see their own customers
        if agent_name and (lead.get("sales_agent") or "").lower() != agent_name.lower():
            continue

        score, reasons, action = _score_customer(lead, deals)
        if score == 0:
            continue

        # Pick the most urgent deal
        active = [d for d in deals if d.get("status") == "Active"]
        active.sort(key=lambda d: (_days_until(d.get("end_date")) or 9999))
        top_deal = active[0] if active else None

        results.append({
            "name":            f"{lead.get('first_name', '')} {lead.get('last_name', '')}".strip(),
            "type":            "Customer",
            "phone":           lead.get("phone") or "—",
            "sgp_customer_id": lead.get("sgp_customer_id"),
            "sales_agent":     lead.get("sales_agent"),
            "supplier":        top_deal.get("supplier") if top_deal else None,
            "plan_name":       top_deal.get("plan_name") if top_deal else None,
            "end_date":        top_deal.get("end_date") if top_deal else None,
            "days_left":       _days_until(top_deal.get("end_date")) if top_deal else None,
            "priority_score":  score,
            "reason":          " · ".join(reasons),
            "action":          action,
            "lead_id":         lead_id,
            "entity_key":      f"lead:{lead_id}",
            "entity_url":      f"/crm/leads/{lead_id}",
        })

    # ── CRM book (crm_deals — most of the business, ~5k active deals) ──
    crm_deals = _fetch_all(
        db, "crm_deals",
        "id, customer_id, deal_status, provider, deal_name, product_type, contract_term, "
        "meter_type, contract_end_date, sales_agent, business_name, "
        "crm_customers(full_name, phone)",
        filters=[("eq", ("deal_status", "ACTIVE"))])

    by_customer: dict = {}
    for d in crm_deals:
        if _is_month_to_month(d):
            continue
        if agent_name and (d.get("sales_agent") or "").lower() != agent_name.lower():
            continue
        # normalize to the shape _score_customer expects
        norm = {
            "status": "Active",
            "end_date": d.get("contract_end_date"),
            # meter_type carries Commercial/Residential for the scoring boost
            "product_type": d.get("meter_type"),
            "supplier": d.get("provider"),
            "plan_name": d.get("deal_name") or d.get("product_type"),
            "sales_agent": d.get("sales_agent"),
            "customer": d.get("crm_customers") or {},
            "business_name": d.get("business_name"),
            "customer_id": d.get("customer_id"),
        }
        key = d.get("customer_id") or f"deal:{d['id']}"
        by_customer.setdefault(key, []).append(norm)

    for key, deals in by_customer.items():
        score, reasons, action = _score_customer({}, deals)
        if score == 0:
            continue
        deals.sort(key=lambda d: (_days_until(d.get("end_date")) or 9999))
        top = deals[0]
        cust = top.get("customer") or {}
        name = (cust.get("full_name") or top.get("business_name")
                or top.get("plan_name") or "Unknown")
        customer_id = top.get("customer_id")
        results.append({
            "name":            name,
            "type":            "Customer",
            "phone":           cust.get("phone") or "—",
            "sgp_customer_id": None,
            "sales_agent":     top.get("sales_agent"),
            "supplier":        top.get("supplier"),
            "plan_name":       top.get("plan_name"),
            "end_date":        top.get("end_date"),
            "days_left":       _days_until(top.get("end_date")),
            "priority_score":  score,
            "reason":          " · ".join(reasons),
            "action":          action,
            "lead_id":         None,
            "entity_key":      f"crm:{customer_id}" if customer_id else key.replace("deal:", "crmdeal:"),
            "entity_url":      f"/crm/customers/{customer_id}" if customer_id else "/crm/deals",
        })

    return results


@router.get("")
def get_call_list(
    priority_filter: Optional[str] = Query(None),
    limit:           int           = Query(50),
    user: UserContext = Depends(get_current_user),
):
    db = get_client()
    results = _build_entries(db, user)

    # Drop entries the team already marked RESOLVED for this contract cycle
    results = _drop_resolved(results, _load_resolved(db))

    results.sort(key=lambda x: x["priority_score"], reverse=True)

    if priority_filter == "high":
        results = [r for r in results if r["priority_score"] >= 75]

    return results[:limit]


def _lookup_names(db, entity_keys: list[str]) -> dict[str, dict]:
    """Name/phone/agent for resolved entities that are no longer on the live
    list (renewed, deactivated). Batched per entity type."""
    out: dict[str, dict] = {}
    lead_ids = [k.split(":", 1)[1] for k in entity_keys if k.startswith("lead:")]
    cust_ids = [k.split(":", 1)[1] for k in entity_keys if k.startswith("crm:")]
    deal_ids = [k.split(":", 1)[1] for k in entity_keys if k.startswith("crmdeal:")]
    try:
        for i in range(0, len(lead_ids), 200):
            for r in (db.table("leads").select("id, first_name, last_name, phone, sales_agent, sgp_customer_id")
                      .in_("id", lead_ids[i:i + 200]).execute().data or []):
                out[f"lead:{r['id']}"] = {
                    "name": f"{r.get('first_name') or ''} {r.get('last_name') or ''}".strip() or "Unknown",
                    "phone": r.get("phone") or "—", "sales_agent": r.get("sales_agent"),
                    "sgp_customer_id": r.get("sgp_customer_id"),
                    "entity_url": f"/crm/leads/{r['id']}",
                }
        for i in range(0, len(cust_ids), 200):
            for r in (db.table("crm_customers").select("id, full_name, phone")
                      .in_("id", cust_ids[i:i + 200]).execute().data or []):
                out[f"crm:{r['id']}"] = {
                    "name": r.get("full_name") or "Unknown", "phone": r.get("phone") or "—",
                    "sales_agent": None, "sgp_customer_id": None,
                    "entity_url": f"/crm/customers/{r['id']}",
                }
        for i in range(0, len(deal_ids), 200):
            for r in (db.table("crm_deals").select("id, business_name, deal_name, sales_agent")
                      .in_("id", deal_ids[i:i + 200]).execute().data or []):
                out[f"crmdeal:{r['id']}"] = {
                    "name": r.get("business_name") or r.get("deal_name") or "Unknown", "phone": "—",
                    "sales_agent": r.get("sales_agent"), "sgp_customer_id": None,
                    "entity_url": "/crm/deals",
                }
    except Exception as e:  # pragma: no cover
        log.warning("resolved-list name lookup failed: %s", e)
    return out


@router.get("/resolved")
def get_resolved_list(limit: int = Query(200), user: UserContext = Depends(get_current_user)):
    """Customers marked RESOLVED, newest first. Rows still on the live list carry
    their full call-list details; renewed/deactivated ones fall back to a name lookup."""
    db = get_client()
    try:
        rows = (db.table("call_list_resolutions")
                .select("id, entity_key, end_date, resolved_by_name, resolved_at, note")
                .order("resolved_at", desc=True).range(0, max(limit, 1) - 1).execute().data or [])
    except Exception as e:
        log.warning("call_list_resolutions unavailable: %s", e)
        return []
    if not rows:
        return []

    live = {_cycle_key(e["entity_key"], e.get("end_date")): e for e in _build_entries(db, user)}
    missing = [r["entity_key"] for r in rows
               if _cycle_key(r["entity_key"], r.get("end_date")) not in live]
    names = _lookup_names(db, list(dict.fromkeys(missing))) if missing else {}

    agent_name = user.sales_agent_name if user.is_sales_agent else None
    out = []
    for r in rows:
        key = _cycle_key(r["entity_key"], r.get("end_date"))
        base = live.get(key)
        if base is None:
            info = names.get(r["entity_key"])
            if info is None:
                continue  # entity no longer exists
            if agent_name and (info.get("sales_agent") or "").lower() != agent_name.lower():
                continue  # sales agents only see their own customers
            base = {**info, "supplier": None, "plan_name": None,
                    "days_left": _days_until(r.get("end_date")), "priority_score": 0,
                    "reason": "No longer on the call list (renewed or deactivated)", "action": "",
                    "lead_id": r["entity_key"].split(":", 1)[1] if r["entity_key"].startswith("lead:") else None,
                    "still_due": False}
        else:
            base = {**base, "still_due": True}
        out.append({**base,
                    "entity_key":       r["entity_key"],
                    "end_date":         r.get("end_date"),
                    "resolution_id":    r["id"],
                    "resolved_by_name": r.get("resolved_by_name"),
                    "resolved_at":      r.get("resolved_at"),
                    "note":             r.get("note")})
    return out


# ── RESOLVED button ──────────────────────────────────────────────────────────

class ResolveBody(BaseModel):
    entity_key: str
    end_date: Optional[str] = None
    note: Optional[str] = None


def _entity_agent(db, entity_key: str) -> Optional[str]:
    """Sales agent that owns the entity behind a call-list row (for the
    sales-agent scope check). Returns None when it cannot be determined."""
    kind, _, ident = entity_key.partition(":")
    try:
        if kind == "lead":
            row = db.table("leads").select("sales_agent").eq("id", ident).limit(1).execute().data
        elif kind == "crm":
            row = (db.table("crm_deals").select("sales_agent").eq("customer_id", ident)
                   .eq("deal_status", "ACTIVE").limit(1).execute().data)
        elif kind == "crmdeal":
            row = db.table("crm_deals").select("sales_agent").eq("id", ident).limit(1).execute().data
        else:
            return None
    except Exception:
        return None
    return (row[0].get("sales_agent") if row else None)


def _check_scope(db, user: UserContext, entity_key: str) -> None:
    if not user.is_sales_agent:
        return
    owner = _entity_agent(db, entity_key)
    if not user.sales_agent_name or (owner or "").lower() != user.sales_agent_name.lower():
        raise HTTPException(status_code=403, detail="Not your customer")


@router.post("/resolve")
def resolve_entry(body: ResolveBody, user: UserContext = Depends(get_current_user)):
    """Mark a Who-To-Call row as handled. It stays hidden until the customer's
    contract end date changes (i.e. a renewal starts a new cycle)."""
    entity_key = (body.entity_key or "").strip()
    if not entity_key or ":" not in entity_key:
        raise HTTPException(status_code=400, detail="entity_key required")
    db = get_client()
    _check_scope(db, user, entity_key)
    end_date = (body.end_date or "")[:10] or None
    row = {
        "entity_key":       entity_key,
        "end_date":         end_date,
        "resolved_by":      user.user_id,
        "resolved_by_name": user.name,
        "note":             (body.note or "").strip() or None,
    }
    try:
        # Idempotent: a second click on the same cycle just returns the existing mark
        existing = db.table("call_list_resolutions").select("id") \
            .eq("entity_key", entity_key)
        existing = (existing.eq("end_date", end_date) if end_date else existing.is_("end_date", "null"))
        found = existing.limit(1).execute().data or []
        if found:
            return {"ok": True, "id": found[0]["id"], "already": True}
        res = db.table("call_list_resolutions").insert(row).execute().data or []
    except Exception as e:
        log.error("resolve call-list entry failed: %s", e)
        raise HTTPException(status_code=500,
                            detail="Could not save — has migration 015_call_list_resolutions been applied?")
    return {"ok": True, "id": res[0]["id"] if res else None}


@router.delete("/resolve/{resolution_id}")
def unresolve_entry(resolution_id: str, user: UserContext = Depends(get_current_user)):
    """Undo a RESOLVED click — the row returns to the list."""
    db = get_client()
    row = db.table("call_list_resolutions").select("id, entity_key") \
        .eq("id", resolution_id).limit(1).execute().data or []
    if not row:
        return {"ok": True, "deleted": 0}
    _check_scope(db, user, row[0]["entity_key"])
    db.table("call_list_resolutions").delete().eq("id", resolution_id).execute()
    return {"ok": True, "deleted": 1}
