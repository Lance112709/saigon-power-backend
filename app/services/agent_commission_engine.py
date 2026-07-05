"""Sales-agent commission engine — pays agents from provider-paid dollars.

Every agent has a custom plan in sales_agents.commission_rules. A plan is a
list of components that can be freely combined:

  {"components": [
      {"type": "flat_per_deal", "amount": 20, "supplier": null},   # one-time, on the deal's FIRST provider payment
      {"type": "per_kwh", "rate": 0.001, "supplier": "Budget"},    # monthly, on ACTUAL kWh the provider paid on
      {"type": "percent_of_commission", "percent": 30},            # monthly, % of gross commission RECEIVED
      {"type": "flat_monthly", "amount": 250}                      # fixed monthly (only in months with paid deals)
   ],
   "exclude_plan_types": ["Month-Month"]}

Legacy plans ({default_type, default_rate, overrides, exclude_plan_types})
are translated automatically, so nothing already configured breaks.

Nothing is owed until the provider pays: the engine reads actual_commissions
rows for the payout month, so an account the provider skipped generates no
agent commission, and a one-time bounty triggers only in the month the
provider's first payment for that ESIID arrives.

Agents with NO plan configured accrue $0 and are reported in `warnings`
(the old engine silently paid them 100% of gross — that default was unsafe).
"""
import re
from datetime import date

from app.services.reconciliation_v2 import fetch_all

LEGACY_TYPE_MAP = {
    "per_kwh": "per_kwh",
    "percentage": "percent_of_commission",
    "flat_monthly": "flat_monthly",
    "flat_per_deal": "flat_per_deal",
}


def norm_name(s) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip().lower()


def plan_components(rules: dict) -> list:
    """Return the plan as a components list, translating the legacy shape."""
    rules = rules or {}
    if isinstance(rules.get("components"), list):
        return rules["components"]
    comps = []
    for o in rules.get("overrides") or []:
        t = LEGACY_TYPE_MAP.get(o.get("type") or "per_kwh", "per_kwh")
        c = {"type": t, "supplier": o.get("supplier") or None}
        if t == "percent_of_commission":
            c["percent"] = float(o.get("rate") or 0)
        elif t == "per_kwh":
            c["rate"] = float(o.get("rate") or 0)
        else:
            c["amount"] = float(o.get("rate") or 0)
        comps.append(c)
    if rules.get("default_rate") not in (None, "", 0, "0"):
        t = LEGACY_TYPE_MAP.get(rules.get("default_type") or "per_kwh", "per_kwh")
        c = {"type": t, "supplier": None}
        if t == "percent_of_commission":
            c["percent"] = float(rules["default_rate"])
        elif t == "per_kwh":
            c["rate"] = float(rules["default_rate"])
        else:
            c["amount"] = float(rules["default_rate"])
        comps.append(c)
    return comps


def _supplier_matches(component, supplier_name: str, supplier_code: str, deal_supplier: str) -> bool:
    want = norm_name(component.get("supplier"))
    if not want:
        return True
    for cand in (supplier_name, supplier_code, deal_supplier):
        c = norm_name(cand)
        if c and (want in c or c in want):
            return True
    return False


def _excluded(plan_type: str, rules: dict) -> bool:
    pt = norm_name(plan_type)
    return bool(pt) and any(norm_name(x) == pt for x in (rules or {}).get("exclude_plan_types") or [])


def load_agent_plans(db) -> dict:
    agents = fetch_all(db, "sales_agents", "id,name,commission_rules")
    return {norm_name(a["name"]): {"id": a["id"], "name": a["name"],
                                   "rules": a.get("commission_rules") or {},
                                   "components": plan_components(a.get("commission_rules"))}
            for a in agents if a.get("name")}


def load_deal_book(db) -> dict:
    """esiid → deal info across both deal tables (active deal preferred)."""
    book = {}

    def put(esiid, deal):
        es = re.sub(r"\D", "", esiid or "")
        if not es:
            return
        cur = book.get(es)
        if cur is None or (deal["active"] and not cur["active"]):
            book[es] = deal

    for d in fetch_all(db, "lead_deals",
                       "id,status,supplier,esiid,adder,rate_type,plan_name,contract_term,sales_agent,"
                       "provider_status,leads(first_name,last_name)"):
        lead = d.get("leads") or {}
        put(d.get("esiid"), {
            "source": "lead_deals", "id": d["id"], "active": d.get("status") == "Active",
            "provider_status": d.get("provider_status"),
            "agent": (d.get("sales_agent") or "").strip(),
            "supplier": (d.get("supplier") or "").strip(),
            "plan_type": (d.get("rate_type") or d.get("plan_name") or d.get("contract_term") or "").strip(),
            "adder": float(d["adder"]) if d.get("adder") is not None else None,
            "customer": f"{lead.get('first_name', '')} {lead.get('last_name', '')}".strip(),
        })
    for d in fetch_all(db, "crm_deals",
                       "id,deal_status,provider,esiid,adder,product_type,contract_term,sales_agent,business_name,"
                       "provider_status,crm_customers(full_name)"):
        cust = d.get("crm_customers") or {}
        put(d.get("esiid"), {
            "source": "crm_deals", "id": d["id"], "active": d.get("deal_status") == "ACTIVE",
            "provider_status": d.get("provider_status"),
            "agent": (d.get("sales_agent") or "").strip(),
            "supplier": (d.get("provider") or "").strip(),
            "plan_type": (d.get("product_type") or d.get("contract_term") or "").strip(),
            "adder": float(d["adder"]) if d.get("adder") is not None else None,
            "customer": cust.get("full_name") or d.get("business_name") or "",
        })
    return book


def _month_label(year: int, month: int) -> str:
    return f"{year}-{month:02d}"


def _paid_rows(db, label: str) -> list:
    return fetch_all(
        db, "actual_commissions",
        "raw_esiid,raw_amount,raw_kwh,raw_rate,supplier_id,suppliers(name,code)",
        filters=[("eq", ("billing_month", f"{label}-01"))])


def _previously_paid_esiids(db, esiids: list, label: str) -> set:
    """ESIIDs that already had a payment in ANY month before `label`."""
    seen = set()
    esiids = list(esiids)
    for i in range(0, len(esiids), 100):
        rows = db.table("actual_commissions").select("raw_esiid") \
            .lt("billing_month", f"{label}-01") \
            .in_("raw_esiid", esiids[i:i + 100]).limit(1000).execute().data or []
        seen.update(r["raw_esiid"] for r in rows)
    return seen


def calculate_month(db, year: int, month: int, plans: dict = None, book: dict = None) -> dict:
    """Compute every agent's payout for a month from actual provider payments.

    Returns {agents: {display_name: {...}}, unassigned: {...}, warnings: [...]}.
    """
    label = _month_label(year, month)
    plans = plans if plans is not None else load_agent_plans(db)
    book = book if book is not None else load_deal_book(db)

    rows = _paid_rows(db, label)
    if not rows:
        return {"agents": {}, "unassigned": {}, "warnings": [
            f"No provider payments imported for {label} — upload the statements first."], "rows": 0}

    # group rows per esiid
    per_esiid = {}
    for r in rows:
        per_esiid.setdefault(r["raw_esiid"], []).append(r)

    first_payment_esiids = set(per_esiid) - _previously_paid_esiids(db, list(per_esiid), label)

    agents: dict = {}
    unassigned = {"no_deal": {"esiids": 0, "gross": 0.0},
                  "no_agent_on_deal": {"esiids": 0, "gross": 0.0},
                  "agent_not_registered": {}}
    unknown_agent_names = {}

    def agent_bucket(display_name):
        return agents.setdefault(display_name, {
            "total": 0.0, "residual": 0.0, "bonuses": 0.0, "flat_monthly": 0.0,
            "deals_paid": 0, "gross_received": 0.0, "excluded_deals": 0,
            "deals": [],  # per-deal detail for breakdowns
        })

    for esiid, group in per_esiid.items():
        gross = sum(float(r.get("raw_amount") or 0) for r in group)
        kwh = sum(float(r.get("raw_kwh") or 0) for r in group)
        sup_name = (group[0].get("suppliers") or {}).get("name", "")
        sup_code = (group[0].get("suppliers") or {}).get("code", "")

        deal = book.get(esiid)
        if deal is None:
            unassigned["no_deal"]["esiids"] += 1
            unassigned["no_deal"]["gross"] += gross
            continue
        if not deal["agent"]:
            unassigned["no_agent_on_deal"]["esiids"] += 1
            unassigned["no_agent_on_deal"]["gross"] += gross
            continue

        plan = plans.get(norm_name(deal["agent"]))
        if plan is None:
            k = deal["agent"]
            unknown_agent_names[k] = unknown_agent_names.get(k, 0) + 1
            unassigned["agent_not_registered"][k] = round(
                unassigned["agent_not_registered"].get(k, 0) + gross, 2)
            continue

        b = agent_bucket(plan["name"])
        b["deals_paid"] += 1
        b["gross_received"] += gross

        excluded = _excluded(deal["plan_type"], plan["rules"])
        if excluded:
            b["excluded_deals"] += 1

        payout = 0.0
        applied = []
        if not excluded:
            for c in plan["components"]:
                ctype = c.get("type")
                if ctype == "flat_monthly":
                    continue  # handled per agent below
                if not _supplier_matches(c, sup_name, sup_code, deal["supplier"]):
                    continue
                if ctype == "per_kwh":
                    amt = float(c.get("rate") or 0) * kwh
                    if amt:
                        payout += amt
                        b["residual"] += amt
                        applied.append(f"{c.get('rate'):g}/kWh × {kwh:g} = ${amt:.2f}")
                elif ctype == "percent_of_commission":
                    amt = float(c.get("percent") or 0) / 100.0 * gross
                    if amt:
                        payout += amt
                        b["residual"] += amt
                        applied.append(f"{c.get('percent'):g}% of ${gross:.2f} = ${amt:.2f}")
                elif ctype == "flat_per_deal":
                    if esiid in first_payment_esiids:
                        amt = float(c.get("amount") or 0)
                        if amt:
                            payout += amt
                            b["bonuses"] += amt
                            applied.append(f"new-deal bonus ${amt:.2f}")

        b["total"] += payout
        b["deals"].append({
            "esiid": esiid, "customer": deal["customer"], "supplier": sup_name or deal["supplier"],
            "deal_source": deal["source"], "deal_id": deal["id"],
            "kwh_paid": round(kwh, 2), "gross_received": round(gross, 2),
            "first_payment": esiid in first_payment_esiids,
            "excluded": excluded, "plan_type": deal["plan_type"],
            "commission": round(payout, 4),
            "applied": "; ".join(applied) if applied else ("excluded plan type" if excluded else "no matching component"),
        })

    # flat monthly components — once per agent, only in months they had paid deals
    for name, b in agents.items():
        plan = plans.get(norm_name(name))
        if not plan or b["deals_paid"] == 0:
            continue
        for c in plan["components"]:
            if c.get("type") == "flat_monthly":
                amt = float(c.get("amount") or 0)
                b["flat_monthly"] += amt
                b["total"] += amt

    warnings = []
    for nm, cnt in sorted(unknown_agent_names.items()):
        warnings.append(f"Deals credit agent '{nm}' ({cnt} paid accounts) but no such agent is registered — "
                        f"add them (or fix the name on the deals) and recalculate.")
    for nm, plan in plans.items():
        if plan["name"] in agents and not plan["components"] and agents[plan["name"]]["total"] == 0:
            warnings.append(f"{plan['name']} has paid deals but NO commission plan configured — payout is $0 "
                            f"until you set their plan.")

    for b in agents.values():
        for k in ("total", "residual", "bonuses", "flat_monthly", "gross_received"):
            b[k] = round(b[k], 2)
        b["deals"].sort(key=lambda d: -d["commission"])

    unassigned["no_deal"]["gross"] = round(unassigned["no_deal"]["gross"], 2)
    unassigned["no_agent_on_deal"]["gross"] = round(unassigned["no_agent_on_deal"]["gross"], 2)

    return {"agents": agents, "unassigned": unassigned, "warnings": warnings,
            "rows": len(rows), "label": label,
            "gross_total": round(sum(float(r.get("raw_amount") or 0) for r in rows), 2)}
