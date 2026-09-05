"""Sales-agent commission engine — pays agents from provider-paid dollars.

Every agent has a custom plan in sales_agents.commission_rules. A plan is a
list of components that can be freely combined:

  {"components": [
      {"type": "flat_per_deal", "amount": 20, "supplier": null},   # one-time, on the deal's FIRST provider payment
      {"type": "per_kwh", "rate": 0.001, "supplier": "Budget"},    # monthly, on ACTUAL kWh the provider paid on
      {"type": "percent_of_commission", "percent": 30},            # monthly, % of gross commission RECEIVED
      {"type": "flat_monthly", "amount": 250},                     # fixed monthly (only in months with paid deals)
      {"type": "flat_per_enrollment", "amount": 5}                 # one-time, in the month the deal's CONTRACT STARTS
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

flat_per_enrollment is the one component NOT tied to provider dollars: it pays
in the month the deal's contract starts (new deals and renewals alike), once
per service address. A deal whose address already carries another contract
that is still running is HELD (paid $0, listed for review) until an admin
releases or rejects it — see enrollment_bonuses().
"""
import re
from datetime import date

from app.services.reconciliation_v2 import fetch_all, norm_addr, zip5

# deals stamped by importers/transfers were never sold by the credited agent
IMPORTED_OWNER_RE = re.compile(r"import|transfer|trueup|backfill", re.I)
HOLD_RELEASE = "enrollment_bonus_release"
HOLD_REJECT = "enrollment_bonus_reject"

LEGACY_TYPE_MAP = {
    "per_kwh": "per_kwh",
    "percentage": "percent_of_commission",
    "flat_monthly": "flat_monthly",
    "flat_per_deal": "flat_per_deal",
    "flat_per_enrollment": "flat_per_enrollment",
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


def load_agent_plans(db, payout_label: str = None) -> dict:
    """All agent plans keyed by normalized name.

    With `payout_label` (YYYY-MM), eligible SGP Agents' plans are replaced by
    their permanent tier split for that month (see services/sgp_tiers.py).
    Agents not classified SGP_AGENT are byte-identical to the plain call —
    the tier program is strictly opt-in."""
    agents = fetch_all(db, "sales_agents", "id,name,commission_rules")
    plans = {norm_name(a["name"]): {"id": a["id"], "name": a["name"],
                                    "rules": a.get("commission_rules") or {},
                                    "components": plan_components(a.get("commission_rules"))}
             for a in agents if a.get("name")}
    if payout_label:
        try:
            from app.services.sgp_tiers import apply_sgp_overrides
            plans = apply_sgp_overrides(db, plans, payout_label)
        except Exception:
            pass  # SGP tables not migrated yet — legacy behavior
    return plans


def _sgp_pct_for_deal(plan: dict, deal_start) -> float:
    """NEXT_DEAL promotion rule: a deal keeps the split that was current when
    it started; deals started after a promotion get the new split."""
    history = plan.get("sgp_history") or []
    if not history or not deal_start:
        return float(plan.get("sgp_split") or 0)
    start = str(deal_start)[:10]
    pct = float(plan.get("sgp_base_split") or history[0]["split"])
    for h in history:
        if h["effective_from"] <= start:
            pct = float(h["split"])
    return pct


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
                       "provider_status,start_date,leads(first_name,last_name)"):
        lead = d.get("leads") or {}
        put(d.get("esiid"), {
            "source": "lead_deals", "id": d["id"], "active": d.get("status") == "Active",
            "provider_status": d.get("provider_status"), "start": d.get("start_date"),
            "agent": (d.get("sales_agent") or "").strip(),
            "supplier": (d.get("supplier") or "").strip(),
            "plan_type": (d.get("rate_type") or d.get("plan_name") or d.get("contract_term") or "").strip(),
            "adder": float(d["adder"]) if d.get("adder") is not None else None,
            "customer": f"{lead.get('first_name', '')} {lead.get('last_name', '')}".strip(),
        })
    for d in fetch_all(db, "crm_deals",
                       "id,deal_status,provider,esiid,adder,product_type,contract_term,sales_agent,business_name,"
                       "provider_status,contract_start_date,crm_customers(full_name)"):
        cust = d.get("crm_customers") or {}
        put(d.get("esiid"), {
            "source": "crm_deals", "id": d["id"], "active": d.get("deal_status") == "ACTIVE",
            "provider_status": d.get("provider_status"), "start": d.get("contract_start_date"),
            "agent": (d.get("sales_agent") or "").strip(),
            "supplier": (d.get("provider") or "").strip(),
            "plan_type": (d.get("product_type") or d.get("contract_term") or "").strip(),
            "adder": float(d["adder"]) if d.get("adder") is not None else None,
            "customer": cust.get("full_name") or d.get("business_name") or "",
        })
    return book


def _addr_key(address, zipcode) -> str:
    a = norm_addr(address)
    return f"{a}|{zip5(zipcode)}" if a else ""


def load_enrollment_book(db) -> list:
    """Every deal with the fields the enrollment bonus needs (both tables)."""
    out = []
    for d in fetch_all(db, "lead_deals",
                       "id,status,supplier,esiid,rate_type,plan_name,contract_term,sales_agent,"
                       "start_date,end_date,service_address,service_zip,created_at,leads(first_name,last_name)"):
        lead = d.get("leads") or {}
        out.append({
            "source": "lead_deals", "id": d["id"], "status": d.get("status") or "",
            "agent": (d.get("sales_agent") or "").strip(), "supplier": (d.get("supplier") or "").strip(),
            "plan_type": (d.get("rate_type") or d.get("plan_name") or d.get("contract_term") or "").strip(),
            "esiid": re.sub(r"\D", "", d.get("esiid") or ""),
            "start": (d.get("start_date") or "")[:10], "end": (d.get("end_date") or "")[:10],
            "address": d.get("service_address") or "", "addr_key": _addr_key(d.get("service_address"), d.get("service_zip")),
            "customer": f"{lead.get('first_name', '')} {lead.get('last_name', '')}".strip(),
            "owner": "", "created": (d.get("created_at") or "")[:10],
        })
    for d in fetch_all(db, "crm_deals",
                       "id,deal_status,provider,esiid,product_type,contract_term,sales_agent,business_name,deal_owner,"
                       "contract_start_date,contract_end_date,service_address,service_zip,created_at,"
                       "crm_customers(full_name,postal_code)"):
        cust = d.get("crm_customers") or {}
        out.append({
            "source": "crm_deals", "id": d["id"], "status": d.get("deal_status") or "",
            "agent": (d.get("sales_agent") or "").strip(), "supplier": (d.get("provider") or "").strip(),
            "plan_type": (d.get("product_type") or d.get("contract_term") or "").strip(),
            "esiid": re.sub(r"\D", "", d.get("esiid") or ""),
            "start": (d.get("contract_start_date") or "")[:10], "end": (d.get("contract_end_date") or "")[:10],
            "address": d.get("service_address") or "",
            "addr_key": _addr_key(d.get("service_address"), d.get("service_zip") or cust.get("postal_code")),
            "customer": cust.get("full_name") or d.get("business_name") or "",
            "owner": d.get("deal_owner") or "", "created": (d.get("created_at") or "")[:10],
        })
    return out


def load_hold_decisions(db) -> dict:
    """deal_id → 'release' | 'reject' (latest admin decision, from audit_log)."""
    out = {}
    try:
        rows = fetch_all(db, "audit_log", "record_id,action,created_at",
                         filters=[("in_", ("action", [HOLD_RELEASE, HOLD_REJECT]))])
    except Exception:
        return out
    for r in sorted(rows, key=lambda r: r.get("created_at") or ""):
        out[r["record_id"]] = "release" if r["action"] == HOLD_RELEASE else "reject"
    return out


def _plus_days(iso: str, days: int) -> str:
    from datetime import timedelta
    try:
        return (date.fromisoformat(iso) + timedelta(days=days)).isoformat()
    except Exception:
        return iso


def _prior_contract(deal: dict, by_esiid: dict, by_addr: dict):
    """The most recent contract on file for this meter / service address that
    started in an EARLIER month than `deal` — present means the customer was
    already ours and this enrollment is a renewal (same or different provider);
    absent means a brand-new customer. Matches by ESI ID first, then by the
    normalized service address for deals without one."""
    cands = []
    if deal["esiid"]:
        cands += by_esiid.get(deal["esiid"], [])
    if deal["addr_key"]:
        cands += by_addr.get(deal["addr_key"], [])
    best = None
    for o in cands:
        if o["id"] == deal["id"] or not o["start"] or o["start"][:7] >= deal["start"][:7]:
            continue
        if best is None or o["start"] > best["start"]:
            best = o
    return best


def _duplicate_of(deal: dict, same_addr: list):
    """Another contract at this service address that makes this one look like
    a second payment for the same enrollment: it started earlier (or the same
    month) and is still running well past this deal's start. A genuine renewal
    starts when the old contract ends, so it is NOT flagged; an early renewal
    or a re-entered deal is — and the admin decides."""
    for o in same_addr:
        if o["id"] == deal["id"] or not o["start"]:
            continue
        # the earlier contract is the one that gets paid; on identical start
        # dates the lower id wins so exactly one of the pair is held
        if (o["start"], o["id"]) > (deal["start"], deal["id"]):
            continue
        if o["start"][:7] == deal["start"][:7]:
            return o, "same service address, contract also started this month"
        if o["end"]:
            if o["end"] > _plus_days(deal["start"], 30):
                return o, f"same service address, earlier contract still running until {o['end']}"
        elif o["start"] > _plus_days(deal["start"], -365):
            return o, "same service address, earlier contract started within the last 12 months"
    return None, ""


def enrollment_bonuses(db, label: str, plans: dict, book: list = None, decisions: dict = None) -> dict:
    """flat_per_enrollment payouts for `label` (YYYY-MM): every deal credited to
    an agent with that component whose contract starts in the month. Returns
    {agent_display_name: [deal dicts]} — held deals carry commission 0."""
    book = book if book is not None else load_enrollment_book(db)
    decisions = decisions if decisions is not None else load_hold_decisions(db)
    by_addr: dict = {}
    by_esiid: dict = {}
    for d in book:
        if d["addr_key"]:
            by_addr.setdefault(d["addr_key"], []).append(d)
        if d["esiid"]:
            by_esiid.setdefault(d["esiid"], []).append(d)

    out: dict = {}
    for d in book:
        if not d["start"] or d["start"][:7] != label or not d["agent"]:
            continue
        if IMPORTED_OWNER_RE.search(d["owner"] or ""):
            continue
        plan = plans.get(norm_name(d["agent"]))
        if not plan:
            continue
        comps = [c for c in plan["components"] if c.get("type") == "flat_per_enrollment"
                 and _supplier_matches(c, d["supplier"], "", d["supplier"])]
        if not comps:
            continue
        excluded = _excluded(d["plan_type"], plan["rules"])
        amount = 0.0 if excluded else sum(float(c.get("amount") or 0) for c in comps)

        prior = _prior_contract(d, by_esiid, by_addr)
        enrollment_type = "renewal" if prior else "new"
        type_txt = (f"renewal — prior {prior['supplier'] or 'contract'} from {prior['start']}"
                    + (f" ended {prior['end']}" if prior and prior["end"] else "")) if prior else "brand-new customer"

        dup, why = (None, "")
        if d["addr_key"]:
            dup, why = _duplicate_of(d, by_addr.get(d["addr_key"], []))
        decision = decisions.get(d["id"])
        held = bool(dup) and decision != "release"
        rejected = decision == "reject"
        if rejected:
            held, commission, applied = True, 0.0, "rejected by admin — duplicate service address"
        elif held:
            commission = 0.0
            applied = f"HELD — {why} ({dup['customer'] or 'unknown'}, {dup['source']} {dup['id'][:8]})"
        elif excluded:
            commission, applied = 0.0, "excluded plan type"
        else:
            commission = amount
            applied = f"enrollment bonus ${amount:.2f} (contract start {d['start']}) · {type_txt}" + \
                      (" — released by admin" if dup and decision == "release" else "")
        out.setdefault(plan["name"], []).append({
            "kind": "enrollment", "esiid": d["esiid"], "customer": d["customer"], "supplier": d["supplier"],
            "deal_source": d["source"], "deal_id": d["id"], "address": d["address"],
            "contract_start": d["start"], "kwh_paid": 0, "gross_received": 0,
            "first_payment": False, "excluded": excluded, "plan_type": d["plan_type"],
            "enrollment_type": enrollment_type,
            "prior_contract": {"source": prior["source"], "id": prior["id"], "customer": prior["customer"],
                               "supplier": prior["supplier"], "agent": prior["agent"],
                               "contract_start": prior["start"], "contract_end": prior["end"]} if prior else None,
            "held": held, "hold_reason": why if held and not rejected else ("rejected" if rejected else ""),
            "duplicate_of": {"source": dup["source"], "id": dup["id"], "customer": dup["customer"],
                             "contract_start": dup["start"], "contract_end": dup["end"], "agent": dup["agent"]} if dup else None,
            "commission": round(commission, 4), "applied": applied,
        })
    return out


def _month_label(year: int, month: int) -> str:
    return f"{year}-{month:02d}"


def _paid_rows(db, label: str) -> list:
    return fetch_all(
        db, "actual_commissions",
        "raw_esiid,raw_amount,raw_kwh,raw_rate,supplier_id,suppliers(name,code)",
        filters=[("eq", ("billing_month", f"{label}-01"))])


def _previously_paid_esiids(db, esiids: list, label: str) -> set:
    """ESIIDs that already had a payment in ANY month before `label`.

    Must paginate past PostgREST's row cap: a bare limit(1000) returned an
    ARBITRARY subset when a chunk had more prior payments than that, which
    made first-payment detection — and therefore new-deal bonuses —
    nondeterministic between runs."""
    seen = set()
    esiids = list(esiids)
    for i in range(0, len(esiids), 100):
        rows = fetch_all(db, "actual_commissions", "raw_esiid",
                         filters=[("lt", ("billing_month", f"{label}-01")),
                                  ("in_", ("raw_esiid", esiids[i:i + 100]))])
        seen.update(r["raw_esiid"] for r in rows)
    return seen


def calculate_month(db, year: int, month: int, plans: dict = None, book: dict = None) -> dict:
    """Compute every agent's payout for a month from actual provider payments.

    Returns {agents: {display_name: {...}}, unassigned: {...}, warnings: [...]}.
    """
    label = _month_label(year, month)
    plans = plans if plans is not None else load_agent_plans(db, payout_label=label)
    book = book if book is not None else load_deal_book(db)

    rows = _paid_rows(db, label)
    enrolled = enrollment_bonuses(db, label, plans)
    if not rows and not enrolled:
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
            "enrollment_bonuses": 0.0, "enrolled": 0, "held": 0,
            "new_enrollments": 0, "renewals": 0,  # enrolled split: brand-new customers vs renewals
            "deals_paid": 0, "gross_received": 0.0, "excluded_deals": 0,
            "deals": [],  # per-deal detail for breakdowns
        })

    # enrollment bonuses first — they do not depend on provider payments
    for display_name, deals in enrolled.items():
        b = agent_bucket(display_name)
        for d in deals:
            b["enrolled"] += 1
            if d.get("enrollment_type") == "renewal":
                b["renewals"] += 1
            else:
                b["new_enrollments"] += 1
            if d["held"]:
                b["held"] += 1
            b["enrollment_bonuses"] += d["commission"]
            b["total"] += d["commission"]
            b["deals"].append(d)

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
                    pct = float(c.get("percent") or 0)
                    if plan.get("sgp_history"):
                        pct = _sgp_pct_for_deal(plan, deal.get("start"))
                    amt = pct / 100.0 * gross
                    if amt:
                        payout += amt
                        b["residual"] += amt
                        if plan.get("sgp_tier"):
                            label_txt = (f"SGP Tier {plan['sgp_tier']}" if pct == plan.get("sgp_split")
                                         else "SGP")
                            applied.append(f"{label_txt} ({pct:g}%) of ${gross:.2f} = ${amt:.2f}")
                        else:
                            applied.append(f"{pct:g}% of ${gross:.2f} = ${amt:.2f}")
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
    # SGP eligibility notes (e.g. classified SGP but agreement not approved)
    for plan in plans.values():
        if isinstance(plan, dict) and plan.get("sgp_warning") and plan.get("name") in agents:
            warnings.append(plan["sgp_warning"])
    for nm, cnt in sorted(unknown_agent_names.items()):
        warnings.append(f"Deals credit agent '{nm}' ({cnt} paid accounts) but no such agent is registered — "
                        f"add them (or fix the name on the deals) and recalculate.")
    held_total = sum(b["held"] for b in agents.values())
    if held_total:
        warnings.append(f"{held_total} enrollment bonus(es) HELD for review — same service address as another "
                        f"contract. Open the agent's breakdown to release or reject each one before payout.")
    for nm, plan in plans.items():
        if plan["name"] in agents and not plan["components"] and agents[plan["name"]]["total"] == 0:
            warnings.append(f"{plan['name']} has paid deals but NO commission plan configured — payout is $0 "
                            f"until you set their plan.")

    for b in agents.values():
        for k in ("total", "residual", "bonuses", "flat_monthly", "gross_received", "enrollment_bonuses"):
            b[k] = round(b[k], 2)
        b["deals"].sort(key=lambda d: -d["commission"])

    unassigned["no_deal"]["gross"] = round(unassigned["no_deal"]["gross"], 2)
    unassigned["no_agent_on_deal"]["gross"] = round(unassigned["no_agent_on_deal"]["gross"], 2)

    return {"agents": agents, "unassigned": unassigned, "warnings": warnings,
            "rows": len(rows), "label": label,
            "gross_total": round(sum(float(r.get("raw_amount") or 0) for r in rows), 2)}


def save_month_results(db, year: int, month: int, result: dict, performed_by: str):
    """Persist a calculate_month() result as agent_commissions rows (one per
    agent-month) with a 'recalculated' log line each. Rows already approved,
    closed out or paid are locked and left untouched. Returns (saved, locked)."""
    import json
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    saved, locked = [], []
    for agent, vals in result["agents"].items():
        existing = (
            db.table("agent_commissions")
            .select("id, status")
            .eq("agent_name", agent).eq("month", month).eq("year", year)
            .limit(1).execute().data
        )
        summary_note = json.dumps({
            "engine": "actuals-v1",
            "gross_received": vals["gross_received"],
            "residual": vals["residual"],
            "bonuses": vals["bonuses"],
            "flat_monthly": vals["flat_monthly"],
            "enrollment_bonuses": vals.get("enrollment_bonuses", 0),
            "enrolled": vals.get("enrolled", 0),
            "held": vals.get("held", 0),
            "new_enrollments": vals.get("new_enrollments", 0),
            "renewals": vals.get("renewals", 0),
            "excluded_deals": vals["excluded_deals"],
        })

        if existing:
            rec = existing[0]
            if rec["status"] in ("approved", "closed_out", "paid"):
                locked.append(agent)
                continue
            db.table("agent_commissions").update({
                "total_deals":      vals["deals_paid"] + vals.get("enrolled", 0),
                "total_commission": vals["total"],
                "status":           "calculated",
                "notes":            summary_note,
                "updated_at":       now,
            }).eq("id", rec["id"]).execute()
            rec_id = rec["id"]
        else:
            ins = db.table("agent_commissions").insert({
                "agent_name":       agent,
                "month":            month,
                "year":             year,
                "total_deals":      vals["deals_paid"] + vals.get("enrolled", 0),
                "total_commission": vals["total"],
                "status":           "calculated",
                "notes":            summary_note,
                "created_at":       now,
                "updated_at":       now,
            }).execute().data
            rec_id = ins[0]["id"] if ins else None

        db.table("commission_logs").insert({
            "commission_id": rec_id,
            "action":        "recalculated",
            "performed_by":  performed_by,
            "agent_name":    agent,
            "month":         month,
            "year":          year,
            "notes":         f"{vals['deals_paid']} paid deals · {vals.get('enrolled', 0)} enrolled"
                             f" ({vals.get('new_enrollments', 0)} new · {vals.get('renewals', 0)} renewals"
                             f"{' · ' + str(vals['held']) + ' held' if vals.get('held') else ''})"
                             f" · gross ${vals['gross_received']} · payout ${vals['total']}",
            "created_at":    now,
        }).execute()
        saved.append({"agent_name": agent, "total_commission": vals["total"],
                      "deals_paid": vals["deals_paid"], "gross_received": vals["gross_received"]})
    return saved, locked
