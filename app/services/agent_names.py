"""Sales-agent name hygiene.

Deals store the agent as free text, so 'NGA NGUYEN', 'Nga Nguyen ' and
'Nga Nguyen' were three different people to the payout engine. Every write
path canonicalizes the name against sales_agents (case/space-insensitive);
normalize_deal_agent_names() repairs what is already stored.
"""
import re
from typing import Optional

from app.services.reconciliation_v2 import fetch_all


def _key(s) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip().lower()


def registered_agents(db) -> dict:
    """{normalized name: canonical display name} from sales_agents."""
    rows = db.table("sales_agents").select("name").execute().data or []
    return {_key(r["name"]): r["name"].strip() for r in rows if r.get("name")}


def canonical_agent(db, name, registry: dict = None) -> Optional[str]:
    """The registered spelling of `name` (None for blank). Unknown names are
    returned trimmed and single-spaced — never invented, never dropped."""
    raw = re.sub(r"\s+", " ", str(name or "")).strip()
    if not raw:
        return None
    reg = registry if registry is not None else registered_agents(db)
    return reg.get(_key(raw), raw)


SUFFIX_RE = re.compile(r"\b(realtor|loan\s*officer|loan\s*offier|insurance|agent|referral|apt.*|\(.*\))\b.*$", re.I)


def suggest_canonical(raw: str, registry: dict) -> Optional[str]:
    """Best registered match for an off-registry spelling: the name with a
    trailing role word removed ('Tai Dinh Realtor' → 'Tai Dinh'), or a
    registered name the spelling starts with. None when nothing fits."""
    base = _key(SUFFIX_RE.sub("", raw))
    if base and base in registry:
        return registry[base]
    k = _key(raw)
    hits = [v for kk, v in registry.items() if kk and (k.startswith(kk + " ") or kk == k)]
    if len(hits) == 1:
        return hits[0]
    return None


START_COL = {"crm_deals": "contract_start_date", "lead_deals": "start_date"}


def agent_name_report(db, month_label: str = None) -> dict:
    """Which agent spellings on deals are off-registry, with deal counts.
    fixable = differs from a registered agent only by case/spacing;
    unknown  = matches nobody (with a suggested registered name when one is
    obvious). With month_label (YYYY-MM) unknown names are counted only on
    deals whose contract starts that month, so the monthly checklist shows
    what matters now rather than every legacy referral name."""
    reg = registered_agents(db)
    fixable, unknown = {}, {}
    for table, col in (("crm_deals", "sales_agent"), ("lead_deals", "sales_agent")):
        start_col = START_COL[table]
        for r in fetch_all(db, table, f"id,{col},{start_col}"):
            raw = r.get(col)
            if not raw:
                continue
            canon = reg.get(_key(raw))
            if canon is None:
                if month_label and (r.get(start_col) or "")[:7] != month_label:
                    continue
                u = unknown.setdefault(raw, {"deals": 0, "suggested": suggest_canonical(raw, reg)})
                u["deals"] += 1
            elif canon != raw:
                fixable.setdefault(raw, {"canonical": canon, "deals": 0})["deals"] += 1
    return {"fixable": fixable, "unknown": unknown}


def normalize_deal_agent_names(db, dry_run: bool = True, renames: dict = None) -> dict:
    """Rewrite deal agent names. Automatically: spellings that differ from a
    registered agent only by case/spacing. Explicitly: `renames`
    {'Tai Dinh Realtor': 'Tai Dinh'} chosen by the admin — the target must be
    a registered agent. Everything else is left alone and reported."""
    reg = registered_agents(db)
    explicit = {}
    for frm, to in (renames or {}).items():
        canon = reg.get(_key(to))
        if not canon:
            raise ValueError(f"'{to}' is not a registered agent")
        explicit[_key(frm)] = canon
    changed = []
    for table, col in (("crm_deals", "sales_agent"), ("lead_deals", "sales_agent")):
        for r in fetch_all(db, table, f"id,{col}"):
            raw = r.get(col)
            if not raw:
                continue
            canon = reg.get(_key(raw)) or explicit.get(_key(raw))
            if canon and canon != raw:
                changed.append({"table": table, "id": r["id"], "from": raw, "to": canon})
                if not dry_run:
                    db.table(table).update({col: canon}).eq("id", r["id"]).execute()
    return {"dry_run": dry_run, "changed": len(changed), "changes": changed[:200],
            "unknown": agent_name_report(db)["unknown"]}
