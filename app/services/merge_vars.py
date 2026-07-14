"""Email personalization ({{merge}} tag) resolution — one source of truth.

Both the single-send composer (/email/merge-vars) and the bulk campaign builder
turn a contact + their deals into the same set of tag values, so a template like
"Hi {{first_name}}, your {{esi_id}} contract ends {{contract_end_date}}" renders
identically everywhere.

Deal-level tags (ESI ID, service address, city/state/zip, contract dates) come
from the contact's ACTIVE deal when there is one; otherwise the first deal.
"""
from datetime import datetime

# The tags the compose UI offers, in display order. `tag` is what goes in
# {{...}}; `label` is the human name shown on the chip / editor.
MERGE_TAGS = [
    {"tag": "first_name",          "label": "First name"},
    {"tag": "last_name",           "label": "Last name"},
    {"tag": "service_address",     "label": "Service address"},
    {"tag": "city",                "label": "City"},
    {"tag": "state",               "label": "State"},
    {"tag": "zip",                 "label": "Zipcode"},
    {"tag": "esi_id",              "label": "ESI ID"},
    {"tag": "phone",               "label": "Phone number"},
    {"tag": "email",               "label": "Email"},
    {"tag": "contract_start_date", "label": "Contract start date"},
    {"tag": "contract_end_date",   "label": "Contract end date"},
]

EMPTY_VARS = {t["tag"]: "" for t in MERGE_TAGS}


def fmt_date(val) -> str:
    """Render an ISO/date string as e.g. 'October 15, 2026'. Falls back to raw."""
    if not val:
        return ""
    s = str(val)
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%m/%d/%Y"):
        try:
            return datetime.strptime(s[:len(fmt) + 8], fmt).strftime("%B %-d, %Y")
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).strftime("%B %-d, %Y")
    except ValueError:
        return s


def pick_deal(deals: list, status_key: str, active_val: str) -> dict:
    """Prefer an active deal for sourcing ESI ID / address / dates; else the first."""
    if not deals:
        return {}
    active = [d for d in deals if (d.get(status_key) or "").upper() == active_val]
    return (active or deals)[0]


def lead_merge_vars(lead: dict, deals: list) -> dict:
    """Resolve tag values for a converted lead-customer (leads + lead_deals)."""
    d = pick_deal(deals or [], "status", "ACTIVE")
    fn = (lead.get("first_name") or "").strip()
    ln = (lead.get("last_name") or "").strip()
    v = dict(EMPTY_VARS)
    v.update({
        "first_name":        fn,
        "last_name":         ln,
        "name":              f"{fn} {ln}".strip(),
        "phone":             lead.get("phone") or "",
        "email":             lead.get("email") or "",
        "city":              d.get("service_city") or lead.get("city") or "",
        "state":             d.get("service_state") or lead.get("state") or "",
        "zip":               d.get("service_zip") or lead.get("zip") or "",
        "service_address":   d.get("service_address") or lead.get("address") or "",
        "esi_id":            d.get("esiid") or "",
        "contract_start_date": fmt_date(d.get("start_date")),
        "contract_end_date":   fmt_date(d.get("end_date")),
    })
    return v


def crm_customer_merge_vars(c: dict, deals: list) -> dict:
    """Resolve tag values for an imported CRM customer (crm_customers + crm_deals)."""
    d = pick_deal(deals or [], "deal_status", "ACTIVE")
    full = (c.get("full_name") or "").strip()
    fn = (c.get("first_name") or (full.split()[0] if full else "")).strip()
    ln = (c.get("last_name") or (" ".join(full.split()[1:]) if full else "")).strip()
    v = dict(EMPTY_VARS)
    v.update({
        "first_name":        fn,
        "last_name":         ln,
        "name":              full or f"{fn} {ln}".strip(),
        "phone":             c.get("phone") or "",
        "email":             c.get("email") or "",
        "city":              c.get("city") or "",
        "state":             c.get("state") or "",
        "zip":               c.get("postal_code") or "",
        "service_address":   d.get("service_address") or "",
        "esi_id":            d.get("esiid") or "",
        "contract_start_date": fmt_date(d.get("contract_start_date")),
        "contract_end_date":   fmt_date(d.get("contract_end_date")),
    })
    return v
