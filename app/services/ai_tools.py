"""Read-only CRM query tools for the AI chat agent.

Gives the chat model real reach into the database instead of canned answers:
it can query and aggregate over an allowlisted set of tables, with sensitive
tables (users/auth) excluded entirely. Everything here is READ ONLY — there is
no insert/update/delete path, and filters/columns are validated against the
allowlist before touching the database.
"""
import json
from typing import Optional

from app.services.reconciliation_v2 import fetch_all

MAX_ROWS = 100          # rows returned to the model per query
MAX_SCAN = 100_000      # safety cap for aggregate scans
MAX_RESULT_CHARS = 14_000

# table -> {"desc": ..., "cols": [...]} — the model sees this as its schema.
# users/role_permissions are deliberately absent (auth data).
ALLOWED_TABLES: dict = {
    "leads": {
        "desc": "Sales leads (prospects). status: New/Contacted/Converted/Dropped etc.",
        "cols": ["id", "first_name", "last_name", "status", "phone", "email", "source",
                 "referral_by", "business_name", "created_at", "updated_at"],
    },
    "lead_deals": {
        "desc": "Deal book part 1 — deals from leads (mostly residential). "
                "status 'Active' = live. adder = commission rate $/kWh. supplier = provider name.",
        "cols": ["id", "lead_id", "status", "supplier", "esiid", "adder", "est_kwh",
                 "rate", "rate_type", "start_date", "end_date", "sales_agent",
                 "service_address", "service_city", "service_zip", "provider_status",
                 "contract_term", "created_at"],
    },
    "crm_deals": {
        "desc": "Deal book part 2 — imported book (7k+ deals, most of the business). "
                "deal_status 'ACTIVE' = live. adder = commission rate $/kWh. provider = provider name.",
        "cols": ["id", "customer_id", "deal_name", "deal_status", "provider", "esiid",
                 "adder", "energy_rate", "product_type", "meter_type", "sales_agent",
                 "contract_start_date", "contract_end_date", "contract_term",
                 "service_address", "provider_status", "created_at"],
    },
    "crm_customers": {
        "desc": "Imported customers (linked from crm_deals.customer_id).",
        "cols": ["id", "full_name", "first_name", "last_name", "phone", "email",
                 "city", "state", "postal_code", "created_at"],
    },
    "suppliers": {
        "desc": "Energy providers (REPs) that pay commissions.",
        "cols": ["id", "name", "code", "default_adder", "is_active", "contact_email"],
    },
    "actual_commissions": {
        "desc": "Commission payments ledger — one row per account per statement (70k+ rows). "
                "raw_amount = $ paid, raw_rate = $/kWh paid, billing_month = statement month.",
        "cols": ["raw_esiid", "raw_customer_name", "raw_amount", "raw_kwh", "raw_rate",
                 "billing_month", "supplier_id", "is_matched"],
    },
    "reconciliation_runs": {
        "desc": "One row per provider+month reconciliation: expected vs received totals.",
        "cols": ["id", "billing_month", "supplier_id", "total_expected", "total_actual",
                 "total_discrepancy", "matched_count", "short_paid_count", "over_paid_count",
                 "missing_count", "unexpected_count", "run_at"],
    },
    "reconciliation_items": {
        "desc": "Per-account reconciliation findings. status: matched/short_paid/over_paid/missing/unexpected.",
        "cols": ["esiid", "supplier_id", "billing_month", "expected_amount", "actual_amount",
                 "discrepancy_amount", "status", "severity", "is_resolved", "resolution_notes"],
    },
    "exception_cases": {
        "desc": "Durable audit cases (money issues being worked). workflow_status: "
                "open/investigating/waiting_on_provider/resolved/recovered/ignored.",
        "cols": ["id", "supplier_id", "billing_month", "esiid", "issue_type", "workflow_status",
                 "priority_score", "estimated_loss", "recovered_amount", "customer_name",
                 "agent", "recommended_action", "explanation", "dispute_id"],
    },
    "audit_findings": {
        "desc": "Grouped systemic audit findings (e.g. a provider cutting rates across the book).",
        "cols": ["id", "supplier_id", "billing_month", "finding_type", "severity", "title",
                 "explanation", "affected_count", "estimated_impact", "status"],
    },
    "disputes": {
        "desc": "Dispute packages sent to providers. status: draft/sent/provider_responded/recovered/rejected.",
        "cols": ["id", "supplier_id", "status", "title", "months", "total_claimed",
                 "total_recovered", "created_at", "sent_at"],
    },
    "expected_commission_snapshots": {
        "desc": "Permanent expected-vs-paid history per account per month.",
        "cols": ["esiid", "supplier_id", "billing_month", "expected_amount", "actual_amount",
                 "variance_amount", "rate_expected", "rate_paid", "status", "created_at"],
    },
    "commission_rules": {
        "desc": "Versioned provider pay rules (rate per kWh, flat, tiered).",
        "cols": ["id", "supplier_id", "name", "rule_type", "config", "effective_from",
                 "effective_to", "version"],
    },
    "sales_agents": {
        "desc": "Sales agents and their payout plans.",
        "cols": ["id", "name", "agent_code", "agent_type", "email", "phone"],
    },
    "agent_commissions": {
        "desc": "Monthly agent payout runs. status: calculated/approved/closed_out/paid.",
        "cols": ["id", "agent_name", "month", "year", "total_deals", "total_commission", "status"],
    },
    "upload_batches": {
        "desc": "Imported provider statement files.",
        "cols": ["id", "original_filename", "supplier_id", "status", "rows_imported",
                 "amount_received", "total_affinity_amount", "created_at"],
    },
    "ai_alerts": {
        "desc": "System alerts. status open/resolved.",
        "cols": ["id", "type", "severity", "status", "message", "created_at"],
    },
    "tasks": {
        "desc": "Follow-up tasks for staff.",
        "cols": ["id", "title", "status", "due_date", "assigned_to", "created_at"],
    },
    "proposals": {
        "desc": "Customer proposals.",
        "cols": ["id", "customer_name", "status", "created_at"],
    },
}

_ALLOWED_OPS = {"eq", "neq", "gt", "gte", "lt", "lte", "like", "ilike", "is", "in"}


def schema_for_prompt() -> str:
    lines = []
    for t, meta in ALLOWED_TABLES.items():
        lines.append(f"- {t}: {meta['desc']} Columns: {', '.join(meta['cols'])}")
    return "\n".join(lines)


def _validate(table: str, columns: Optional[list], filters: Optional[list]) -> tuple:
    if table not in ALLOWED_TABLES:
        raise ValueError(f"Table '{table}' is not queryable. Available: {', '.join(ALLOWED_TABLES)}")
    allowed = set(ALLOWED_TABLES[table]["cols"])
    cols = columns or ALLOWED_TABLES[table]["cols"]
    bad = [c for c in cols if c not in allowed]
    if bad:
        raise ValueError(f"Column(s) {bad} not available on {table}. Available: {sorted(allowed)}")
    norm_filters = []
    for f in filters or []:
        col, op, val = f.get("column"), (f.get("op") or "eq").lower(), f.get("value")
        if col not in allowed:
            raise ValueError(f"Filter column '{col}' not available on {table}.")
        if op not in _ALLOWED_OPS:
            raise ValueError(f"Filter op '{op}' not supported. Use one of {sorted(_ALLOWED_OPS)}.")
        norm_filters.append((col, op, val))
    return cols, norm_filters


def _apply_filters(q, filters: list):
    for col, op, val in filters:
        if op == "in":
            q = q.in_(col, val if isinstance(val, list) else [val])
        elif op == "is":
            q = q.is_(col, "null" if val in (None, "null") else val)
        else:
            q = getattr(q, op)(col, val)
    return q


def query_crm(db, table: str, columns: Optional[list] = None,
              filters: Optional[list] = None, order_by: Optional[str] = None,
              descending: bool = True, limit: int = 25) -> dict:
    """Return sample rows plus the total matching count."""
    cols, norm = _validate(table, columns, filters)
    limit = max(1, min(int(limit or 25), MAX_ROWS))

    q = db.table(table).select(", ".join(cols), count="exact")
    q = _apply_filters(q, norm)
    if order_by:
        if order_by not in set(ALLOWED_TABLES[table]["cols"]):
            raise ValueError(f"order_by column '{order_by}' not available on {table}.")
        q = q.order(order_by, desc=descending)
    res = q.limit(limit).execute()
    rows = res.data or []
    return {"table": table, "total_matching": getattr(res, "count", None) or len(rows),
            "returned": len(rows), "rows": rows}


def aggregate_crm(db, table: str, filters: Optional[list] = None,
                  group_by: Optional[str] = None,
                  sum_columns: Optional[list] = None) -> dict:
    """Count (and optionally sum) matching rows, optionally grouped by a column."""
    needed = [c for c in ([group_by] if group_by else []) + (sum_columns or []) if c]
    cols, norm = _validate(table, needed or None, filters)
    if not needed:
        # count-only: no data fetch needed
        q = db.table(table).select("*", count="exact")
        q = _apply_filters(q, norm)
        res = q.limit(1).execute()
        return {"table": table, "count": getattr(res, "count", None) or 0}

    supabase_filters = []
    for col, op, val in norm:
        if op == "in":
            supabase_filters.append(("in_", (col, val if isinstance(val, list) else [val])))
        elif op == "is":
            supabase_filters.append(("is_", (col, "null" if val in (None, "null") else val)))
        else:
            supabase_filters.append((op, (col, val)))
    rows = fetch_all(db, table, ", ".join(needed), filters=supabase_filters)
    if len(rows) > MAX_SCAN:
        rows = rows[:MAX_SCAN]

    def _bucket():
        return {"count": 0, **{f"sum_{c}": 0.0 for c in (sum_columns or [])}}

    if group_by:
        groups: dict = {}
        for r in rows:
            key = str(r.get(group_by))
            g = groups.setdefault(key, _bucket())
            g["count"] += 1
            for c in sum_columns or []:
                g[f"sum_{c}"] += float(r.get(c) or 0)
        for g in groups.values():
            for c in sum_columns or []:
                g[f"sum_{c}"] = round(g[f"sum_{c}"], 2)
        ordered = dict(sorted(groups.items(), key=lambda kv: -kv[1]["count"])[:50])
        return {"table": table, "total_count": len(rows), "groups": ordered}

    out = _bucket()
    out["count"] = len(rows)
    for c in sum_columns or []:
        out[f"sum_{c}"] = round(sum(float(r.get(c) or 0) for r in rows), 2)
    return {"table": table, **out}


# ---- Anthropic tool definitions -------------------------------------------

TOOL_DEFINITIONS = [
    {
        "name": "query_crm",
        "description": (
            "Query a CRM table and get sample rows plus the total matching count. "
            "Call this whenever the user asks about specific records, names, lists, or details. "
            "IMPORTANT: deals live in TWO tables — lead_deals AND crm_deals — always check both "
            "for questions about deals/accounts/customers."),
        "input_schema": {
            "type": "object",
            "properties": {
                "table": {"type": "string", "description": "Table name from the schema."},
                "columns": {"type": "array", "items": {"type": "string"},
                            "description": "Columns to return (default: all allowed)."},
                "filters": {"type": "array", "items": {
                    "type": "object",
                    "properties": {
                        "column": {"type": "string"},
                        "op": {"type": "string",
                               "enum": ["eq", "neq", "gt", "gte", "lt", "lte", "like", "ilike", "is", "in"]},
                        "value": {"description": "Value; list for 'in'; use % wildcards with ilike."},
                    },
                    "required": ["column", "op", "value"],
                }},
                "order_by": {"type": "string"},
                "descending": {"type": "boolean"},
                "limit": {"type": "integer", "description": "Max rows (<=100, default 25)."},
            },
            "required": ["table"],
        },
    },
    {
        "name": "aggregate_crm",
        "description": (
            "Count rows and/or sum numeric columns, optionally grouped by a column. "
            "Call this for every 'how many', 'total', 'per provider/agent/month' question — "
            "it scans ALL matching rows (query_crm only returns a sample). "
            "Remember deals live in BOTH lead_deals (status='Active') and crm_deals (deal_status='ACTIVE')."),
        "input_schema": {
            "type": "object",
            "properties": {
                "table": {"type": "string"},
                "filters": {"type": "array", "items": {
                    "type": "object",
                    "properties": {
                        "column": {"type": "string"},
                        "op": {"type": "string",
                               "enum": ["eq", "neq", "gt", "gte", "lt", "lte", "like", "ilike", "is", "in"]},
                        "value": {},
                    },
                    "required": ["column", "op", "value"],
                }},
                "group_by": {"type": "string", "description": "Column to group counts by."},
                "sum_columns": {"type": "array", "items": {"type": "string"},
                                "description": "Numeric columns to sum."},
            },
            "required": ["table"],
        },
    },
]


def execute_tool(db, name: str, tool_input: dict) -> str:
    """Run one tool call and return a JSON string result (truncated for safety)."""
    try:
        if name == "query_crm":
            result = query_crm(db, **{k: v for k, v in tool_input.items()
                                      if k in ("table", "columns", "filters", "order_by",
                                               "descending", "limit")})
        elif name == "aggregate_crm":
            result = aggregate_crm(db, **{k: v for k, v in tool_input.items()
                                          if k in ("table", "filters", "group_by", "sum_columns")})
        else:
            return json.dumps({"error": f"Unknown tool {name}"})
        text = json.dumps(result, default=str)
        if len(text) > MAX_RESULT_CHARS:
            text = text[:MAX_RESULT_CHARS] + '... (truncated — narrow the query or use aggregate_crm)"}'
        return text
    except ValueError as e:
        return json.dumps({"error": str(e)})
    except Exception as e:
        return json.dumps({"error": f"Query failed: {str(e)[:200]}"})
