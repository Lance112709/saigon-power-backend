"""AI chat query tools: allowlist enforcement + query/aggregate semantics."""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from fakedb import FakeDB
from app.services.ai_tools import (
    ALLOWED_TABLES, aggregate_crm, execute_tool, query_crm, schema_for_prompt,
)


def seeded_db():
    db = FakeDB()
    db.tables["crm_deals"] = [
        {"id": f"d{i}", "deal_status": "ACTIVE" if i < 7 else "CANCELLED",
         "provider": "Discount Power" if i % 2 == 0 else "NRG",
         "adder": 0.008, "esiid": f"100890100000000000{i:04d}",
         "sales_agent": "Lance", "energy_rate": 0.09, "product_type": "Fixed",
         "meter_type": "Residential", "deal_name": f"Deal {i}", "customer_id": None,
         "contract_start_date": None, "contract_end_date": None,
         "contract_term": 12, "service_address": "", "provider_status": None,
         "created_at": f"2026-01-{i+1:02d}"}
        for i in range(10)
    ]
    return db


def test_users_table_not_queryable():
    assert "users" not in ALLOWED_TABLES
    assert "role_permissions" not in ALLOWED_TABLES
    with pytest.raises(ValueError):
        query_crm(seeded_db(), "users")


def test_unknown_column_rejected():
    with pytest.raises(ValueError):
        query_crm(seeded_db(), "crm_deals", columns=["password_hash"])
    with pytest.raises(ValueError):
        query_crm(seeded_db(), "crm_deals", filters=[{"column": "secret", "op": "eq", "value": 1}])


def test_unknown_op_rejected():
    with pytest.raises(ValueError):
        query_crm(seeded_db(), "crm_deals",
                  filters=[{"column": "provider", "op": "delete", "value": "x"}])


def test_query_filters_and_counts():
    out = query_crm(seeded_db(), "crm_deals",
                    filters=[{"column": "deal_status", "op": "eq", "value": "ACTIVE"}],
                    limit=3)
    assert out["total_matching"] == 7
    assert out["returned"] == 3


def test_aggregate_count_only():
    out = aggregate_crm(seeded_db(), "crm_deals",
                        filters=[{"column": "deal_status", "op": "eq", "value": "ACTIVE"}])
    assert out["count"] == 7


def test_aggregate_group_by_with_sum():
    out = aggregate_crm(seeded_db(), "crm_deals", group_by="provider",
                        sum_columns=["adder"])
    assert out["total_count"] == 10
    assert out["groups"]["Discount Power"]["count"] == 5
    assert abs(out["groups"]["NRG"]["sum_adder"] - 0.04) < 1e-9


def test_execute_tool_returns_json_and_survives_errors():
    db = seeded_db()
    ok = json.loads(execute_tool(db, "query_crm", {"table": "crm_deals", "limit": 2}))
    assert ok["returned"] == 2
    err = json.loads(execute_tool(db, "query_crm", {"table": "users"}))
    assert "error" in err
    unknown = json.loads(execute_tool(db, "nope", {}))
    assert "error" in unknown


def test_schema_prompt_mentions_both_deal_tables():
    s = schema_for_prompt()
    assert "lead_deals" in s and "crm_deals" in s
