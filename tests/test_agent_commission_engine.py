"""Agent commission engine tests — payouts must come from provider-paid rows."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.agent_commission_engine import calculate_month, plan_components


class FQ:
    def __init__(self, db, table):
        self.db, self.tname = db, table
        self.preds = []
        self._range = None
        self._limit = None

    def select(self, *a, **k): return self
    def order(self, *a, **k): return self

    def eq(self, col, val):
        self.preds.append(lambda r: r.get(col) == val)
        return self

    def lt(self, col, val):
        self.preds.append(lambda r: (r.get(col) or "") < val)
        return self

    def in_(self, col, vals):
        vs = set(vals)
        self.preds.append(lambda r: r.get(col) in vs)
        return self

    def limit(self, n):
        self._limit = n
        return self

    def range(self, a, b):
        self._range = (a, b)
        return self

    def execute(self):
        rows = [r for r in self.db.tables.get(self.tname, []) if all(p(r) for p in self.preds)]
        if self._range:
            rows = rows[self._range[0]:self._range[1] + 1]
        elif self._limit:
            rows = rows[:self._limit]
        class R: pass
        res = R(); res.data = [dict(r) for r in rows]; res.count = len(rows)
        return res


class FakeDB:
    def __init__(self, tables):
        self.tables = tables

    def table(self, name):
        return FQ(self, name)


E1, E2, E3 = "1008901000000000000001", "1008901000000000000002", "1008901000000000000003"
SUP = {"name": "Budget Power", "code": "BUDGET"}


def paid(esiid, amount, kwh, month="2026-05-01"):
    return {"raw_esiid": esiid, "raw_amount": amount, "raw_kwh": kwh,
            "raw_rate": None, "supplier_id": "s1", "suppliers": SUP,
            "billing_month": month}


def agent(name, rules):
    return {"id": f"a-{name}", "name": name, "commission_rules": rules}


def ldeal(esiid, agent_name, plan_type="Fixed", supplier="Budget Power"):
    return {"id": f"d-{esiid}", "status": "Active", "supplier": supplier, "esiid": esiid,
            "adder": 0.008, "rate_type": plan_type, "plan_name": None, "contract_term": "24",
            "sales_agent": agent_name, "leads": {"first_name": "Test", "last_name": "Customer"}}


def run(agents, deals, payments):
    db = FakeDB({"sales_agents": agents, "lead_deals": deals, "crm_deals": [],
                 "actual_commissions": payments})
    return calculate_month(db, 2026, 5)


def test_per_kwh_pays_on_actual_kwh():
    out = run([agent("Amy", {"components": [{"type": "per_kwh", "rate": 0.001}]})],
              [ldeal(E1, "Amy")], [paid(E1, 8.0, 1000)])
    assert out["agents"]["Amy"]["total"] == 1.0  # 0.001 × 1000


def test_percent_of_commission_received():
    out = run([agent("Amy", {"components": [{"type": "percent_of_commission", "percent": 30}]})],
              [ldeal(E1, "Amy")], [paid(E1, 10.0, 1000)])
    assert out["agents"]["Amy"]["total"] == 3.0


def test_flat_per_deal_only_on_first_payment_month():
    a = [agent("Amy", {"components": [{"type": "flat_per_deal", "amount": 20}]})]
    d = [ldeal(E1, "Amy"), ldeal(E2, "Amy")]
    pay = [paid(E1, 8.0, 1000), paid(E2, 9.0, 1100),
           paid(E1, 7.5, 950, month="2026-04-01")]  # E1 already paid in April
    out = run(a, d, pay)
    # E2 is new → $20; E1 was paid before → no bonus
    assert out["agents"]["Amy"]["bonuses"] == 20.0
    assert out["agents"]["Amy"]["total"] == 20.0


def test_no_provider_payment_means_no_payout():
    out = run([agent("Amy", {"components": [{"type": "per_kwh", "rate": 0.001}]})],
              [ldeal(E1, "Amy")], [])
    assert out["agents"] == {}
    assert any("No provider payments" in w for w in out["warnings"])


def test_flat_monthly_once_when_agent_has_paid_deals():
    out = run([agent("Amy", {"components": [{"type": "flat_monthly", "amount": 250}]})],
              [ldeal(E1, "Amy"), ldeal(E2, "Amy")],
              [paid(E1, 8.0, 1000), paid(E2, 9.0, 1100)])
    assert out["agents"]["Amy"]["flat_monthly"] == 250.0
    assert out["agents"]["Amy"]["total"] == 250.0  # once, not per deal


def test_plan_type_exclusions():
    rules = {"components": [{"type": "flat_per_deal", "amount": 20}],
             "exclude_plan_types": ["Month-Month"]}
    out = run([agent("Amy", rules)],
              [ldeal(E1, "Amy", plan_type="Month-Month"), ldeal(E2, "Amy")],
              [paid(E1, 8.0, 1000), paid(E2, 9.0, 1100)])
    assert out["agents"]["Amy"]["total"] == 20.0
    assert out["agents"]["Amy"]["excluded_deals"] == 1


def test_supplier_scoped_component():
    rules = {"components": [
        {"type": "flat_per_deal", "amount": 10, "supplier": "NRG"},
        {"type": "flat_per_deal", "amount": 5},
    ]}
    out = run([agent("Jennie", rules)], [ldeal(E1, "Jennie")], [paid(E1, 8.0, 1000)])
    # Budget payment: NRG component skipped, unscoped $5 applies
    assert out["agents"]["Jennie"]["total"] == 5.0


def test_legacy_rules_translate():
    legacy = {"default_rate": 20, "default_type": "flat_per_deal",
              "overrides": [{"supplier": "NRG", "rate": 10, "type": "flat_per_deal"}],
              "exclude_plan_types": ["Month-Month"]}
    comps = plan_components(legacy)
    assert {c["type"] for c in comps} == {"flat_per_deal"}
    out = run([agent("Tai", legacy)], [ldeal(E1, "Tai")], [paid(E1, 8.0, 1000)])
    assert out["agents"]["Tai"]["total"] == 20.0  # unscoped default applies, NRG override doesn't


def test_agent_without_plan_gets_zero_and_warning():
    out = run([agent("Vince", {})], [ldeal(E1, "Vince")], [paid(E1, 8.0, 1000)])
    assert out["agents"]["Vince"]["total"] == 0.0
    assert any("NO commission plan" in w for w in out["warnings"])


def test_agent_name_case_insensitive_match():
    out = run([agent("Nga Nguyen", {"components": [{"type": "flat_per_deal", "amount": 5}]})],
              [ldeal(E1, "NGA  NGUYEN")], [paid(E1, 8.0, 1000)])
    assert out["agents"]["Nga Nguyen"]["total"] == 5.0


def test_unassigned_buckets():
    out = run([agent("Amy", {"components": [{"type": "flat_per_deal", "amount": 20}]})],
              [ldeal(E1, "Amy"), ldeal(E2, ""), ldeal(E3, "Ghost Agent")],
              [paid(E1, 8.0, 1000), paid(E2, 9.0, 1100), paid(E3, 7.0, 900),
               paid("1008901000000000000009", 5.0, 500)])
    u = out["unassigned"]
    assert u["no_deal"]["esiids"] == 1 and u["no_deal"]["gross"] == 5.0
    assert u["no_agent_on_deal"]["esiids"] == 1
    assert "Ghost Agent" in u["agent_not_registered"]
    assert any("Ghost Agent" in w for w in out["warnings"])


def test_split_rows_for_one_esiid_counted_once():
    # two statement lines for one meter: kWh sums, bonus pays once
    rules = {"components": [{"type": "per_kwh", "rate": 0.001},
                            {"type": "flat_per_deal", "amount": 20}]}
    out = run([agent("Amy", rules)], [ldeal(E1, "Amy")],
              [paid(E1, 4.0, 600), paid(E1, 3.0, 400)])
    a = out["agents"]["Amy"]
    assert a["deals_paid"] == 1
    assert a["residual"] == 1.0     # 0.001 × (600+400)
    assert a["bonuses"] == 20.0
