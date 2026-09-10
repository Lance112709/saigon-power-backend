"""Agent commission engine tests — payouts must come from provider-paid rows."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.agent_commission_engine import calculate_month, plan_components, enrollment_math


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


# ── flat_per_enrollment: paid at contract start, once per service address ────

def cdeal(id, agent_name, start, end, address, zipc="77036", owner=None, status="ACTIVE", supplier="Heritage Power"):
    return {"id": id, "deal_status": status, "provider": supplier, "esiid": "", "adder": 0.007,
            "product_type": "Fixed Rate", "contract_term": "12", "sales_agent": agent_name, "business_name": None,
            "deal_owner": owner, "contract_start_date": start, "contract_end_date": end,
            "service_address": address, "service_zip": zipc, "created_at": start,
            "crm_customers": {"full_name": f"Cust {id}", "postal_code": zipc}}


def run_enroll(deals, payments=(), audit=()):
    db = FakeDB({"sales_agents": [agent("Nga Nguyen", {"components": [{"type": "flat_per_enrollment", "amount": 5}]})],
                 "lead_deals": [], "crm_deals": list(deals), "actual_commissions": list(payments),
                 "audit_log": list(audit)})
    return calculate_month(db, 2026, 8)


def test_enrollment_bonus_pays_in_contract_start_month_without_provider_payment():
    r = run_enroll([cdeal("d1", "Nga Nguyen", "2026-08-10", "2027-08-10", "1 Main St"),
                    cdeal("d2", "Nga Nguyen", "2026-07-10", "2027-07-10", "2 Oak St")])
    nga = r["agents"]["Nga Nguyen"]
    assert nga["total"] == 5.0 and nga["enrolled"] == 1 and nga["held"] == 0
    assert r["rows"] == 0  # no provider rows needed


def test_renewal_at_contract_end_is_paid_not_held():
    old = cdeal("old", "Nga Nguyen", "2025-08-01", "2026-08-01", "5 Elm St", status="RENEWED")
    new = cdeal("new", "Nga Nguyen", "2026-08-01", "2027-08-01", "5 Elm St")
    nga = run_enroll([old, new])["agents"]["Nga Nguyen"]
    assert nga["total"] == 5.0 and nga["held"] == 0


def test_same_address_with_running_contract_is_held_until_released():
    first = cdeal("a1", "Nga Nguyen", "2026-05-01", "2027-05-01", "9 Pine St")
    dup = cdeal("a2", "Nga Nguyen", "2026-08-15", "2027-08-15", "9 Pine St")
    r = run_enroll([first, dup])
    nga = r["agents"]["Nga Nguyen"]
    assert nga["held"] == 1 and nga["total"] == 0.0
    d = nga["deals"][0]
    assert d["held"] and "HELD" in d["applied"] and d["duplicate_of"]["id"] == "a1"
    assert any("HELD for review" in w for w in r["warnings"])
    # admin releases it → paid next calculation
    r2 = run_enroll([first, dup], audit=[{"record_id": "a2", "action": "enrollment_bonus_release", "created_at": "2026-09-01"}])
    assert r2["agents"]["Nga Nguyen"]["total"] == 5.0 and r2["agents"]["Nga Nguyen"]["held"] == 0
    # admin rejects it → stays $0
    r3 = run_enroll([first, dup], audit=[{"record_id": "a2", "action": "enrollment_bonus_reject", "created_at": "2026-09-01"}])
    assert r3["agents"]["Nga Nguyen"]["total"] == 0.0 and r3["agents"]["Nga Nguyen"]["deals"][0]["hold_reason"] == "rejected"


def test_two_deals_same_address_same_month_hold_the_second():
    a = cdeal("m1", "Nga Nguyen", "2026-08-03", "2027-08-03", "12 Birch Ln")
    b = cdeal("m2", "Nga Nguyen", "2026-08-20", "2027-08-20", "12 Birch Ln")
    nga = run_enroll([a, b])["agents"]["Nga Nguyen"]
    assert nga["enrolled"] == 2 and nga["held"] == 1 and nga["total"] == 5.0


def test_imported_and_transferred_deals_never_earn_enrollment_bonus():
    nga = run_enroll([cdeal("i1", "Nga Nguyen", "2026-08-01", "2027-08-01", "3 Cedar", owner="heritage-book-import"),
                      cdeal("i2", "Nga Nguyen", "2026-08-01", "2027-08-01", "4 Cedar", owner="budget-direct-transfer")]) \
        ["agents"].get("Nga Nguyen")
    assert nga is None or nga["enrolled"] == 0


def test_identical_start_dates_hold_exactly_one_of_the_pair():
    a = cdeal("aaa", "Nga Nguyen", "2026-08-03", "2029-08-03", "2727 Pecan Ridge Dr")
    b = cdeal("bbb", "Nga Nguyen", "2026-08-03", "2029-08-03", "2727 Pecan Ridge Dr")
    nga = run_enroll([b, a])["agents"]["Nga Nguyen"]
    assert nga["enrolled"] == 2 and nga["held"] == 1 and nga["total"] == 5.0
    assert [d["deal_id"] for d in nga["deals"] if d["held"]] == ["bbb"]


# ── enrollment type: brand-new customer vs renewal ───────────────────────────

def test_enrollment_split_new_vs_renewal():
    old = cdeal("old", "Jennie Duong", "2025-08-01", "2026-08-01", "5 Elm St", status="RENEWED", supplier="Iron Horse")
    ren = cdeal("ren", "Nga Nguyen", "2026-08-01", "2027-08-01", "5 Elm St")          # same address → renewal
    new = cdeal("new", "Nga Nguyen", "2026-08-12", "2027-08-12", "77 Fresh Ave")      # nothing on file → new
    nga = run_enroll([old, ren, new])["agents"]["Nga Nguyen"]
    assert nga["enrolled"] == 2 and nga["new_enrollments"] == 1 and nga["renewals"] == 1
    by = {d["deal_id"]: d for d in nga["deals"]}
    assert by["ren"]["enrollment_type"] == "renewal" and by["ren"]["prior_contract"]["id"] == "old"
    assert "renewal — prior Iron Horse from 2025-08-01" in by["ren"]["applied"]
    assert by["new"]["enrollment_type"] == "new" and by["new"]["prior_contract"] is None
    assert "brand-new customer" in by["new"]["applied"]


def test_enrollment_type_matches_by_esiid_when_address_differs():
    old = cdeal("old", "Nga Nguyen", "2025-06-01", "2026-06-01", "1 Old Way")
    old["esiid"] = "1008901000000000000001"
    new = cdeal("new", "Nga Nguyen", "2026-08-01", "2027-08-01", "1 Old Way, Houston TX")
    new["esiid"] = "1008901000000000000001"
    nga = run_enroll([old, new])["agents"]["Nga Nguyen"]
    assert nga["renewals"] == 1 and nga["new_enrollments"] == 0
    # a contract that started the SAME month is a duplicate/hold question, not a renewal
    a = cdeal("m1", "Nga Nguyen", "2026-08-03", "2027-08-03", "12 Birch Ln")
    b = cdeal("m2", "Nga Nguyen", "2026-08-20", "2027-08-20", "12 Birch Ln")
    nga2 = run_enroll([a, b])["agents"]["Nga Nguyen"]
    assert nga2["new_enrollments"] == 2 and nga2["renewals"] == 0


def test_enrollment_only_plan_ignores_provider_payments_entirely():
    """Nga's rule: payout = customers enrolled in the month × rate. Provider
    dollars on her accounts never show on her breakdown or change the total."""
    old = cdeal("old", "Nga Nguyen", "2023-12-26", "2026-12-25", "20303 Kingsland Blvd")
    old["esiid"] = "1008901049787440449100"
    new = cdeal("new", "Nga Nguyen", "2026-08-05", "2027-08-05", "1 New St")
    pay = {"raw_esiid": "1008901049787440449100", "raw_amount": 18.48, "raw_kwh": 2640,
           "raw_rate": None, "supplier_id": "s1", "billing_month": "2026-08-01", "suppliers": {"name": "APG&E", "code": "APGE"}}
    r = run_enroll([old, new], payments=[pay])
    nga = r["agents"]["Nga Nguyen"]
    assert nga["enrollment_only"] is True and nga["enrollment_rate"] == 5.0
    assert nga["deals_paid"] == 0 and nga["gross_received"] == 0.0
    assert [d["deal_id"] for d in nga["deals"]] == ["new"]
    assert nga["enrolled"] == 1 and nga["total"] == 5.0


def test_enrollment_bonus_by_segment_residential_vs_commercial():
    """Jennie's rule: $5 per residential enrollment, $10 per commercial one."""
    plan = {"components": [{"type": "flat_per_enrollment", "amount": 5, "segment": "residential"},
                           {"type": "flat_per_enrollment", "amount": 10, "segment": "commercial"}]}
    res = cdeal("r1", "Jennie Duong", "2026-08-04", "2027-08-04", "1 Home St")
    com = cdeal("c1", "Jennie Duong", "2026-08-06", "2027-08-06", "9 Shop Rd"); com["meter_type"] = "Commercial"
    biz = cdeal("c2", "Jennie Duong", "2026-08-09", "2027-08-09", "11 Plaza Dr"); biz["business_name"] = "Pho 99"
    lead_com = {"id": "l1", "status": "Active", "supplier": "Heritage Power", "esiid": "", "rate_type": "Fixed Rate",
                "plan_name": None, "contract_term": "12", "sales_agent": "Jennie Duong", "product_type": "Commercial",
                "start_date": "2026-08-12", "end_date": "2027-08-12", "service_address": "5 Mall Way", "service_zip": "77036",
                "created_at": "2026-08-12", "leads": {"first_name": "Nail", "last_name": "Salon", "business_name": "Nails"}}
    db = FakeDB({"sales_agents": [agent("Jennie Duong", plan)], "lead_deals": [lead_com],
                 "crm_deals": [res, com, biz], "actual_commissions": [], "audit_log": []})
    j = calculate_month(db, 2026, 8)["agents"]["Jennie Duong"]
    assert j["enrollment_only"] and j["enrollment_rate"] == 0.0  # mixed rates → per segment
    assert j["enrolled"] == 4 and j["total"] == 35.0
    assert j["enrolled_by_segment"] == {"residential": {"count": 1, "paid": 1, "amount": 5.0},
                                        "commercial": {"count": 3, "paid": 3, "amount": 30.0}}
    assert enrollment_math(j) == "1 residential × $5 + 3 commercial × $10"
    by = {d["deal_id"]: d for d in j["deals"]}
    assert by["r1"]["segment"] == "residential" and by["c1"]["segment"] == "commercial" and by["l1"]["segment"] == "commercial"
    assert "commercial, contract start 2026-08-06" in by["c1"]["applied"]
    # a plan with no segment on the component still pays everyone
    db2 = FakeDB({"sales_agents": [agent("Jennie Duong", {"components": [{"type": "flat_per_enrollment", "amount": 5}]})],
                  "lead_deals": [lead_com], "crm_deals": [res, com, biz], "actual_commissions": [], "audit_log": []})
    j2 = calculate_month(db2, 2026, 8)["agents"]["Jennie Duong"]
    assert j2["total"] == 20.0 and enrollment_math(j2) == "4 enrolled × $5"
