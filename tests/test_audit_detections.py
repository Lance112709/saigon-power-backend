"""Extended audit detections: systemic rate cuts, clawbacks, stopped payments."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from fakedb import FakeDB
from app.services.audit_detections import (
    detect_clawback_anomalies, detect_churned_still_paid, detect_out_of_range,
    detect_payment_stopped, detect_systemic_rate_change, detect_term_mismatch,
    run_extended_audit,
)

SUP = "00000000-0000-0000-0000-000000000001"
LABEL = "2026-04"


def es(n):
    return f"10089010000000000{n:05d}"


def stmt_row(esiid, rate, kwh=1000.0, amount=None, row_type="commission", label=LABEL):
    return {"esiid": esiid, "rate": rate, "usage_kwh": kwh,
            "amount": amount if amount is not None else round(rate * kwh, 2),
            "row_type": row_type, "statement_label": label,
            "service_start": None, "service_end": None, "provider_status": ""}


def hist(esiid_rates_by_month):
    """{esiid: [(month, rate, amount)]} -> history dict."""
    out = {}
    for esiid, entries in esiid_rates_by_month.items():
        out[esiid] = [{"month": m, "rate": r, "amount": a, "kwh": 1000.0}
                      for m, r, a in entries]
    return out


def deal(esiid, active=True, start="2024-01-01", end="2027-01-01", name="Cust", agent="Lance"):
    return {"source": "lead_deals", "id": f"deal-{esiid}", "active": active,
            "status": "Active" if active else "Inactive", "adder": 0.008,
            "est_kwh": 1000.0, "start": start, "end": end, "esiid": esiid,
            "name": name, "agent": agent}


# -- systemic rate change (the Discount Power Apr-2026 case) ------------------

def test_systemic_rate_cut_grouped_into_one_finding():
    n = 455
    rows = {es(i): [stmt_row(es(i), 0.005)] for i in range(n)}
    history = hist({es(i): [("2026-02", 0.008, 8.0), ("2026-03", 0.008, 8.0)]
                    for i in range(n)})
    findings = detect_systemic_rate_change(rows, history, "Discount Power", LABEL)
    assert len(findings) == 1
    f = findings[0]
    assert f["affected_count"] == 455
    # (0.008 - 0.005) * 1000 kWh * 455 accounts = $1,365 this month
    assert abs(f["estimated_impact"] - 1365.0) < 0.01
    assert "0.008" in f["title"] and "0.005" in f["title"]
    assert "455 accounts" in f["explanation"]
    assert f["fingerprint"] == f"systemic_rate_change:{LABEL}:0.008->0.005"
    assert f["severity"] == "critical"


def test_below_threshold_rate_change_ignored():
    rows = {es(i): [stmt_row(es(i), 0.005)] for i in range(5)}  # only 5 accounts
    history = hist({es(i): [("2026-03", 0.008, 8.0)] for i in range(5)})
    assert detect_systemic_rate_change(rows, history, "DP", LABEL) == []


def test_rate_increase_not_flagged():
    rows = {es(i): [stmt_row(es(i), 0.010)] for i in range(50)}
    history = hist({es(i): [("2026-03", 0.008, 8.0)] for i in range(50)})
    assert detect_systemic_rate_change(rows, history, "DP", LABEL) == []


def test_distinct_rate_groups_produce_distinct_findings():
    rows = {}
    history = {}
    for i in range(15):
        rows[es(i)] = [stmt_row(es(i), 0.005)]
        history[es(i)] = hist({es(i): [("2026-03", 0.008, 8.0)]})[es(i)]
    for i in range(15, 30):
        rows[es(i)] = [stmt_row(es(i), 0.006)]
        history[es(i)] = hist({es(i): [("2026-03", 0.010, 10.0)]})[es(i)]
    findings = detect_systemic_rate_change(rows, history, "DP", LABEL)
    assert len(findings) == 2
    assert {f["details"]["rate_to"] for f in findings} == {0.005, 0.006}


# -- clawbacks -----------------------------------------------------------------

def test_clawback_without_prior_payment_flagged():
    rows = {es(1): [stmt_row(es(1), 0.008, amount=-12.5, row_type="clawback")]}
    findings = detect_clawback_anomalies(rows, {}, "Tara", LABEL)
    assert len(findings) == 1
    assert findings[0]["estimated_impact"] == 12.5
    assert "never paid" in findings[0]["details"]["accounts"][0]["reason"]


def test_clawback_exceeding_recent_payments_flagged():
    rows = {es(1): [stmt_row(es(1), 0.008, amount=-100.0, row_type="clawback")]}
    history = hist({es(1): [("2026-02", 0.008, 8.0), ("2026-03", 0.008, 8.0)]})
    findings = detect_clawback_anomalies(rows, history, "Tara", LABEL)
    assert len(findings) == 1
    assert "exceeds" in findings[0]["details"]["accounts"][0]["reason"]


def test_reasonable_clawback_not_flagged():
    rows = {es(1): [stmt_row(es(1), 0.008, amount=-5.0, row_type="clawback")]}
    history = hist({es(1): [("2026-03", 0.008, 8.0)]})
    assert detect_clawback_anomalies(rows, history, "Tara", LABEL) == []


# -- payment stopped -----------------------------------------------------------

def test_payment_stopped_after_streak():
    deals = {es(1): deal(es(1)), es(2): deal(es(2))}
    history = hist({es(1): [("2026-02", 0.008, 8.0), ("2026-03", 0.008, 8.0)],
                    es(2): [("2026-02", 0.008, 8.0), ("2026-03", 0.008, 8.0)]})
    findings = detect_payment_stopped(history, {es(2)}, deals, "DP", LABEL)
    assert len(findings) == 1
    assert findings[0]["affected_count"] == 1
    assert findings[0]["details"]["accounts"][0]["esiid"] == es(1)


def test_never_paid_account_not_stopped():
    deals = {es(1): deal(es(1))}
    assert detect_payment_stopped({es(1): []}, set(), deals, "DP", LABEL) == []


# -- churned still paid / term mismatch -----------------------------------------

def test_churned_still_paid():
    deals = {es(1): deal(es(1), active=False)}
    rows = {es(1): [stmt_row(es(1), 0.008)]}
    findings = detect_churned_still_paid(deals, rows, "DP", LABEL)
    assert len(findings) == 1 and findings[0]["affected_count"] == 1


def test_term_mismatch_paid_after_contract_end():
    deals = {es(1): deal(es(1), end="2025-06-30")}
    rows = {es(1): [stmt_row(es(1), 0.008)]}
    findings = detect_term_mismatch(deals, rows, "DP", LABEL)
    assert len(findings) == 1


def test_in_window_payment_is_fine():
    deals = {es(1): deal(es(1))}
    rows = {es(1): [stmt_row(es(1), 0.008)]}
    assert detect_term_mismatch(deals, rows, "DP", LABEL) == []


# -- out of range ----------------------------------------------------------------

def test_big_drop_from_median_flagged():
    rows = {es(1): [stmt_row(es(1), 0.008, kwh=100, amount=0.8)]}
    history = hist({es(1): [("2026-01", 0.008, 20.0), ("2026-02", 0.008, 22.0),
                            ("2026-03", 0.008, 21.0)]})
    findings = detect_out_of_range(rows, history, "DP", LABEL)
    assert len(findings) == 1
    assert findings[0]["details"]["accounts"][0]["deviation_pct"] < -50


def test_out_of_range_skips_covered_esiids():
    rows = {es(1): [stmt_row(es(1), 0.008, kwh=100, amount=0.8)]}
    history = hist({es(1): [("2026-01", 0.008, 20.0), ("2026-02", 0.008, 22.0)]})
    assert detect_out_of_range(rows, history, "DP", LABEL, skip_esiids={es(1)}) == []


# -- orchestrator: fingerprint idempotency ---------------------------------------

def _seed_db_for_orchestrator(n=12):
    db = FakeDB()
    db.tables["suppliers"] = [{"id": SUP, "name": "Discount Power"}]
    db.tables["actual_commissions"] = [
        {"supplier_id": SUP, "raw_esiid": es(i), "raw_rate": 0.008,
         "raw_amount": 8.0, "raw_kwh": 1000.0, "billing_month": f"{m}-01"}
        for i in range(n) for m in ("2026-02", "2026-03")]
    rows = [stmt_row(es(i), 0.005) for i in range(n)]
    deals = {"by_esiid": {es(i): deal(es(i)) for i in range(n)}}
    return db, rows, deals


def test_run_extended_audit_upserts_by_fingerprint():
    db, rows, deals = _seed_db_for_orchestrator()
    first = run_extended_audit(db, SUP, "Discount Power", LABEL, rows, deals)
    assert any(f["finding_type"] == "systemic_rate_change" for f in first)
    count_after_first = len(db.tables["audit_findings"])

    # re-run: same fingerprints must update, not duplicate
    second = run_extended_audit(db, SUP, "Discount Power", LABEL, rows, deals)
    assert len(db.tables["audit_findings"]) == count_after_first
    assert {f["fingerprint"] for f in first} == {f["fingerprint"] for f in second}


def test_run_extended_audit_preserves_finding_status_on_rerun():
    db, rows, deals = _seed_db_for_orchestrator()
    run_extended_audit(db, SUP, "Discount Power", LABEL, rows, deals)
    target = next(f for f in db.tables["audit_findings"]
                  if f["finding_type"] == "systemic_rate_change")
    target["status"] = "investigating"
    run_extended_audit(db, SUP, "Discount Power", LABEL, rows, deals)
    again = next(f for f in db.tables["audit_findings"]
                 if f["finding_type"] == "systemic_rate_change")
    assert again["status"] == "investigating"


def test_stale_open_finding_auto_resolves_when_fixed():
    db, rows, deals = _seed_db_for_orchestrator()
    run_extended_audit(db, SUP, "Discount Power", LABEL, rows, deals)
    fixed_rows = [stmt_row(e["esiid"], 0.008) for e in
                  [{"esiid": es(i)} for i in range(12)]]
    run_extended_audit(db, SUP, "Discount Power", LABEL, fixed_rows, deals)
    f = next(x for x in db.tables["audit_findings"]
             if x["finding_type"] == "systemic_rate_change")
    assert f["status"] == "resolved"
    assert f["resolved_by"] == "system"
