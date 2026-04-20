from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime
from app.db.client import get_client

TOLERANCE_PCT = Decimal("2.0")  # within 2% = matched

def normalize_esiid(esiid: str) -> str:
    return str(esiid).strip().lstrip("0") if esiid else ""

def classify_severity(pct: Decimal) -> str:
    pct = abs(pct)
    if pct < 2:
        return "low"
    elif pct < 10:
        return "medium"
    elif pct < 25:
        return "high"
    return "critical"

def run_reconciliation(billing_month: str, supplier_id: str = None, run_by: str = "system") -> dict:
    db = get_client()

    # Load expected
    q_exp = db.table("expected_commissions").select("*").eq("billing_month", billing_month)
    if supplier_id:
        q_exp = q_exp.eq("supplier_id", supplier_id)
    expected_rows = q_exp.execute().data

    # Load actual
    q_act = db.table("actual_commissions").select("*").eq("billing_month", billing_month)
    if supplier_id:
        q_act = q_act.eq("supplier_id", supplier_id)
    actual_rows = q_act.execute().data

    # Build lookup dicts by normalized ESIID
    expected_by_esiid = {}
    for row in expected_rows:
        sp = db.table("service_points").select("esiid").eq("id", row["service_point_id"]).single().execute()
        if sp.data:
            key = normalize_esiid(sp.data["esiid"])
            expected_by_esiid[key] = row

    actual_by_esiid = {}
    for row in actual_rows:
        key = normalize_esiid(row["raw_esiid"])
        actual_by_esiid[key] = row

    all_esiids = set(expected_by_esiid.keys()) | set(actual_by_esiid.keys())

    items = []
    totals = {"expected": Decimal("0"), "actual": Decimal("0"), "discrepancy": Decimal("0"),
              "matched": 0, "short_paid": 0, "over_paid": 0, "missing": 0, "unexpected": 0}

    for esiid in all_esiids:
        exp = expected_by_esiid.get(esiid)
        act = actual_by_esiid.get(esiid)

        exp_amt = Decimal(str(exp["expected_amount"])) if exp else None
        act_amt = Decimal(str(act["raw_amount"])) if act else None

        if exp_amt is not None and act_amt is not None:
            discrepancy = act_amt - exp_amt
            pct = (discrepancy / exp_amt * 100).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP) if exp_amt else Decimal("0")
            if abs(pct) <= TOLERANCE_PCT:
                status = "matched"
                totals["matched"] += 1
            elif discrepancy < 0:
                status = "short_paid"
                totals["short_paid"] += 1
            else:
                status = "over_paid"
                totals["over_paid"] += 1
            severity = classify_severity(pct)
        elif exp_amt is not None:
            discrepancy = -exp_amt
            pct = Decimal("-100")
            status = "missing"
            severity = "critical"
            totals["missing"] += 1
        else:
            discrepancy = act_amt
            pct = Decimal("100")
            status = "unexpected"
            severity = "low"
            totals["unexpected"] += 1

        totals["expected"] += exp_amt or Decimal("0")
        totals["actual"] += act_amt or Decimal("0")
        totals["discrepancy"] += discrepancy

        items.append({
            "esiid": esiid,
            "supplier_id": (exp or act)["supplier_id"],
            "billing_month": billing_month,
            "expected_commission_id": exp["id"] if exp else None,
            "actual_commission_id": act["id"] if act else None,
            "service_point_id": exp["service_point_id"] if exp else (act.get("service_point_id") if act else None),
            "expected_amount": float(exp_amt) if exp_amt is not None else None,
            "actual_amount": float(act_amt) if act_amt is not None else None,
            "discrepancy_amount": float(discrepancy),
            "discrepancy_percentage": float(pct),
            "status": status,
            "severity": severity,
        })

    # Create run record
    run = db.table("reconciliation_runs").insert({
        "billing_month": billing_month,
        "supplier_id": supplier_id,
        "total_expected": float(totals["expected"]),
        "total_actual": float(totals["actual"]),
        "total_discrepancy": float(totals["discrepancy"]),
        "matched_count": totals["matched"],
        "short_paid_count": totals["short_paid"],
        "over_paid_count": totals["over_paid"],
        "missing_count": totals["missing"],
        "unexpected_count": totals["unexpected"],
    }).execute().data[0]

    # Insert items
    for item in items:
        item["reconciliation_run_id"] = run["id"]
    if items:
        db.table("reconciliation_items").insert(items).execute()

    return {
        "run_id": run["id"],
        "billing_month": billing_month,
        "total_expected": float(totals["expected"]),
        "total_actual": float(totals["actual"]),
        "total_discrepancy": float(totals["discrepancy"]),
        "matched": totals["matched"],
        "short_paid": totals["short_paid"],
        "over_paid": totals["over_paid"],
        "missing": totals["missing"],
        "unexpected": totals["unexpected"],
        "items_count": len(items)
    }
