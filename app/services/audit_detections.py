"""Extended commission audit — portfolio-level anomaly detection.

Runs after each statement reconciliation and looks for patterns a per-account
comparison can't see: a provider silently cutting the rate across the whole
book, clawbacks that were never earned, canceled accounts still being paid,
payments that stopped, amounts far outside an account's normal range.

Each detection is ONE grouped `audit_findings` row (with a plain-English
explanation and a per-account breakdown in `details`), keyed by a fingerprint
so re-running a month updates the finding instead of duplicating it.

All detectors are pure functions over pre-fetched data so they can be unit
tested without a database; `build_rate_history` is the only DB helper.
"""
from collections import Counter
from datetime import datetime, timezone
from typing import Optional

from app.services.audit import audit
from app.services.reconciliation_v2 import _in_window, _prev_labels, fetch_all

MIN_SYSTEMIC_ACCOUNTS = 10     # accounts moving to the same lower rate together
OUT_OF_RANGE_PCT = 50.0        # deviation vs the account's trailing median
OUT_OF_RANGE_MIN_DOLLARS = 5.0
STOPPED_MIN_STREAK = 2         # consecutive paid months before "stopped" counts


def build_rate_history(db, supplier_id: str, esiids: list, label: str,
                       months_back: int = 4) -> dict:
    """esiid -> [{month, rate, amount, kwh}] for the months BEFORE `label`."""
    months = [f"{m}-01" for m in _prev_labels(label, months_back)]
    hist: dict = {es: [] for es in esiids}
    for i in range(0, len(esiids), 100):
        chunk = esiids[i:i + 100]
        rows = fetch_all(db, "actual_commissions",
                         "raw_esiid,raw_rate,raw_amount,raw_kwh,billing_month",
                         filters=[("eq", ("supplier_id", supplier_id)),
                                  ("in_", ("billing_month", months)),
                                  ("in_", ("raw_esiid", chunk))])
        for r in rows:
            hist.setdefault(r["raw_esiid"], []).append({
                "month": str(r["billing_month"])[:7],
                "rate": float(r["raw_rate"]) if r.get("raw_rate") is not None else None,
                "amount": float(r["raw_amount"]) if r.get("raw_amount") is not None else 0.0,
                "kwh": float(r["raw_kwh"]) if r.get("raw_kwh") is not None else None,
            })
    return hist


def _modal_rate(entries: list) -> Optional[float]:
    rates = [round(e["rate"], 6) for e in entries if e.get("rate") is not None]
    if not rates:
        return None
    return Counter(rates).most_common(1)[0][0]


def _fmt_month(label: str) -> str:
    return datetime.strptime(label + "-01", "%Y-%m-%d").strftime("%B %Y")


def detect_systemic_rate_change(rows_by_esiid: dict, history: dict,
                                supplier_name: str, label: str,
                                min_accounts: int = MIN_SYSTEMIC_ACCOUNTS) -> list:
    """Many accounts moving from one rate to the same lower rate in one month
    = the provider repriced the book (e.g. Discount Power cutting 455 Value
    Power accounts from 0.008 to 0.005 in Apr 2026)."""
    groups: dict = {}
    for es, group in rows_by_esiid.items():
        cur = _modal_rate([{"rate": r.get("rate")} for r in group])
        prior = _modal_rate(history.get(es) or [])
        if cur is None or prior is None or cur >= prior - 1e-9:
            continue
        kwh = sum(r.get("usage_kwh") or 0 for r in group)
        loss = (prior - cur) * kwh
        groups.setdefault((prior, cur), []).append(
            {"esiid": es, "rate_from": prior, "rate_to": cur,
             "kwh": round(kwh, 1), "monthly_loss": round(loss, 2)})

    findings = []
    for (prior, cur), accounts in groups.items():
        if len(accounts) < min_accounts:
            continue
        impact = round(sum(a["monthly_loss"] for a in accounts), 2)
        findings.append({
            "finding_type": "systemic_rate_change",
            "severity": "critical",
            "title": f"{supplier_name} cut {len(accounts)} accounts from "
                     f"{prior:g} to {cur:g} $/kWh",
            "explanation": (
                f"In {_fmt_month(label)}, {len(accounts)} accounts that were consistently paid "
                f"{prior:g} $/kWh in prior months were all paid {cur:g} $/kWh — the provider "
                f"repriced this book, not individual accounts. Estimated loss this month: "
                f"${impact:,.2f} (≈${impact:,.0f}/month until corrected). "
                f"Dispute with the provider and request a true-up back to {prior:g}."),
            "affected_count": len(accounts),
            "estimated_impact": impact,
            "details": {"rate_from": prior, "rate_to": cur,
                        "accounts": sorted(accounts, key=lambda a: -a["monthly_loss"])},
            "fingerprint": f"systemic_rate_change:{label}:{prior:g}->{cur:g}",
            "affected_esiids": [a["esiid"] for a in accounts],
        })
    return findings


def detect_churned_still_paid(deals_by_esiid: dict, rows_by_esiid: dict,
                              supplier_name: str, label: str) -> list:
    accounts = []
    for es, group in rows_by_esiid.items():
        deal = deals_by_esiid.get(es)
        if deal is None or deal.get("active"):
            continue
        amt = sum(r.get("amount") or 0 for r in group)
        if amt <= 0:
            continue
        accounts.append({"esiid": es, "customer": deal.get("name") or "",
                         "deal_status": deal.get("status"), "amount": round(amt, 2)})
    if not accounts:
        return []
    total = round(sum(a["amount"] for a in accounts), 2)
    return [{
        "finding_type": "churned_still_paid",
        "severity": "medium",
        "title": f"{len(accounts)} canceled account(s) still being paid by {supplier_name}",
        "explanation": (
            f"In {_fmt_month(label)}, {supplier_name} paid ${total:,.2f} on {len(accounts)} "
            f"account(s) the CRM shows as inactive. Either the customers are actually still "
            f"active (update the CRM — possible win-back) or the provider will claw these back "
            f"later. Verify each account's real status."),
        "affected_count": len(accounts),
        "estimated_impact": 0.0,
        "details": {"accounts": sorted(accounts, key=lambda a: -a["amount"])},
        "fingerprint": f"churned_still_paid:{label}",
        "affected_esiids": [a["esiid"] for a in accounts],
    }]


def detect_clawback_anomalies(all_rows_by_esiid: dict, history: dict,
                              supplier_name: str, label: str) -> list:
    accounts = []
    for es, group in all_rows_by_esiid.items():
        neg = sum(r.get("amount") or 0 for r in group
                  if (r.get("amount") or 0) < 0 or r.get("row_type") == "clawback")
        if neg >= 0:
            continue
        prior_paid = sum(e["amount"] for e in (history.get(es) or []) if e["amount"] > 0)
        if prior_paid <= 0:
            reason = "clawed back but never paid in recent months"
        elif abs(neg) > prior_paid + 0.01:
            reason = f"clawback ${abs(neg):,.2f} exceeds recent payments ${prior_paid:,.2f}"
        else:
            continue
        accounts.append({"esiid": es, "clawback": round(neg, 2),
                         "recent_paid": round(prior_paid, 2), "reason": reason})
    if not accounts:
        return []
    total = round(sum(abs(a["clawback"]) for a in accounts), 2)
    return [{
        "finding_type": "clawback_anomaly",
        "severity": "high",
        "title": f"{len(accounts)} questionable clawback(s) from {supplier_name}",
        "explanation": (
            f"In {_fmt_month(label)}, {supplier_name} clawed back ${total:,.2f} across "
            f"{len(accounts)} account(s) where the clawback doesn't match what was recently "
            f"paid — either nothing was paid to claw back, or the clawback is larger than the "
            f"payments. Ask the provider for the clawback detail."),
        "affected_count": len(accounts),
        "estimated_impact": total,
        "details": {"accounts": sorted(accounts, key=lambda a: a["clawback"])},
        "fingerprint": f"clawback_anomaly:{label}",
        "affected_esiids": [a["esiid"] for a in accounts],
    }]


def detect_term_mismatch(deals_by_esiid: dict, rows_by_esiid: dict,
                         supplier_name: str, label: str) -> list:
    accounts = []
    for es, group in rows_by_esiid.items():
        deal = deals_by_esiid.get(es)
        if deal is None or _in_window(deal, label):
            continue
        amt = sum(r.get("amount") or 0 for r in group)
        accounts.append({"esiid": es, "customer": deal.get("name") or "",
                         "contract_start": deal.get("start"), "contract_end": deal.get("end"),
                         "amount": round(amt, 2)})
    if not accounts:
        return []
    return [{
        "finding_type": "term_mismatch",
        "severity": "medium",
        "title": f"{len(accounts)} account(s) paid outside their contract window",
        "explanation": (
            f"In {_fmt_month(label)}, {supplier_name} paid {len(accounts)} account(s) whose CRM "
            f"contract dates don't cover this month. Either the contract dates in the CRM are "
            f"wrong (fix them — they drive the missing-payment audit) or the customer renewed "
            f"without the CRM knowing (record the renewal)."),
        "affected_count": len(accounts),
        "estimated_impact": 0.0,
        "details": {"accounts": accounts},
        "fingerprint": f"term_mismatch:{label}",
        "affected_esiids": [a["esiid"] for a in accounts],
    }]


def detect_payment_stopped(history: dict, stmt_esiids: set, deals_by_esiid: dict,
                           supplier_name: str, label: str,
                           min_streak: int = STOPPED_MIN_STREAK) -> list:
    prev = _prev_labels(label, min_streak)
    accounts = []
    for es, deal in deals_by_esiid.items():
        if not deal.get("active") or es in stmt_esiids or not _in_window(deal, label):
            continue
        months_paid = {e["month"] for e in (history.get(es) or []) if e["amount"] > 0}
        if not all(m in months_paid for m in prev):
            continue
        last = max((e for e in history.get(es, []) if e["amount"] > 0),
                   key=lambda e: e["month"], default=None)
        accounts.append({"esiid": es, "customer": deal.get("name") or "",
                         "agent": deal.get("agent") or "",
                         "last_amount": round(last["amount"], 2) if last else None})
    if not accounts:
        return []
    est = round(sum(a["last_amount"] or 0 for a in accounts), 2)
    return [{
        "finding_type": "payment_stopped",
        "severity": "high",
        "title": f"{len(accounts)} account(s) stopped paying on {supplier_name}",
        "explanation": (
            f"{len(accounts)} active account(s) were paid in each of the last {min_streak} "
            f"months but are absent from the {_fmt_month(label)} statement — likely churned "
            f"customers or accounts the provider dropped. Based on their last payments this is "
            f"≈${est:,.2f}/month. Confirm each account's status with the provider; win back "
            f"churned customers."),
        "affected_count": len(accounts),
        "estimated_impact": est,
        "details": {"accounts": sorted(accounts, key=lambda a: -(a["last_amount"] or 0))},
        "fingerprint": f"payment_stopped:{label}",
        "affected_esiids": [a["esiid"] for a in accounts],
    }]


def detect_out_of_range(rows_by_esiid: dict, history: dict, supplier_name: str,
                        label: str, skip_esiids: set = None,
                        pct: float = OUT_OF_RANGE_PCT) -> list:
    skip_esiids = skip_esiids or set()
    accounts = []
    for es, group in rows_by_esiid.items():
        if es in skip_esiids:
            continue
        monthly: dict = {}
        for e in history.get(es) or []:
            monthly[e["month"]] = monthly.get(e["month"], 0.0) + e["amount"]
        vals = sorted(v for v in monthly.values() if v > 0)
        if len(vals) < 2:
            continue
        median = vals[len(vals) // 2]
        cur = sum(r.get("amount") or 0 for r in group)
        if median <= 0 or abs(cur - median) < OUT_OF_RANGE_MIN_DOLLARS:
            continue
        dev = (cur - median) / median * 100
        if abs(dev) < pct:
            continue
        accounts.append({"esiid": es, "amount": round(cur, 2), "typical": round(median, 2),
                        "deviation_pct": round(dev, 1)})
    if not accounts:
        return []
    return [{
        "finding_type": "out_of_range",
        "severity": "medium",
        "title": f"{len(accounts)} account(s) paid far outside their normal range",
        "explanation": (
            f"In {_fmt_month(label)}, {len(accounts)} account(s) were paid more than {pct:g}% "
            f"away from their typical month on {supplier_name}. Big drops can be missing usage "
            f"or partial payments; big jumps can be catch-up payments worth verifying."),
        "affected_count": len(accounts),
        "estimated_impact": round(sum(max(0.0, a["typical"] - a["amount"])
                                      for a in accounts), 2),
        "details": {"accounts": sorted(accounts, key=lambda a: a["deviation_pct"])},
        "fingerprint": f"out_of_range:{label}",
        "affected_esiids": [a["esiid"] for a in accounts],
    }]


def run_extended_audit(db, supplier_id: str, provider_group: str, label: str,
                       rows: list, deals: dict, run_id: str = None,
                       actor: str = "system") -> list:
    """Run every detector for one supplier+month, upsert audit_findings by
    fingerprint, stamp finding_id onto the run's items. Returns the findings."""
    by_esiid = deals["by_esiid"]
    comm_by_esiid: dict = {}
    all_by_esiid: dict = {}
    for r in rows:
        if r.get("statement_label") != label:
            continue
        all_by_esiid.setdefault(r["esiid"], []).append(r)
        if r.get("row_type", "commission") == "commission":
            comm_by_esiid.setdefault(r["esiid"], []).append(r)

    active_esiids = [es for es, d in by_esiid.items() if d.get("active")]
    esiids = sorted(set(comm_by_esiid) | set(active_esiids))
    history = build_rate_history(db, supplier_id, esiids, label)

    sup = db.table("suppliers").select("name").eq("id", supplier_id).limit(1).execute().data
    supplier_name = (sup[0]["name"] if sup else provider_group) or provider_group

    findings = []
    findings += detect_systemic_rate_change(comm_by_esiid, history, supplier_name, label)
    covered = {es for f in findings for es in f["affected_esiids"]}
    findings += detect_churned_still_paid(by_esiid, comm_by_esiid, supplier_name, label)
    findings += detect_clawback_anomalies(all_by_esiid, history, supplier_name, label)
    findings += detect_term_mismatch(by_esiid, comm_by_esiid, supplier_name, label)
    findings += detect_payment_stopped(history, set(comm_by_esiid), by_esiid,
                                       supplier_name, label)
    findings += detect_out_of_range(comm_by_esiid, history, supplier_name, label,
                                    skip_esiids=covered)

    now = datetime.now(timezone.utc).isoformat()
    saved = []
    fingerprints = set()
    for f in findings:
        esiids_affected = f.pop("affected_esiids", [])
        fp = f"{supplier_id}:{f['fingerprint']}"
        fingerprints.add(fp)
        record = {**f, "fingerprint": fp, "supplier_id": supplier_id,
                  "billing_month": f"{label}-01", "reconciliation_run_id": run_id,
                  "updated_at": now}
        existing = db.table("audit_findings").select("id,status") \
            .eq("fingerprint", fp).limit(1).execute().data or []
        if existing:
            db.table("audit_findings").update(record).eq("id", existing[0]["id"]).execute()
            record["id"], record["status"] = existing[0]["id"], existing[0]["status"]
        else:
            record["status"] = "open"
            record = db.table("audit_findings").insert(record).execute().data[0]
            audit(db, "audit_findings", record["id"], "finding_detected",
                  None, {"type": f["finding_type"], "title": f["title"],
                         "impact": f["estimated_impact"]},
                  reason=f["explanation"][:400], actor=actor)
        record["affected_esiids"] = esiids_affected
        saved.append(record)

        if run_id and esiids_affected:
            for i in range(0, len(esiids_affected), 100):
                db.table("reconciliation_items") \
                    .update({"finding_id": record["id"]}) \
                    .eq("reconciliation_run_id", run_id) \
                    .in_("esiid", esiids_affected[i:i + 100]).execute()

    # Findings for this month that no longer reproduce were fixed upstream —
    # close them so the exception center reflects reality.
    stale = db.table("audit_findings").select("id,fingerprint") \
        .eq("supplier_id", supplier_id).eq("billing_month", f"{label}-01") \
        .eq("status", "open").execute().data or []
    for s in stale:
        if s["fingerprint"] not in fingerprints:
            db.table("audit_findings").update({
                "status": "resolved", "resolved_at": now, "resolved_by": "system",
                "updated_at": now,
            }).eq("id", s["id"]).execute()

    return saved
