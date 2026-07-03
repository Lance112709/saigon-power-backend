"""Reconciliation engine v2 — runs against the live deal book.

Compares a parsed commission statement (one provider, one statement month)
against active deals in lead_deals + crm_deals and produces one
reconciliation_run with items explaining every discrepancy:

  missing     - active in-window deal absent from the statement
  short_paid  - statement rate below the deal's contracted adder (wrong mills)
  over_paid   - identical row appears more than once (duplicate payment)
  unexpected  - paid ESIID unknown to the CRM, or provider marked the account
                inactive/final while the CRM still shows it active
  matched     - everything else

Root causes are written into resolution_notes so the UI can show WHY.
All queries paginate past Supabase's 1000-row limit.
"""
import json
import re
from datetime import datetime, timezone

from app.services.file_parser.provider_parsers import CRM_PROVIDER_GROUPS, CHURN_KEYWORDS

RATE_TOLERANCE = 1e-6

ABBREV = {
    "STREET": "ST", "DRIVE": "DR", "LANE": "LN", "ROAD": "RD", "AVENUE": "AVE",
    "BOULEVARD": "BLVD", "COURT": "CT", "CIRCLE": "CIR", "PARKWAY": "PKWY",
    "HIGHWAY": "HWY", "PLACE": "PL", "TRAIL": "TRL", "TERRACE": "TER",
    "NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W", "FREEWAY": "FWY",
}
_UNIT_RE = re.compile(r"\b(APT|UNIT|STE|SUITE|BLDG|LOT|TRLR|#)\s*\.?\s*([\w-]+)\s*$", re.I)


def norm_addr(a) -> str:
    if not a or str(a).lower() in ("nan", "none"):
        return ""
    s = str(a).upper().strip()
    s = re.sub(r"[.,]", " ", s)
    m = _UNIT_RE.search(s)
    unit = ""
    if m:
        unit = m.group(2)
        s = s[:m.start()].strip()
    s = " ".join(ABBREV.get(w, w) for w in s.split())
    s = re.sub(r"\s+", " ", s).strip()
    return (s + (" #" + unit if unit else "")).strip()


def zip5(z) -> str:
    return re.sub(r"\D", "", str(z or ""))[:5]


def fetch_all(db, table: str, cols: str, filters=None):
    out, off = [], 0
    while True:
        q = db.table(table).select(cols)
        for fn, args in (filters or []):
            q = getattr(q, fn)(*args)
        r = q.range(off, off + 999).execute().data or []
        out.extend(r)
        if len(r) < 1000:
            break
        off += 1000
    return out


def load_deals(db, provider_group: str) -> dict:
    """All deals for a provider group, indexed for matching."""
    wanted = {k for k, v in CRM_PROVIDER_GROUPS.items() if v == provider_group}
    by_esiid = {}
    no_esiid = []

    for d in fetch_all(db, "lead_deals",
                       "id,lead_id,status,supplier,esiid,adder,est_kwh,start_date,end_date,"
                       "service_address,service_zip,sales_agent,leads(first_name,last_name,phone)"):
        if (d.get("supplier") or "").strip().lower() not in wanted:
            continue
        lead = d.get("leads") or {}
        deal = {
            "source": "lead_deals", "id": d["id"], "lead_id": d.get("lead_id"),
            "active": d.get("status") == "Active", "status": d.get("status"),
            "adder": float(d["adder"]) if d.get("adder") is not None else None,
            "est_kwh": float(d["est_kwh"]) if d.get("est_kwh") is not None else None,
            "start": d.get("start_date"), "end": d.get("end_date"),
            "esiid": re.sub(r"\D", "", d.get("esiid") or ""),
            "name": f"{lead.get('first_name', '')} {lead.get('last_name', '')}".strip(),
            "phone": lead.get("phone"), "agent": d.get("sales_agent") or "",
            "addr_n": norm_addr(d.get("service_address")), "zip5": zip5(d.get("service_zip")),
        }
        _index_deal(by_esiid, no_esiid, deal)

    for d in fetch_all(db, "crm_deals",
                       "id,customer_id,deal_status,provider,esiid,adder,meter_type,contract_start_date,"
                       "contract_end_date,service_address,sales_agent,business_name,"
                       "crm_customers(full_name,phone,postal_code)"):
        if (d.get("provider") or "").strip().lower() not in wanted:
            continue
        cust = d.get("crm_customers") or {}
        deal = {
            "source": "crm_deals", "id": d["id"], "lead_id": None,
            "active": d.get("deal_status") == "ACTIVE", "status": d.get("deal_status"),
            "adder": float(d["adder"]) if d.get("adder") is not None else None,
            "est_kwh": 2500.0 if d.get("meter_type") == "Commercial" else 1100.0,
            "start": d.get("contract_start_date"), "end": d.get("contract_end_date"),
            "esiid": re.sub(r"\D", "", d.get("esiid") or ""),
            "name": cust.get("full_name") or d.get("business_name") or "",
            "phone": cust.get("phone"), "agent": d.get("sales_agent") or "",
            "addr_n": norm_addr(d.get("service_address")), "zip5": zip5(cust.get("postal_code")),
        }
        _index_deal(by_esiid, no_esiid, deal)

    addr_index = {}
    for deal in no_esiid:
        if deal["addr_n"] and deal["active"]:
            addr_index.setdefault(deal["addr_n"], []).append(deal)
    return {"by_esiid": by_esiid, "no_esiid": no_esiid, "addr_index": addr_index}


def _index_deal(by_esiid, no_esiid, deal):
    es = deal["esiid"]
    if es:
        cur = by_esiid.get(es)
        if cur is None or (deal["active"] and not cur["active"]):
            by_esiid[es] = deal
    else:
        no_esiid.append(deal)


def backfill_esiids(db, deals: dict, rows: list, actor: str) -> list:
    """Fill missing deal ESIIDs when a statement row's address matches exactly
    one active deal without an ESIID. Every change is audit-logged."""
    from app.services.audit import audit
    filled = []
    seen = set()
    for r in rows:
        addr = norm_addr(r.get("address"))
        if not addr or r["esiid"] in seen:
            continue
        seen.add(r["esiid"])
        if r["esiid"] in deals["by_esiid"]:
            continue
        candidates = deals["addr_index"].get(addr) or []
        if len(candidates) != 1:
            continue
        deal = candidates[0]
        if deal["zip5"] and zip5(r.get("zip")) and deal["zip5"] != zip5(r.get("zip")):
            continue
        table = deal["source"]
        db.table(table).update({"esiid": r["esiid"]}).eq("id", deal["id"]).execute()
        audit(db, table, deal["id"], "esiid_backfill", {"esiid": None}, {"esiid": r["esiid"]},
              reason=f"Statement address match: {addr}", actor=actor)
        deal["esiid"] = r["esiid"]
        deals["by_esiid"][r["esiid"]] = deal
        deals["addr_index"].pop(addr, None)
        filled.append({"esiid": r["esiid"], "deal_id": deal["id"], "source": table,
                       "customer": deal["name"], "address": r.get("address", "")})
    return filled


def _in_window(deal, label: str) -> bool:
    """Deal's contract window covers the statement month (with grace periods:
    starts ≥1 month before statement, ended no more than 2 months before)."""
    month_start = f"{label}-01"
    if deal.get("start"):
        y, m = int(label[:4]), int(label[5:7])
        m -= 1
        if m < 1:
            y, m = y - 1, 12
        if str(deal["start"])[:10] > f"{y}-{m:02d}-28":
            return False
    if deal.get("end"):
        y, m = int(label[:4]), int(label[5:7])
        m -= 2
        if m < 1:
            y, m = y + (m - 1) // 12, ((m - 1) % 12) + 1
        if str(deal["end"])[:10] < f"{y}-{m:02d}-01":
            return False
    return True


def _prev_labels(label: str, n: int = 2):
    y, m = int(label[:4]), int(label[5:7])
    out = []
    for _ in range(n):
        m -= 1
        if m < 1:
            y, m = y - 1, 12
        out.append(f"{y}-{m:02d}")
    return out


def _sev(pct: float) -> str:
    pct = abs(pct)
    if pct < 2:
        return "low"
    if pct < 10:
        return "medium"
    if pct < 25:
        return "high"
    return "critical"


def rows_from_db(db, supplier_id: str, label: str) -> list:
    """Rebuild normalized rows from every imported batch for a supplier+month,
    so supplemental statements merge into one reconciliation.

    Identical rows (esiid + service period + amount) appearing in more than
    one BATCH are the same payment re-listed (e.g. a cumulative report
    overlapping a monthly one) — keep only the batch that has the most copies
    of that row, so genuine within-statement duplicates still surface."""
    recs = fetch_all(db, "actual_commissions",
                     "upload_batch_id,raw_esiid,raw_customer_name,raw_amount,raw_kwh,raw_rate,raw_row_data",
                     filters=[("eq", ("supplier_id", supplier_id)),
                              ("eq", ("billing_month", f"{label}-01"))])

    per_key_batch = {}
    for r in recs:
        norm = (r.get("raw_row_data") or {}).get("_norm") or {}
        key = (r["raw_esiid"], norm.get("service_start"), norm.get("service_end"),
               float(r["raw_amount"]) if r.get("raw_amount") is not None else 0.0)
        per_key_batch.setdefault(key, {}).setdefault(r["upload_batch_id"], []).append(r)

    kept = []
    for key, batches in per_key_batch.items():
        best = max(batches.values(), key=len)
        kept.extend(best)

    rows = []
    for r in kept:
        norm = (r.get("raw_row_data") or {}).get("_norm") or {}
        rows.append({
            "esiid": r["raw_esiid"], "customer_name": r.get("raw_customer_name") or "",
            "address": "", "city": "", "zip": "",
            "usage_kwh": float(r["raw_kwh"]) if r.get("raw_kwh") is not None else None,
            "rate": float(r["raw_rate"]) if r.get("raw_rate") is not None else None,
            "amount": float(r["raw_amount"]) if r.get("raw_amount") is not None else 0.0,
            "service_start": norm.get("service_start"), "service_end": norm.get("service_end"),
            "provider_status": norm.get("provider_status") or "",
            "row_type": norm.get("row_type") or "commission",
            "statement_label": label, "raw": {},
        })
    return rows


def replace_prior_runs(db, supplier_id: str, label: str) -> dict:
    """Delete earlier v2 runs for this supplier+month; return resolved items
    keyed by (esiid, status) so resolutions survive the re-run."""
    carry = {}
    runs = db.table("reconciliation_runs").select("id,notes") \
        .eq("supplier_id", supplier_id).eq("billing_month", f"{label}-01") \
        .like("notes", '%"engine": "v2"%').execute().data or []
    for run in runs:
        items = fetch_all(db, "reconciliation_items", "esiid,status,is_resolved,resolution_notes",
                          filters=[("eq", ("reconciliation_run_id", run["id"])),
                                   ("eq", ("is_resolved", True))])
        for it in items:
            if it.get("status") != "matched":
                carry[(it["esiid"], it["status"])] = it.get("resolution_notes") or ""
        db.table("reconciliation_runs").delete().eq("id", run["id"]).execute()  # items cascade
    return carry


def run_reconciliation_v2(db, supplier_id: str, provider_group: str, label: str,
                          rows: list, batch_id: str = None, deals: dict = None,
                          actor: str = "system", carry_resolved: dict = None) -> dict:
    """Reconcile one statement month of parsed rows. Returns run summary."""
    if deals is None:
        deals = load_deals(db, provider_group)
    carry_resolved = carry_resolved or {}
    by_esiid = deals["by_esiid"]

    comm = [r for r in rows if r.get("row_type", "commission") == "commission"
            and r.get("statement_label") == label]
    stmt_esiids = {r["esiid"] for r in comm}

    # When most of a statement is marked churned, the provider's status column
    # is unreliable that month (e.g. Budget Power flagged 94% of accounts
    # "Inactive" in Apr 2026) — suppress per-account churn conflicts and note
    # it once on the run instead.
    churn_rows = sum(1 for r in comm if r.get("provider_status")
                     and any(k in r["provider_status"].lower() for k in CHURN_KEYWORDS))
    status_reliable = not comm or (churn_rows / len(comm)) <= 0.5

    items = []
    totals = {"expected": 0.0, "actual": 0.0, "matched": 0, "short_paid": 0,
              "over_paid": 0, "missing": 0, "unexpected": 0}

    # group rows per esiid
    per_esiid = {}
    for r in comm:
        per_esiid.setdefault(r["esiid"], []).append(r)

    for es, group in per_esiid.items():
        deal = by_esiid.get(es)
        actual = sum(r["amount"] or 0 for r in group)
        totals["actual"] += actual

        # duplicates: identical (service period, amount) appearing 2+ times
        sig_count = {}
        for r in group:
            sig = (r.get("service_start"), r.get("service_end"), r.get("amount"))
            sig_count[sig] = sig_count.get(sig, 0) + 1
        dup_extra = sum((c - 1) * (sig[2] or 0) for sig, c in sig_count.items() if c > 1)

        # rate check
        rate_loss, bad_rates = 0.0, set()
        if deal and deal.get("adder") is not None:
            for r in group:
                if r.get("rate") is not None and abs(r["rate"] - deal["adder"]) > RATE_TOLERANCE:
                    bad_rates.add(f"{r['rate']:g}")
                    if r["rate"] < deal["adder"] and r.get("usage_kwh"):
                        rate_loss += (deal["adder"] - r["rate"]) * r["usage_kwh"]

        # churn status conflict (only when the status column is trustworthy)
        churn_status = ""
        if status_reliable:
            churn_status = next((r["provider_status"] for r in group if r.get("provider_status")
                                 and any(k in r["provider_status"].lower() for k in CHURN_KEYWORDS)), "")

        expected = actual + rate_loss - dup_extra
        totals["expected"] += expected

        name = (deal["name"] if deal else "") or group[0].get("customer_name", "")
        pct = (actual - expected) / expected * 100 if expected else 0.0
        pct = max(-9999.0, min(9999.0, pct))  # column is NUMERIC(8,4)
        base = {
            "esiid": es, "supplier_id": supplier_id, "billing_month": f"{label}-01",
            "actual_amount": round(actual, 4), "expected_amount": round(expected, 4),
            "discrepancy_amount": round(actual - expected, 4),
            "discrepancy_percentage": round(pct, 2),
        }

        if deal is None:
            totals["unexpected"] += 1
            items.append({**base, "status": "unexpected", "severity": "low",
                          "resolution_notes": f"ROOT CAUSE: ESIID paid by {provider_group} but not found in CRM "
                                              f"(statement name: {name}). Add the deal or link this ESIID."})
        elif dup_extra > 0:
            totals["over_paid"] += 1
            items.append({**base, "status": "over_paid", "severity": "medium",
                          "resolution_notes": f"ROOT CAUSE: duplicate payment — identical service period and amount "
                                              f"appears more than once (extra ${dup_extra:.2f}). Customer: {name}."})
        elif bad_rates:
            totals["short_paid"] += 1
            pct = (rate_loss / expected * 100) if expected else 100
            items.append({**base, "status": "short_paid", "severity": _sev(pct),
                          "resolution_notes": f"ROOT CAUSE: wrong commission rate — contract adder is "
                                              f"{deal['adder']:g} but statement paid {', '.join(sorted(bad_rates))}. "
                                              f"Underpaid ${rate_loss:.2f} this month. Customer: {name}. "
                                              f"Verify contract; request true-up from provider."})
        elif churn_status:
            totals["unexpected"] += 1
            items.append({**base, "status": "unexpected", "severity": "high",
                          "resolution_notes": f"ROOT CAUSE: provider reports status '{churn_status}' but CRM deal is "
                                              f"{deal['status']}. Customer {name} is leaving or gone — update CRM, "
                                              f"contact for win-back/renewal."})
        else:
            totals["matched"] += 1
            items.append({**base, "status": "matched", "severity": "low", "resolution_notes": "",
                          "is_resolved": True})

    # completeness: in-window active deals absent from this statement
    missing_deals = [d for es, d in by_esiid.items()
                     if d["active"] and es not in stmt_esiids and _in_window(d, label)]
    prev_paid = set()
    if missing_deals:
        esiids = [d["esiid"] for d in missing_deals]
        for i in range(0, len(esiids), 100):
            hist = db.table("actual_commissions").select("raw_esiid") \
                .eq("supplier_id", supplier_id) \
                .in_("billing_month", [f"{p}-01" for p in _prev_labels(label)]) \
                .in_("raw_esiid", esiids[i:i + 100]).execute().data or []
            prev_paid.update(h["raw_esiid"] for h in hist)

    for d in missing_deals:
        est = round((d["adder"] or 0) * (d["est_kwh"] or 0), 2) or None
        was_paid = d["esiid"] in prev_paid
        totals["missing"] += 1
        if est:
            totals["expected"] += est
        items.append({
            "esiid": d["esiid"], "supplier_id": supplier_id, "billing_month": f"{label}-01",
            "expected_amount": est, "actual_amount": None,
            "discrepancy_amount": -(est or 0), "discrepancy_percentage": -100.0,
            "status": "missing", "severity": "critical" if was_paid else "high",
            "resolution_notes": (f"ROOT CAUSE: active deal not on this statement. Customer: {d['name']} "
                                 f"(agent {d['agent'] or 'n/a'}), contract {d['start']} → {d['end'] or 'open'}. "
                                 + ("Paid in a recent month then stopped — likely churned or provider dropped it; "
                                    "check with provider." if was_paid else
                                    "Never seen on any statement — verify enrollment with provider.")),
        })

    run = db.table("reconciliation_runs").insert({
        "billing_month": f"{label}-01",
        "supplier_id": supplier_id,
        "total_expected": round(totals["expected"], 2),
        "total_actual": round(totals["actual"], 2),
        "total_discrepancy": round(totals["actual"] - totals["expected"], 2),
        "matched_count": totals["matched"],
        "short_paid_count": totals["short_paid"],
        "over_paid_count": totals["over_paid"],
        "missing_count": totals["missing"],
        "unexpected_count": totals["unexpected"],
        "notes": json.dumps({"engine": "v2", "provider_group": provider_group,
                             "upload_batch_id": batch_id, "run_by": actor,
                             **({} if status_reliable else
                                {"status_column_unreliable":
                                 f"{churn_rows}/{len(comm)} rows marked churned — provider status ignored this month"})}),
    }).execute().data[0]

    now = datetime.now(timezone.utc).isoformat()
    for it in items:
        it["reconciliation_run_id"] = run["id"]
        it.setdefault("is_resolved", False)
        prior_note = carry_resolved.get((it["esiid"], it["status"]))
        if prior_note is not None and not it["is_resolved"]:
            it["is_resolved"] = True
            it["resolved_at"] = now
            if prior_note:
                it["resolution_notes"] = prior_note
    for i in range(0, len(items), 200):
        db.table("reconciliation_items").insert(items[i:i + 200]).execute()

    return {
        "run_id": run["id"], "billing_month": label, "provider_group": provider_group,
        "total_expected": round(totals["expected"], 2),
        "total_actual": round(totals["actual"], 2),
        "total_discrepancy": round(totals["actual"] - totals["expected"], 2),
        "matched": totals["matched"], "short_paid": totals["short_paid"],
        "over_paid": totals["over_paid"], "missing": totals["missing"],
        "unexpected": totals["unexpected"], "items_count": len(items),
    }


def get_or_create_supplier(db, supplier_def: dict) -> str:
    """Find supplier by code (or create it) and return its id."""
    r = db.table("suppliers").select("id").eq("code", supplier_def["code"]).limit(1).execute().data
    if r:
        return r[0]["id"]
    ins = db.table("suppliers").insert({
        "name": supplier_def["name"], "code": supplier_def["code"], "is_active": True,
        "notes": "Auto-created by statement import",
    }).execute().data
    return ins[0]["id"]
