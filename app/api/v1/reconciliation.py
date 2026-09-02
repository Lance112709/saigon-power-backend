from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional, List
from pydantic import BaseModel
from app.db.client import get_client
from app.services.reconciliation_v2 import (
    rows_from_db, replace_prior_runs, run_reconciliation_v2, load_deals, fetch_all,
)
from app.services.file_parser.provider_parsers import PROVIDER_SUPPLIERS
from app.services.commission_rules import get_rule_for_month
from app.services.commission_snapshots import snapshot_history
from app.services.audit_detections import run_extended_audit
from app.services.exception_cases import list_cases, update_case, upsert_cases_from_run
from app.auth.deps import require_admin, UserContext

router = APIRouter()

@router.post("/run")
def trigger_reconciliation(billing_month: str, supplier_id: Optional[str] = None, user: UserContext = Depends(require_admin)):
    """Re-reconcile one month against imported statement rows, per provider.
    Replaces each provider's existing run for the month (resolutions carry over).
    Also re-runs the extended audit (systemic findings) and syncs exception
    cases — this endpoint is the idempotent repair path."""
    db = get_client()
    label = billing_month[:7]
    actor = user.email or "admin"
    results = []
    for group, sdef in PROVIDER_SUPPLIERS.items():
        sup = db.table("suppliers").select("id").eq("code", sdef["code"]).limit(1).execute().data
        if not sup:
            continue
        sup_id = sup[0]["id"]
        if supplier_id and sup_id != supplier_id:
            continue
        rows = rows_from_db(db, sup_id, label)
        if not rows:
            continue
        carry = replace_prior_runs(db, sup_id, label)
        deals = load_deals(db, group)
        rule = None
        try:
            rule = get_rule_for_month(db, sup_id, label)
        except Exception:
            pass  # rules table not created yet — plain adder math applies
        summary = run_reconciliation_v2(
            db, sup_id, group, label, rows,
            deals=deals, actor=actor, carry_resolved=carry, rule=rule)
        findings = []
        try:
            findings = run_extended_audit(db, sup_id, group, label, rows, deals,
                                          run_id=summary["run_id"], actor=actor)
            summary["findings"] = [{"id": f["id"], "type": f["finding_type"],
                                    "title": f["title"],
                                    "impact": f["estimated_impact"]} for f in findings]
        except Exception as e:
            summary["findings_error"] = str(e)[:200]
        summary["cases"] = upsert_cases_from_run(db, summary["run_id"], sup_id, label,
                                                 deals=deals, actor=actor)
        # Alert on wrong rates / big misses from manual re-runs too, not just imports
        try:
            from app.services.audit_notifications import notify_big_findings
            notify_big_findings(db, group, [summary], findings)
        except Exception:
            pass
        results.append(summary)
    if not results:
        raise HTTPException(status_code=404,
                            detail=f"No imported statement rows for {label}. Upload the statements first.")
    return {"billing_month": label, "runs": results}

# ── Payments received (REP → SGP) ─────────────────────────────────────────────

_MONTH_RE = r"20\d{2}-(0[1-9]|1[0-2])"


def _payments_received(db, month_from: Optional[str], month_to: Optional[str],
                       providers: Optional[str]):
    """Provider payments to SGP by month, from the reconciliation ledger
    (reconciliation_runs.total_actual = every statement row imported for that
    supplier+month). Filters: month_from/month_to = YYYY-MM inclusive,
    providers = comma-separated supplier names."""
    import re
    for v in (month_from, month_to):
        if v and not re.fullmatch(_MONTH_RE, v):
            raise HTTPException(status_code=400, detail="months must be YYYY-MM")
    runs = db.table("reconciliation_runs").select(
        "billing_month,total_actual,total_expected,matched_count,suppliers(name,code)"
    ).order("billing_month").execute().data or []
    wanted = {p.strip() for p in providers.split(",") if p.strip()} if providers else None
    first = runs[0]["billing_month"][:7] if runs else None
    last = runs[-1]["billing_month"][:7] if runs else None

    cells: dict = {}
    for r in runs:
        m = r["billing_month"][:7]
        if month_from and m < month_from:
            continue
        if month_to and m > month_to:
            continue
        name = (r.get("suppliers") or {}).get("name") or "Unknown"
        if wanted and name not in wanted:
            continue
        c = cells.setdefault((m, name), {"month": m, "provider": name, "paid": 0.0, "expected": 0.0, "accounts": 0})
        c["paid"] += r.get("total_actual") or 0
        c["expected"] += r.get("total_expected") or 0
        c["accounts"] += r.get("matched_count") or 0

    months = sorted({k[0] for k in cells})
    by_provider: dict = {}
    for c in cells.values():
        p = by_provider.setdefault(c["provider"], {"provider": c["provider"], "paid": 0.0, "expected": 0.0, "months": 0})
        p["paid"] += c["paid"]; p["expected"] += c["expected"]; p["months"] += 1
    prov_list = sorted(by_provider.values(), key=lambda x: -x["paid"])
    for p in prov_list:
        p["paid"] = round(p["paid"], 2); p["expected"] = round(p["expected"], 2)
    by_month = []
    for m in months:
        row = {"month": m, "total": 0.0, "expected": 0.0}
        for c in cells.values():
            if c["month"] == m:
                row[c["provider"]] = round(c["paid"], 2)
                row["total"] += c["paid"]; row["expected"] += c["expected"]
        row["total"] = round(row["total"], 2); row["expected"] = round(row["expected"], 2)
        by_month.append(row)
    total = round(sum(p["paid"] for p in prov_list), 2)
    return {
        "range": {"from": month_from or first, "to": month_to or last},
        "available": {"first": first, "last": last,
                      "providers": sorted({(r.get("suppliers") or {}).get("name") or "Unknown" for r in runs})},
        "total_paid": total,
        "total_expected": round(sum(p["expected"] for p in prov_list), 2),
        "months": months,
        "avg_per_month": round(total / len(months), 2) if months else 0,
        "by_provider": prov_list,
        "by_month": by_month,
        "cells": [{**c, "paid": round(c["paid"], 2), "expected": round(c["expected"], 2)} for c in sorted(cells.values(), key=lambda c: (c["month"], c["provider"]))],
    }


@router.get("/payments-received")
def payments_received(month_from: Optional[str] = Query(None, alias="from"),
                      month_to: Optional[str] = Query(None, alias="to"),
                      providers: Optional[str] = Query(None),
                      user: UserContext = Depends(require_admin)):
    """Total $ each REP paid SGP, by month and provider, for any month range."""
    return _payments_received(get_client(), month_from, month_to, providers)


@router.get("/payments-received/export")
def payments_received_export(month_from: Optional[str] = Query(None, alias="from"),
                             month_to: Optional[str] = Query(None, alias="to"),
                             providers: Optional[str] = Query(None),
                             user: UserContext = Depends(require_admin)):
    """Excel: Summary (by provider), By Month (month × provider matrix), Detail."""
    import io
    import pandas as pd
    from fastapi.responses import StreamingResponse
    data = _payments_received(get_client(), month_from, month_to, providers)
    rng = f"{data['range']['from'] or ''} to {data['range']['to'] or ''}"
    summary = pd.DataFrame([{"Provider": p["provider"], "Paid to SGP $": p["paid"], "Expected $": p["expected"],
                             "Months with payments": p["months"]} for p in data["by_provider"]]
                           + [{"Provider": "TOTAL", "Paid to SGP $": data["total_paid"], "Expected $": data["total_expected"],
                               "Months with payments": len(data["months"])}])
    prov_names = [p["provider"] for p in data["by_provider"]]
    matrix = pd.DataFrame([{"Month": r["month"], **{p: r.get(p, 0.0) for p in prov_names},
                            "Total": r["total"]} for r in data["by_month"]]
                          + ([{"Month": "TOTAL", **{p["provider"]: p["paid"] for p in data["by_provider"]},
                               "Total": data["total_paid"]}] if data["by_month"] else []))
    detail = pd.DataFrame([{"Month": c["month"], "Provider": c["provider"], "Paid to SGP $": c["paid"],
                            "Expected $": c["expected"], "Accounts paid": c["accounts"]} for c in data["cells"]])
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        pd.DataFrame([{"Payments received by SGP": rng, "Total": data["total_paid"]}]).to_excel(w, sheet_name="Summary", index=False, startrow=0)
        summary.to_excel(w, sheet_name="Summary", index=False, startrow=3)
        (matrix if len(matrix) else pd.DataFrame(columns=["Month"])).to_excel(w, sheet_name="By Month", index=False)
        (detail if len(detail) else pd.DataFrame(columns=["Month"])).to_excel(w, sheet_name="Detail", index=False)
        for ws in w.book.worksheets:
            for col in ws.columns:
                ws.column_dimensions[col[0].column_letter].width = min(28, max(12, max((len(str(c.value)) if c.value is not None else 0) for c in col) + 2))
            ws.freeze_panes = "A2" if ws.title != "Summary" else "A5"
    buf.seek(0)
    fname = f"payments-received_{(data['range']['from'] or 'all')}_{(data['range']['to'] or 'all')}.xlsx"
    return StreamingResponse(buf, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers={"Content-Disposition": f'attachment; filename="{fname}"'})


@router.get("/runs")
def list_runs(billing_month: Optional[str] = Query(None), user: UserContext = Depends(require_admin)):
    db = get_client()
    q = db.table("reconciliation_runs").select("*, suppliers(name, code)").order("run_at", desc=True)
    if billing_month:
        q = q.eq("billing_month", billing_month)
    return q.execute().data

@router.get("/runs/{id}")
def get_run(id: str, user: UserContext = Depends(require_admin)):
    db = get_client()
    run = db.table("reconciliation_runs").select("*, suppliers(name, code)").eq("id", id).single().execute()
    if not run.data:
        raise HTTPException(status_code=404, detail="Run not found")
    return run.data

def _fetch_run_items(db, run_id: str, status=None, severity=None, is_resolved=None) -> list:
    """All items for a run, paginated past the 1000-row PostgREST cap."""
    out, off = [], 0
    while True:
        q = db.table("reconciliation_items").select("*, suppliers(name, code)") \
            .eq("reconciliation_run_id", run_id).order("severity").order("id")
        if status:
            q = q.eq("status", status)
        if severity:
            q = q.eq("severity", severity)
        if is_resolved is not None:
            q = q.eq("is_resolved", is_resolved)
        page = q.range(off, off + 999).execute().data or []
        out.extend(page)
        if len(page) < 1000:
            break
        off += 1000
    return out


@router.get("/runs/{id}/items")
def get_run_items(
    id: str,
    status: Optional[str] = Query(None),
    severity: Optional[str] = Query(None),
    is_resolved: Optional[bool] = Query(None),
    user: UserContext = Depends(require_admin)
):
    db = get_client()
    return _fetch_run_items(db, id, status, severity, is_resolved)


STATUS_EXPORT_LABELS = {
    "missing": "Missing payment", "short_paid": "Wrong rate",
    "over_paid": "Duplicate", "unexpected": "Needs review", "matched": "Matched",
}


def _items_export_frame(items: list, run: dict):
    import pandas as pd
    rows = []
    for it in items:
        rows.append({
            "Provider": (it.get("suppliers") or {}).get("name") or (run.get("suppliers") or {}).get("name", ""),
            "Statement Month": (it.get("billing_month") or "")[:7],
            "ESI ID": it.get("esiid"),
            "Issue": STATUS_EXPORT_LABELS.get(it.get("status"), it.get("status")),
            "Severity": it.get("severity"),
            "Expected $": it.get("expected_amount"),
            "Received $": it.get("actual_amount"),
            "Difference $": it.get("discrepancy_amount"),
            "Explanation": (it.get("resolution_notes") or "").replace("ROOT CAUSE: ", ""),
            "Resolved": "Yes" if it.get("is_resolved") else "No",
        })
    return pd.DataFrame(rows)


@router.get("/runs/{id}/export")
def export_run(id: str, format: str = Query("xlsx"), user: UserContext = Depends(require_admin)):
    """Download one reconciliation run as Excel or CSV."""
    import io
    import pandas as pd
    from fastapi.responses import StreamingResponse

    db = get_client()
    run = db.table("reconciliation_runs").select("*, suppliers(name, code)").eq("id", id).single().execute().data
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    items = _fetch_run_items(db, id)
    df = _items_export_frame(items, run)

    sup = (run.get("suppliers") or {}).get("code", "run")
    month = (run.get("billing_month") or "")[:7]
    base = f"reconciliation_{sup}_{month}"

    if format == "csv":
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv",
                                 headers={"Content-Disposition": f'attachment; filename="{base}.csv"'})

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        summary = pd.DataFrame([{
            "Provider": (run.get("suppliers") or {}).get("name"),
            "Statement Month": month,
            "Expected $": run.get("total_expected"),
            "Received $": run.get("total_actual"),
            "Difference $": run.get("total_discrepancy"),
            "Matched": run.get("matched_count"),
            "Missing": run.get("missing_count"),
            "Wrong rate": run.get("short_paid_count"),
            "Duplicates": run.get("over_paid_count"),
            "Needs review": run.get("unexpected_count"),
        }])
        summary.to_excel(w, sheet_name="Summary", index=False)
        df.to_excel(w, sheet_name="Items", index=False)
    buf.seek(0)
    return StreamingResponse(
        buf, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{base}.xlsx"'})


@router.get("/export")
def export_range(
    start: str = Query(..., description="YYYY-MM"),
    end: str = Query(..., description="YYYY-MM"),
    format: str = Query("xlsx"),
    user: UserContext = Depends(require_admin),
):
    """Download all providers' reconciliation results for a month range.
    Excel: Summary sheet (one row per provider-month) + open Issues sheet."""
    import io
    import pandas as pd
    from fastapi.responses import StreamingResponse

    db = get_client()
    runs = db.table("reconciliation_runs").select("*, suppliers(name, code)") \
        .like("notes", '%"engine": "v2"%') \
        .gte("billing_month", f"{start}-01").lte("billing_month", f"{end}-01") \
        .order("billing_month").limit(1000).execute().data or []
    if not runs:
        raise HTTPException(status_code=404, detail=f"No reconciliation runs between {start} and {end}.")

    summary = pd.DataFrame([{
        "Statement Month": r["billing_month"][:7],
        "Provider": (r.get("suppliers") or {}).get("name"),
        "Expected $": r.get("total_expected"),
        "Received $": r.get("total_actual"),
        "Difference $": r.get("total_discrepancy"),
        "Matched": r.get("matched_count"),
        "Missing": r.get("missing_count"),
        "Wrong rate": r.get("short_paid_count"),
        "Duplicates": r.get("over_paid_count"),
        "Needs review": r.get("unexpected_count"),
    } for r in runs])

    if format == "csv":
        buf = io.StringIO()
        summary.to_csv(buf, index=False)
        return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv",
                                 headers={"Content-Disposition": f'attachment; filename="reconciliation_{start}_{end}.csv"'})

    issues = []
    for r in runs:
        for it in _fetch_run_items(db, r["id"], is_resolved=False):
            if it.get("status") in ("missing", "short_paid", "over_paid"):
                issues.append(it)
    issues_df = _items_export_frame(issues, {}) if issues else pd.DataFrame(
        columns=["Provider", "Statement Month", "ESI ID", "Issue", "Severity",
                 "Expected $", "Received $", "Difference $", "Explanation", "Resolved"])

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        summary.to_excel(w, sheet_name="Monthly Summary", index=False)
        issues_df.to_excel(w, sheet_name="Open Issues", index=False)
    buf.seek(0)
    return StreamingResponse(
        buf, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="reconciliation_{start}_{end}.xlsx"'})

class BulkResolveBody(BaseModel):
    item_ids: List[str]
    resolution_notes: str = ""

@router.post("/items/bulk-resolve")
def bulk_resolve_items(body: BulkResolveBody, user: UserContext = Depends(require_admin)):
    db = get_client()
    from datetime import datetime
    now = datetime.utcnow().isoformat()
    updated = 0
    for item_id in body.item_ids:
        db.table("reconciliation_items").update({
            "is_resolved": True,
            "resolution_notes": body.resolution_notes,
            "resolved_at": now,
        }).eq("id", item_id).execute()
        updated += 1
    return {"resolved": updated}

@router.patch("/items/{id}")
def resolve_item(id: str, resolution_notes: str = "", is_resolved: bool = True, user: UserContext = Depends(require_admin)):
    db = get_client()
    from datetime import datetime
    res = db.table("reconciliation_items").update({
        "is_resolved": is_resolved,
        "resolution_notes": resolution_notes,
        "resolved_at": datetime.utcnow().isoformat() if is_resolved else None
    }).eq("id", id).execute()
    return res.data[0]


# ---- Commission Intelligence: findings, cases, snapshots -------------------

def _supplier_names(db) -> dict:
    rows = db.table("suppliers").select("id,name,code").limit(500).execute().data or []
    return {r["id"]: {"name": r["name"], "code": r.get("code")} for r in rows}


@router.get("/findings")
def list_findings(
    billing_month: Optional[str] = Query(None),
    supplier_id: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    user: UserContext = Depends(require_admin),
):
    """Grouped audit findings (systemic rate cuts, stopped payments, ...)."""
    db = get_client()
    filters = []
    if billing_month:
        filters.append(("eq", ("billing_month", f"{billing_month[:7]}-01")))
    if supplier_id:
        filters.append(("eq", ("supplier_id", supplier_id)))
    if status:
        filters.append(("eq", ("status", status)))
    rows = fetch_all(db, "audit_findings", "*", filters=filters)
    rows.sort(key=lambda f: (-(f.get("estimated_impact") or 0), f.get("billing_month") or ""))
    sups = _supplier_names(db)
    try:
        from app.services.recovery_stats import provider_recovery_stats, recovery_hint
        stats = provider_recovery_stats(db)
    except Exception:
        stats = {}
    for r in rows:
        r["supplier"] = sups.get(r["supplier_id"], {})
        r["recovery_hint"] = recovery_hint(stats, r["supplier_id"]) if stats else None
    return rows[:500]


class FindingPatch(BaseModel):
    status: Optional[str] = None
    notes: Optional[str] = None


@router.patch("/findings/{id}")
def update_finding(id: str, body: FindingPatch, user: UserContext = Depends(require_admin)):
    db = get_client()
    from datetime import datetime, timezone
    from app.services.audit import audit
    rows = db.table("audit_findings").select("id,status").eq("id", id).limit(1).execute().data
    if not rows:
        raise HTTPException(status_code=404, detail="Finding not found")
    now = datetime.now(timezone.utc).isoformat()
    fields = {"updated_at": now}
    if body.status:
        if body.status not in ("open", "investigating", "disputed", "resolved", "dismissed"):
            raise HTTPException(status_code=422, detail="Invalid status")
        fields["status"] = body.status
        if body.status in ("resolved", "dismissed"):
            fields["resolved_at"] = now
            fields["resolved_by"] = user.email or "admin"
    res = db.table("audit_findings").update(fields).eq("id", id).execute().data[0]
    audit(db, "audit_findings", id, "finding_updated", {"status": rows[0]["status"]},
          {"status": res.get("status")}, reason=body.notes or "", actor=user.email or "admin")
    return res


@router.get("/cases")
def get_cases(
    workflow_status: Optional[str] = Query(None, description="one status, or 'any_open'"),
    supplier_id: Optional[str] = Query(None),
    billing_month: Optional[str] = Query(None),
    issue_type: Optional[str] = Query(None),
    min_loss: Optional[float] = Query(None),
    user: UserContext = Depends(require_admin),
):
    """Durable exception cases, priority-sorted."""
    db = get_client()
    rows = list_cases(db, workflow_status=workflow_status, supplier_id=supplier_id,
                      billing_month=billing_month, issue_type=issue_type,
                      min_loss=min_loss)
    sups = _supplier_names(db)
    try:
        from app.services.recovery_stats import provider_recovery_stats, recovery_hint
        stats = provider_recovery_stats(db)
    except Exception:
        stats = {}
    for r in rows:
        r["supplier"] = sups.get(r["supplier_id"], {})
        r["recovery_hint"] = recovery_hint(stats, r["supplier_id"]) if stats else None
    return rows


class CasePatch(BaseModel):
    workflow_status: Optional[str] = None
    notes: Optional[str] = None
    recovered_amount: Optional[float] = None


@router.patch("/cases/{id}")
def patch_case(id: str, body: CasePatch, user: UserContext = Depends(require_admin)):
    db = get_client()
    patch = {k: v for k, v in body.model_dump().items() if v is not None}
    if not patch:
        raise HTTPException(status_code=422, detail="Nothing to update")
    try:
        return update_case(db, id, patch, user.email or "admin")
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))


class BulkCaseBody(BaseModel):
    case_ids: List[str]
    workflow_status: str
    notes: str = ""


@router.post("/cases/bulk-update")
def bulk_update_cases(body: BulkCaseBody, user: UserContext = Depends(require_admin)):
    db = get_client()
    updated = 0
    for cid in body.case_ids:
        try:
            update_case(db, cid, {"workflow_status": body.workflow_status,
                                  **({"notes": body.notes} if body.notes else {})},
                        user.email or "admin")
            updated += 1
        except ValueError:
            continue
    return {"updated": updated}


@router.get("/snapshots")
def get_snapshots(esiid: str = Query(...), supplier_id: Optional[str] = Query(None),
                  user: UserContext = Depends(require_admin)):
    """Permanent expected-vs-paid history for one account (newest first)."""
    db = get_client()
    return snapshot_history(db, esiid, supplier_id=supplier_id)
