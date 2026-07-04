from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional, List
from pydantic import BaseModel
from app.db.client import get_client
from app.services.reconciliation_v2 import (
    rows_from_db, replace_prior_runs, run_reconciliation_v2, load_deals,
)
from app.services.file_parser.provider_parsers import PROVIDER_SUPPLIERS
from app.auth.deps import require_admin, UserContext

router = APIRouter()

@router.post("/run")
def trigger_reconciliation(billing_month: str, supplier_id: Optional[str] = None, user: UserContext = Depends(require_admin)):
    """Re-reconcile one month against imported statement rows, per provider.
    Replaces each provider's existing run for the month (resolutions carry over)."""
    db = get_client()
    label = billing_month[:7]
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
        results.append(run_reconciliation_v2(
            db, sup_id, group, label, rows,
            deals=deals, actor=user.email or "admin", carry_resolved=carry))
    if not results:
        raise HTTPException(status_code=404,
                            detail=f"No imported statement rows for {label}. Upload the statements first.")
    return {"billing_month": label, "runs": results}

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
