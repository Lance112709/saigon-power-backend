from fastapi import APIRouter, UploadFile, File, HTTPException, Form, Body, Depends
from typing import Optional
from datetime import datetime
from app.db.client import get_client
from app.services.file_parser.excel_parser import parse_excel, parse_csv
from app.services.file_parser.ai_normalizer import normalize_columns
from app.auth.deps import require_admin, UserContext

router = APIRouter()

# In-memory cache of parsed rows keyed by file_hash (cleared on restart)
_parsed_cache: dict = {}

def _extract_value(row: dict, col: str):
    if not col:
        return None
    return row.get(col)

def _to_float(val) -> Optional[float]:
    try:
        return float(str(val).replace(",", "").strip()) if val not in (None, "", "nan") else None
    except Exception:
        return None

def _to_date(val) -> Optional[str]:
    if not val or str(val) in ("", "nan", "None"):
        return None
    try:
        from datetime import datetime as dt
        if isinstance(val, dt):
            return val.date().replace(day=1).isoformat()
        s = str(val).strip()
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S", "%m-%d-%Y"):
            try:
                return dt.strptime(s[:10], fmt).replace(day=1).date().isoformat()
            except Exception:
                continue
    except Exception:
        pass
    return None

@router.post("")
async def upload_file(
    file: UploadFile = File(...),
    supplier_id: Optional[str] = Form(None),
    billing_period_start: Optional[str] = Form(None),
    billing_period_end: Optional[str] = Form(None),
    user: UserContext = Depends(require_admin)
):
    db = get_client()
    file_bytes = await file.read()
    filename = file.filename or "upload"
    ext = filename.rsplit(".", 1)[-1].lower()

    if ext not in ("csv", "xlsx", "xls", "pdf"):
        raise HTTPException(status_code=400, detail="Unsupported file type. Use CSV, Excel, or PDF.")

    if ext in ("xlsx", "xls"):
        parsed = parse_excel(file_bytes)
    elif ext == "csv":
        parsed = parse_csv(file_bytes)
    else:
        raise HTTPException(status_code=400, detail="PDF upload coming in Phase 2.")

    # Check for duplicate
    existing = db.table("upload_batches").select("id").eq("file_hash", parsed["file_hash"]).execute()
    if existing.data:
        raise HTTPException(status_code=409, detail="This file has already been uploaded.")

    # AI column mapping
    ai_result = normalize_columns(parsed["headers"], parsed["sample_rows"])

    # Cache rows for confirm step
    _parsed_cache[parsed["file_hash"]] = parsed["all_rows"]

    # Create upload batch
    batch = {
        "supplier_id": supplier_id,
        "original_filename": filename,
        "storage_path": f"uploads/{parsed['file_hash']}.{ext}",
        "file_type": ext,
        "file_hash": parsed["file_hash"],
        "status": "review",
        "billing_period_start": billing_period_start,
        "billing_period_end": billing_period_end,
        "ai_column_mapping": ai_result,
        "rows_parsed": parsed["row_count"],
    }

    res = db.table("upload_batches").insert(batch).execute()
    batch_id = res.data[0]["id"]

    return {
        "upload_batch_id": batch_id,
        "file_hash": parsed["file_hash"],
        "status": "review",
        "rows_parsed": parsed["row_count"],
        "headers": parsed["headers"],
        "ai_mapping": ai_result,
        "sample_rows": parsed["sample_rows"][:5]
    }

@router.get("")
def list_uploads(supplier_id: Optional[str] = None, user: UserContext = Depends(require_admin)):
    db = get_client()
    q = db.table("upload_batches").select("*, suppliers(name, code)").order("created_at", desc=True)
    if supplier_id:
        q = q.eq("supplier_id", supplier_id)
    return q.execute().data

@router.get("/{id}")
def get_upload(id: str, user: UserContext = Depends(require_admin)):
    db = get_client()
    res = db.table("upload_batches").select("*, suppliers(name, code)").eq("id", id).single().execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Upload not found")
    return res.data

@router.post("/{id}/confirm")
def confirm_upload(
    id: str,
    file_hash: str = Body(...),
    supplier_id: str = Body(...),
    billing_month: str = Body(...),
    column_mapping: dict = Body(...),
    user: UserContext = Depends(require_admin),
):
    db = get_client()
    batch = db.table("upload_batches").select("*").eq("id", id).single().execute()
    if not batch.data:
        raise HTTPException(status_code=404, detail="Upload not found")

    rows = _parsed_cache.get(file_hash, [])
    if not rows:
        raise HTTPException(status_code=400, detail="Parsed data not found. Please re-upload the file.")

    mapping = column_mapping.get("mapping", column_mapping)
    esiid_col        = mapping.get("esiid")
    name_col         = mapping.get("customer_name")
    first_name_col   = mapping.get("customer_first_name")
    last_name_col    = mapping.get("customer_last_name")
    status_col       = mapping.get("customer_status")
    address_col      = mapping.get("service_address")
    rate_col         = mapping.get("rate")
    amount_col       = mapping.get("amount")
    kwh_col          = mapping.get("kwh")
    start_col        = mapping.get("bill_start_date")
    end_col          = mapping.get("bill_end_date")

    records = []
    skipped = 0
    going_final = []

    for row in rows:
        raw_esiid  = str(_extract_value(row, esiid_col) or "").strip()
        raw_amount = _to_float(_extract_value(row, amount_col))
        if name_col:
            raw_name = str(_extract_value(row, name_col) or "").strip()
        else:
            first = str(_extract_value(row, first_name_col) or "").strip()
            last  = str(_extract_value(row, last_name_col) or "").strip()
            raw_name = " ".join(filter(None, [first, last]))
        raw_name = raw_name[:200]
        raw_status = str(_extract_value(row, status_col) or "").strip().lower() if status_col else ""

        # Flag accounts that are canceling — covers variations across all REPs
        is_going_final = any(kw in raw_status for kw in [
            "going final", "final", "cancelled", "canceled", "churned",
            "terminating", "dropping", "drop", "cancel"
        ])
        if is_going_final and raw_esiid:
            # Try to find matching lead
            lead_match = None
            try:
                ld = db.table("lead_deals").select("lead_id, leads(id, name, phone)") \
                    .eq("esiid", raw_esiid).limit(1).execute()
                if ld.data:
                    lead_match = ld.data[0].get("leads")
            except Exception:
                pass
            going_final.append({
                "esiid":         raw_esiid,
                "customer_name": raw_name,
                "status":        str(_extract_value(row, status_col) or ""),
                "lead":          lead_match,
            })

        if not raw_esiid or raw_amount is None:
            skipped += 1
            continue

        # Use billing_month from form, fallback to bill_start_date in row
        bm = billing_month
        if not bm and start_col:
            bm = _to_date(_extract_value(row, start_col))
        if not bm:
            skipped += 1
            continue

        # Try to match ESIID to a known service point
        sp = db.table("service_points").select("id").eq("esiid", raw_esiid).execute()
        service_point_id = sp.data[0]["id"] if sp.data else None

        records.append({
            "upload_batch_id":  id,
            "supplier_id":      supplier_id,
            "service_point_id": service_point_id,
            "billing_month":    bm,
            "raw_esiid":        raw_esiid,
            "raw_customer_name": raw_name,
            "raw_amount":       raw_amount,
            "raw_kwh":          _to_float(_extract_value(row, kwh_col)),
            "raw_rate":         _to_float(_extract_value(row, rate_col)),
            "raw_row_data":     row,
            "resolved_esiid":   raw_esiid,
            "resolved_amount":  raw_amount,
        })

    if records:
        for i in range(0, len(records), 100):
            db.table("actual_commissions").insert(records[i:i+100]).execute()

    # Persist going_final list on the batch so it's retrievable later
    db.table("upload_batches").update({
        "status":        "confirmed",
        "confirmed_at":  datetime.utcnow().isoformat(),
        "rows_imported": len(records),
        "supplier_id":   supplier_id,
        "going_final":   going_final if going_final else None,
    }).eq("id", id).execute()

    return {
        "status":        "confirmed",
        "rows_imported": len(records),
        "rows_skipped":  skipped,
        "going_final":   going_final,
    }

@router.post("/{id}/reject")
def reject_upload(id: str, reason: str = "", user: UserContext = Depends(require_admin)):
    db = get_client()
    db.table("upload_batches").update({
        "status": "failed",
        "parse_errors": [{"reason": reason}]
    }).eq("id", id).execute()
    return {"status": "failed"}

@router.patch("/{id}")
def update_upload(id: str, data: dict = Body(...), user: UserContext = Depends(require_admin)):
    db = get_client()
    allowed = {"supplier_id", "original_filename"}
    payload = {k: v for k, v in data.items() if k in allowed}
    if not payload:
        raise HTTPException(status_code=400, detail="No valid fields to update")
    res = db.table("upload_batches").update(payload).eq("id", id).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Upload not found")
    return res.data[0]

@router.delete("/{id}")
def delete_upload(id: str, user: UserContext = Depends(require_admin)):
    db = get_client()
    # Remove associated commission rows first
    db.table("actual_commissions").delete().eq("upload_batch_id", id).execute()
    db.table("upload_batches").delete().eq("id", id).execute()
    return {"ok": True}
