"""Commission statement uploads.

One-step flow: POST a statement file and, when the provider format is
recognized (all 5 current REPs), everything happens automatically —
parse → store original file → match rows to deals → backfill missing
ESIIDs by address → import → reconcile → return findings.

Unknown formats fall back to the AI column-mapping review flow. Parsed rows
for that flow are staged in Supabase Storage (not process memory), so a
backend restart between upload and confirm no longer strands the batch.
"""
import json
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, UploadFile, File, HTTPException, Form, Body, Depends

from app.db.client import get_client
from app.services.file_parser.excel_parser import parse_excel, parse_csv
from app.services.file_parser.ai_normalizer import normalize_columns
from app.services.file_parser.provider_parsers import (
    detect_and_parse, normalize_esiid, PROVIDER_SUPPLIERS, label_from_filename,
)
from app.services.reconciliation_v2 import (
    load_deals, backfill_esiids, run_reconciliation_v2, get_or_create_supplier,
    rows_from_db, replace_prior_runs,
)
from app.services.status_sync import sync_statuses, TRUSTED_STATUS_GROUPS
from app.services.audit import audit
from app.auth.deps import require_admin, UserContext

router = APIRouter()

STATEMENTS_BUCKET = "statements"


def _storage_put(db, path: str, blob: bytes, content_type: str):
    try:
        db.storage.from_(STATEMENTS_BUCKET).upload(path, blob, {"content-type": content_type, "upsert": "true"})
    except Exception:
        try:
            db.storage.create_bucket(STATEMENTS_BUCKET)
            db.storage.from_(STATEMENTS_BUCKET).upload(path, blob, {"content-type": content_type})
        except Exception:
            pass  # storing the original is best-effort; import still proceeds


def _storage_get_json(db, path: str):
    try:
        blob = db.storage.from_(STATEMENTS_BUCKET).download(path)
        return json.loads(blob)
    except Exception:
        return None


def _to_float(val) -> Optional[float]:
    try:
        return float(str(val).replace(",", "").strip()) if val not in (None, "", "nan") else None
    except Exception:
        return None


def _to_month(val) -> Optional[str]:
    if not val or str(val) in ("", "nan", "None"):
        return None
    s = str(val).strip()[:10]
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y"):
        try:
            return datetime.strptime(s, fmt).replace(day=1).date().isoformat()
        except Exception:
            continue
    return None


def _process_rows(db, batch_id: str, provider_group: Optional[str], supplier_id: str,
                  rows: list, amount_received: Optional[float], actor: str,
                  warnings: list, going_final: list, trust_status: bool = False) -> dict:
    """Shared pipeline: match rows to deals, backfill ESIIDs, insert
    actual_commissions, sync provider-reported statuses, reconcile each month."""
    deals = load_deals(db, provider_group) if provider_group else {"by_esiid": {}, "no_esiid": [], "addr_index": {}}

    backfilled = backfill_esiids(db, deals, rows, actor) if provider_group else []

    # Auto-update deal statuses from the provider's status column (trusted sources only)
    status_sync = None
    if provider_group and (trust_status or provider_group in TRUSTED_STATUS_GROUPS):
        batch_meta = db.table("upload_batches").select("original_filename").eq("id", batch_id).limit(1).execute().data
        source = f"{provider_group} — {(batch_meta[0]['original_filename'] if batch_meta else batch_id)}"
        status_sync = sync_statuses(db, rows, deals, source, actor)
        if status_sync.get("pending"):
            warnings.append(
                f"Status column looks unreliable this month ({int(status_sync['churn_ratio']*100)}% of accounts "
                f"marked churned) — {status_sync['deactivated']} deactivations held for your review.")

    records, matched_count = [], 0
    for r in rows:
        deal = deals["by_esiid"].get(r["esiid"])
        if deal:
            matched_count += 1
        raw = dict(r.get("raw") or {})
        raw["_norm"] = {
            "service_start": r.get("service_start"), "service_end": r.get("service_end"),
            "provider_status": r.get("provider_status"), "row_type": r.get("row_type", "commission"),
            "statement_label": r.get("statement_label"),
            "deal_source": deal["source"] if deal else None,
            "deal_id": deal["id"] if deal else None,
            "lead_id": deal.get("lead_id") if deal else None,
        }
        records.append({
            "upload_batch_id": batch_id,
            "supplier_id": supplier_id,
            "service_point_id": None,
            "billing_month": f"{r['statement_label']}-01",
            "raw_esiid": r["esiid"],
            "raw_customer_name": (r.get("customer_name") or "")[:200],
            "raw_amount": r.get("amount") or 0,
            "raw_kwh": r.get("usage_kwh"),
            "raw_rate": r.get("rate"),
            "raw_row_data": raw,
            "resolved_esiid": r["esiid"],
            "resolved_amount": r.get("amount") or 0,
            "is_matched": deal is not None,
            "matched_at": datetime.now(timezone.utc).isoformat() if deal else None,
        })
    for i in range(0, len(records), 200):
        db.table("actual_commissions").insert(records[i:i + 200]).execute()

    runs = []
    if provider_group:
        labels = sorted({r["statement_label"] for r in rows if r.get("statement_label")})
        batch_counts = {}
        for r in rows:
            batch_counts[r["statement_label"]] = batch_counts.get(r["statement_label"], 0) + 1
        for label in labels:
            # Reconcile ALL imported rows for this supplier+month (merges
            # supplemental statements, catches cross-upload duplicates) and
            # replace the month's previous run, keeping resolved items resolved.
            merged = rows_from_db(db, supplier_id, label)
            carry = replace_prior_runs(db, supplier_id, label)
            if len(merged) > batch_counts.get(label, 0):
                warnings.append(f"{label}: merged with {len(merged) - batch_counts[label]} rows "
                                f"from earlier uploads for the same month")
            runs.append(run_reconciliation_v2(
                db, supplier_id, provider_group, label, merged,
                batch_id=batch_id, deals=deals, actor=actor, carry_resolved=carry))

    total = round(sum(rec["raw_amount"] for rec in records), 2)
    difference = round(total - amount_received, 2) if amount_received is not None else None

    lead_map = _lead_lookup(db, [g["esiid"] for g in going_final]) if going_final else {}
    for g in going_final:
        g["lead"] = lead_map.get(g["esiid"])

    db.table("upload_batches").update({
        "status": "confirmed",
        "confirmed_at": datetime.now(timezone.utc).isoformat(),
        "rows_imported": len(records),
        "supplier_id": supplier_id,
        "going_final": going_final or None,
        "amount_received": amount_received,
        "total_affinity_amount": total,
    }).eq("id", batch_id).execute()

    return {
        "status": "confirmed",
        "auto": provider_group is not None,
        "upload_batch_id": batch_id,
        "provider_group": provider_group,
        "supplier_id": supplier_id,
        "rows_imported": len(records),
        "matched_count": matched_count,
        "unknown_count": len(records) - matched_count,
        "backfilled_esiids": backfilled,
        "going_final": going_final,
        "amount_received": amount_received,
        "total_affinity_amount": total,
        "difference": difference,
        "amounts_match": abs(difference) < 0.02 if difference is not None else None,
        "runs": runs,
        "status_sync": status_sync,
        "warnings": warnings,
    }


def _lead_lookup(db, esiids: list) -> dict:
    out = {}
    esiids = [e for e in esiids if e]
    for i in range(0, len(esiids), 100):
        ld = db.table("lead_deals").select("esiid, lead_id, leads(id, first_name, last_name, phone)") \
            .in_("esiid", esiids[i:i + 100]).execute().data or []
        for row in ld:
            lead = row.get("leads") or {}
            if row.get("esiid") and lead:
                out[row["esiid"]] = {**lead, "name": f"{lead.get('first_name', '')} {lead.get('last_name', '')}".strip()}
    return out


@router.post("")
async def upload_file(
    file: UploadFile = File(...),
    supplier_id: Optional[str] = Form(None),
    amount_received: Optional[float] = Form(None),
    user: UserContext = Depends(require_admin),
):
    db = get_client()
    file_bytes = await file.read()
    filename = file.filename or "upload"
    ext = filename.rsplit(".", 1)[-1].lower()

    if ext not in ("csv", "xlsx", "xls"):
        raise HTTPException(status_code=400, detail="Unsupported file type. Use CSV or Excel.")

    # ── Auto path: recognized provider format ────────────────────────────────
    parsed_auto = detect_and_parse(file_bytes, filename)
    if parsed_auto:
        existing = db.table("upload_batches").select("id,status").eq("file_hash", parsed_auto["file_hash"]).execute()
        if existing.data:
            raise HTTPException(status_code=409, detail="This exact file was already uploaded.")

        _storage_put(db, f"{parsed_auto['file_hash']}.{ext}", file_bytes,
                     "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        sup_id = get_or_create_supplier(db, parsed_auto["supplier"])
        batch = db.table("upload_batches").insert({
            "supplier_id": sup_id,
            "original_filename": filename,
            "storage_path": f"{STATEMENTS_BUCKET}/{parsed_auto['file_hash']}.{ext}",
            "file_type": ext,
            "file_hash": parsed_auto["file_hash"],
            "status": "parsing",
            "ai_column_mapping": {"auto": True, "provider_group": parsed_auto["provider_group"],
                                  "statement_label": parsed_auto["statement_label"],
                                  "labels": parsed_auto["labels"], "detector": "fingerprint-v1"},
            "rows_parsed": parsed_auto["row_count"],
        }).execute().data[0]

        result = _process_rows(
            db, batch["id"], parsed_auto["provider_group"], sup_id,
            parsed_auto["rows"], amount_received, user.email or "admin",
            parsed_auto["warnings"], parsed_auto["going_final"])
        result.update({
            "statement_label": parsed_auto["statement_label"],
            "labels": parsed_auto["labels"],
            "total_amount": parsed_auto["total_amount"],
            "filename": filename,
        })
        audit(db, "upload_batches", batch["id"], "auto_import", None,
              {"filename": filename, "rows": parsed_auto["row_count"], "provider": parsed_auto["provider_group"]},
              reason="Automatic statement import", actor=user.email or "admin")
        return result

    # ── Review path: unknown format → AI column mapping ─────────────────────
    if ext in ("xlsx", "xls"):
        parsed = parse_excel(file_bytes)
    else:
        parsed = parse_csv(file_bytes)

    existing = db.table("upload_batches").select("id").eq("file_hash", parsed["file_hash"]).execute()
    if existing.data:
        raise HTTPException(status_code=409, detail="This exact file was already uploaded.")

    ai_result = normalize_columns(parsed["headers"], parsed["sample_rows"])

    _storage_put(db, f"{parsed['file_hash']}.{ext}", file_bytes, "application/octet-stream")
    _storage_put(db, f"{parsed['file_hash']}.rows.json",
                 json.dumps(parsed["all_rows"]).encode(), "application/json")

    batch = {
        "supplier_id": supplier_id,
        "original_filename": filename,
        "storage_path": f"{STATEMENTS_BUCKET}/{parsed['file_hash']}.{ext}",
        "file_type": ext,
        "file_hash": parsed["file_hash"],
        "status": "review",
        "ai_column_mapping": ai_result,
        "rows_parsed": parsed["row_count"],
    }
    res = db.table("upload_batches").insert(batch).execute()

    return {
        "auto": False,
        "upload_batch_id": res.data[0]["id"],
        "file_hash": parsed["file_hash"],
        "status": "review",
        "rows_parsed": parsed["row_count"],
        "headers": parsed["headers"],
        "ai_mapping": ai_result,
        "sample_rows": parsed["sample_rows"][:5],
        "suggested_month": label_from_filename(filename),
    }


@router.post("/{id}/confirm")
def confirm_upload(
    id: str,
    file_hash: str = Body(...),
    supplier_id: str = Body(...),
    billing_month: str = Body(...),
    column_mapping: dict = Body(...),
    amount_received: Optional[float] = Body(None),
    user: UserContext = Depends(require_admin),
):
    """Manual-mapping import for statement formats the auto-detector doesn't
    know yet. Uses the same matching/reconciliation pipeline as auto imports."""
    db = get_client()
    batch = db.table("upload_batches").select("*").eq("id", id).single().execute()
    if not batch.data:
        raise HTTPException(status_code=404, detail="Upload not found")
    if batch.data.get("status") == "confirmed":
        raise HTTPException(status_code=409, detail="This upload was already imported.")

    raw_rows = _storage_get_json(db, f"{file_hash}.rows.json")
    if not raw_rows:
        raise HTTPException(status_code=400, detail="Parsed data not found. Please delete this upload and re-upload the file.")

    mapping = column_mapping.get("mapping", column_mapping)

    def col(row, key):
        c = mapping.get(key)
        return row.get(c) if c else None

    month = _to_month(billing_month) or billing_month
    label = month[:7]

    rows, skipped, going_final = [], 0, []
    for row in raw_rows:
        esiid = normalize_esiid(col(row, "esiid"))
        amount = _to_float(col(row, "amount"))
        status = str(col(row, "customer_status") or "").strip()
        name = str(col(row, "customer_name") or "").strip()
        if not name:
            name = " ".join(filter(None, [str(col(row, "customer_first_name") or "").strip(),
                                          str(col(row, "customer_last_name") or "").strip()]))
        if status and any(k in status.lower() for k in
                          ["going final", "final", "cancel", "churn", "terminat", "drop", "inactive", "closed"]):
            if esiid:
                going_final.append({"esiid": esiid, "customer_name": name, "status": status})
        if not esiid or amount is None:
            skipped += 1
            continue
        row_label = label
        if not row_label:
            m = _to_month(col(row, "bill_start_date"))
            row_label = m[:7] if m else None
        if not row_label:
            skipped += 1
            continue
        rows.append({
            "esiid": esiid, "customer_name": name,
            "address": str(col(row, "service_address") or "").strip(),
            "city": "", "zip": "",
            "usage_kwh": _to_float(col(row, "kwh")), "rate": _to_float(col(row, "rate")),
            "amount": amount,
            "service_start": _to_month(col(row, "bill_start_date")),
            "service_end": _to_month(col(row, "bill_end_date")),
            "provider_status": status, "row_type": "commission",
            "statement_label": row_label, "raw": row,
        })

    # Derive provider group from the chosen supplier so reconciliation can run
    provider_group = None
    sup = db.table("suppliers").select("code,name").eq("id", supplier_id).limit(1).execute().data
    if sup:
        for grp, sdef in PROVIDER_SUPPLIERS.items():
            if sdef["code"] == sup[0].get("code"):
                provider_group = grp
                break

    result = _process_rows(db, id, provider_group, supplier_id, rows,
                           amount_received, user.email or "admin", [], going_final,
                           trust_status=bool(mapping.get("customer_status")))
    result["rows_skipped"] = skipped
    audit(db, "upload_batches", id, "manual_import", None,
          {"rows": len(rows), "supplier_id": supplier_id, "billing_month": billing_month},
          reason="Manual-mapping statement import", actor=user.email or "admin")
    return result


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


@router.post("/poll-email")
def poll_email(user: UserContext = Depends(require_admin)):
    """Check the inbox for new commission statements right now."""
    from app.services.email_ingest import poll_inbox
    return poll_inbox(actor=user.email or "admin")


@router.post("/{id}/apply-statuses")
def apply_statuses(id: str, user: UserContext = Depends(require_admin)):
    """Force-apply the provider-reported status changes from a batch whose
    status column tripped the mass-churn safety check."""
    db = get_client()
    batch = db.table("upload_batches").select("*").eq("id", id).single().execute().data
    if not batch:
        raise HTTPException(status_code=404, detail="Upload not found")
    provider_group = (batch.get("ai_column_mapping") or {}).get("provider_group")
    if not provider_group:
        raise HTTPException(status_code=400, detail="This batch has no detected provider.")

    recs, off = [], 0
    while True:
        page = db.table("actual_commissions").select("raw_esiid,raw_row_data") \
            .eq("upload_batch_id", id).range(off, off + 999).execute().data or []
        recs.extend(page)
        if len(page) < 1000:
            break
        off += 1000
    rows = []
    for r in recs:
        norm = (r.get("raw_row_data") or {}).get("_norm") or {}
        rows.append({"esiid": r["raw_esiid"], "provider_status": norm.get("provider_status") or "",
                     "statement_label": norm.get("statement_label") or ""})

    deals = load_deals(db, provider_group)
    source = f"{provider_group} — {batch.get('original_filename')} (force-applied)"
    result = sync_statuses(db, rows, deals, source, user.email or "admin", force=True)
    audit(db, "upload_batches", id, "force_apply_statuses", None, result,
          reason="Admin confirmed status changes despite mass-churn warning", actor=user.email or "admin")
    return result


@router.post("/{id}/reject")
def reject_upload(id: str, reason: str = "", user: UserContext = Depends(require_admin)):
    db = get_client()
    db.table("upload_batches").update({
        "status": "failed",
        "parse_errors": [{"reason": reason}]
    }).eq("id", id).execute()
    audit(db, "upload_batches", id, "reject", None, {"reason": reason}, actor=user.email or "admin")
    return {"status": "failed"}


@router.get("/{id}/records")
def get_upload_records(
    id: str,
    unmatched_only: bool = False,
    limit: int = 200,
    offset: int = 0,
    user: UserContext = Depends(require_admin),
):
    db = get_client()
    q = db.table("actual_commissions").select("*").eq("upload_batch_id", id)
    if unmatched_only:
        q = q.eq("is_matched", False)
    res = q.order("raw_customer_name").range(offset, offset + limit - 1).execute()
    records = res.data or []
    if not records:
        return records

    esiids = list({r["raw_esiid"] for r in records if r.get("raw_esiid")})
    lead_map = _lead_lookup(db, esiids)
    out = []
    for r in records:
        info = lead_map.get(r.get("raw_esiid", ""))
        r["lead_deal_matched"] = r.get("raw_esiid", "") in lead_map
        r["lead_match"] = {"lead_id": info.get("id"), "lead_name": info.get("name")} if info else None
        out.append(r)
    return out


@router.patch("/{batch_id}/records/{record_id}")
def match_record(
    batch_id: str,
    record_id: str,
    data: dict = Body(...),
    user: UserContext = Depends(require_admin),
):
    """Manually link a commission record to a deal by ESI ID."""
    db = get_client()
    esiid = normalize_esiid(data.get("esiid"))
    if not esiid:
        raise HTTPException(status_code=400, detail="esiid is required")

    old = db.table("actual_commissions").select("resolved_esiid").eq("id", record_id).limit(1).execute().data
    lead_info = _lead_lookup(db, [esiid]).get(esiid)
    db.table("actual_commissions").update({
        "resolved_esiid": esiid,
        "is_matched": lead_info is not None,
        "matched_at": datetime.now(timezone.utc).isoformat() if lead_info else None,
    }).eq("id", record_id).execute()
    audit(db, "actual_commissions", record_id, "manual_match",
          {"resolved_esiid": old[0]["resolved_esiid"] if old else None},
          {"resolved_esiid": esiid}, actor=user.email or "admin")
    return {"ok": True, "lead": lead_info}


@router.patch("/{id}")
def update_upload(id: str, data: dict = Body(...), user: UserContext = Depends(require_admin)):
    db = get_client()
    allowed = {"supplier_id", "original_filename"}
    payload = {k: v for k, v in data.items() if k in allowed}
    if not payload:
        raise HTTPException(status_code=400, detail="No valid fields to update")
    old = db.table("upload_batches").select("supplier_id,original_filename").eq("id", id).limit(1).execute().data
    res = db.table("upload_batches").update(payload).eq("id", id).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Upload not found")
    # keep commission rows consistent when the supplier is corrected
    if "supplier_id" in payload and payload["supplier_id"]:
        db.table("actual_commissions").update({"supplier_id": payload["supplier_id"]}).eq("upload_batch_id", id).execute()
    audit(db, "upload_batches", id, "update", old[0] if old else None, payload, actor=user.email or "admin")
    return res.data[0]


@router.delete("/{id}")
def delete_upload(id: str, user: UserContext = Depends(require_admin)):
    """Remove an imported batch: its commission rows and the reconciliation
    runs generated from it. The original file stays in storage and the
    deletion is audit-logged."""
    db = get_client()
    batch = db.table("upload_batches").select("*").eq("id", id).limit(1).execute().data
    runs = db.table("reconciliation_runs").select("id").like("notes", f"%{id}%").execute().data or []
    for r in runs:
        db.table("reconciliation_runs").delete().eq("id", r["id"]).execute()  # items cascade
    db.table("actual_commissions").delete().eq("upload_batch_id", id).execute()
    db.table("upload_batches").delete().eq("id", id).execute()
    audit(db, "upload_batches", id, "delete", batch[0] if batch else None, None,
          reason=f"Deleted batch + {len(runs)} reconciliation runs; original file kept in storage",
          actor=user.email or "admin")
    return {"ok": True, "runs_deleted": len(runs)}
