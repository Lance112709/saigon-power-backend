"""Commission payments linked to customers/deals by ESI ID. Admin only.

The ledger is actual_commissions (written by the statement import pipeline).
Linkage is computed at read time by ESI ID, so a payment that arrived before
its deal was matched attaches automatically once the deal gains an ESI ID.
Paid/partial/unpaid verdicts come from reconciliation_items — the engine that
already compared every payment against the contracted adder.
"""
import re
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.auth.deps import UserContext, require_admin
from app.db.client import get_client

router = APIRouter()

STATUS_MAP = {"matched": "paid", "short_paid": "partial", "missing": "unpaid",
              "over_paid": "paid", "unexpected": "paid"}


def norm_es(v) -> str:
    return re.sub(r"\D", "", str(v or ""))


def month_status(items: list) -> dict:
    """(esiid, month) -> paid/partial/unpaid from reconciliation items.
    The worst verdict wins when a month has several items."""
    rank = {"unpaid": 2, "partial": 1, "paid": 0}
    out: dict = {}
    for i in items:
        key = (norm_es(i.get("esiid")), (i.get("billing_month") or "")[:7])
        st = STATUS_MAP.get(i.get("status") or "", "paid")
        if key not in out or rank[st] > rank[out[key]]:
            out[key] = st
    return out


def _fetch(db, table, cols, flt):
    out, off = [], 0
    while True:
        q = flt(db.table(table).select(cols))
        page = q.order("id").range(off, off + 999).execute().data or []
        out.extend(page)
        if len(page) < 1000 or len(out) >= 8000:
            break
        off += 1000
    return out


def _esiids_for(db, customer_id: Optional[str], deal_id: Optional[str],
                lead_id: Optional[str]) -> list:
    esiids = []
    if deal_id:
        for t in ("crm_deals", "lead_deals"):
            r = db.table(t).select("esiid").eq("id", deal_id).limit(1).execute().data
            if r:
                esiids = [r[0].get("esiid")]
                break
    elif customer_id:
        r = db.table("crm_deals").select("esiid").eq("customer_id", customer_id).execute().data
        esiids = [d.get("esiid") for d in r or []]
    elif lead_id:
        r = db.table("lead_deals").select("esiid").eq("lead_id", lead_id).execute().data
        esiids = [d.get("esiid") for d in r or []]
    return sorted({norm_es(e) for e in esiids if norm_es(e)})


@router.get("")
def list_payments(
    customer_id: Optional[str] = Query(None),
    deal_id: Optional[str] = Query(None),
    lead_id: Optional[str] = Query(None),
    user: UserContext = Depends(require_admin),
):
    if not (customer_id or deal_id or lead_id):
        raise HTTPException(status_code=400, detail="Pass customer_id, deal_id, or lead_id")
    db = get_client()
    esiids = _esiids_for(db, customer_id, deal_id, lead_id)
    if not esiids:
        return {"esi_ids": [], "payments": [], "total": 0, "months": []}

    rows = _fetch(db, "actual_commissions",
                  "id,raw_esiid,resolved_esiid,billing_month,raw_amount,raw_kwh,raw_rate,"
                  "raw_customer_name,supplier_id,upload_batch_id,is_matched,created_at,raw_row_data",
                  lambda q: q.in_("raw_esiid", esiids))

    sup_ids = sorted({r["supplier_id"] for r in rows if r.get("supplier_id")})
    sups = {}
    if sup_ids:
        sups = {s["id"]: s["name"] for s in
                db.table("suppliers").select("id,name").in_("id", sup_ids).execute().data}
    batch_ids = sorted({r["upload_batch_id"] for r in rows if r.get("upload_batch_id")})
    batches = {}
    for i in range(0, len(batch_ids), 100):
        for b in db.table("upload_batches").select("id,original_filename") \
                .in_("id", batch_ids[i:i + 100]).execute().data or []:
            batches[b["id"]] = b.get("original_filename")

    recon = _fetch(db, "reconciliation_items", "esiid,billing_month,status",
                   lambda q: q.in_("esiid", esiids))
    statuses = month_status(recon)

    payments = []
    for r in sorted(rows, key=lambda x: (x.get("billing_month") or "", x.get("created_at") or ""),
                    reverse=True):
        norm = (r.get("raw_row_data") or {}).get("_norm") or {}
        month = (r.get("billing_month") or "")[:7]
        es = norm_es(r.get("resolved_esiid") or r.get("raw_esiid"))
        payments.append({
            "id": r["id"],
            "esi_id": es,
            "payment_date": r.get("billing_month"),
            "amount": float(r.get("raw_amount") or 0),
            "kwh": r.get("raw_kwh"),
            "rate": r.get("raw_rate"),
            "supplier": sups.get(r.get("supplier_id")),
            "statement_reference": r.get("upload_batch_id"),
            "statement_file": batches.get(r.get("upload_batch_id")),
            "service_start": norm.get("service_start"),
            "service_end": norm.get("service_end"),
            "is_matched": r.get("is_matched"),
            "status": statuses.get((es, month), "paid"),
            "created_at": r.get("created_at"),
        })

    months: dict = {}
    for p in payments:
        m = (p["payment_date"] or "")[:7]
        b = months.setdefault(m, {"month": m, "amount": 0.0, "status": "paid"})
        b["amount"] = round(b["amount"] + p["amount"], 2)
        rank = {"unpaid": 2, "partial": 1, "paid": 0}
        if rank[p["status"]] > rank[b["status"]]:
            b["status"] = p["status"]
    month_list = sorted(months.values(), key=lambda x: x["month"], reverse=True)

    return {
        "esi_ids": esiids,
        "payments": payments,
        "total": round(sum(p["amount"] for p in payments), 2),
        "months": month_list,
        "latest_status": month_list[0]["status"] if month_list else None,
    }


@router.get("/unmatched")
def unmatched_payments(limit: int = Query(50), user: UserContext = Depends(require_admin)):
    """Orphan payments: statement rows whose ESI ID still matches no deal.
    Grouped per ESI ID, largest lifetime total first."""
    db = get_client()
    deal_es = set()
    for t in ("crm_deals", "lead_deals"):
        off = 0
        while True:
            page = db.table(t).select("esiid").order("id").range(off, off + 999).execute().data or []
            deal_es.update(norm_es(d.get("esiid")) for d in page)
            if len(page) < 1000:
                break
            off += 1000

    groups: dict = {}
    off = 0
    while True:
        page = db.table("actual_commissions") \
            .select("raw_esiid,raw_customer_name,raw_amount,billing_month,supplier_id") \
            .eq("is_matched", False).order("id").range(off, off + 999).execute().data or []
        for r in page:
            es = norm_es(r.get("raw_esiid"))
            if not es or es in deal_es:
                continue
            g = groups.setdefault(es, {"esi_id": es, "customer_name": r.get("raw_customer_name"),
                                       "total": 0.0, "payments": 0, "last_month": "",
                                       "supplier_id": r.get("supplier_id")})
            g["total"] = round(g["total"] + float(r.get("raw_amount") or 0), 2)
            g["payments"] += 1
            g["last_month"] = max(g["last_month"], (r.get("billing_month") or "")[:7])
        if len(page) < 1000 or off >= 30000:
            break
        off += 1000

    sups = {s["id"]: s["name"] for s in db.table("suppliers").select("id,name").execute().data}
    out = sorted(groups.values(), key=lambda g: -g["total"])[:limit]
    for g in out:
        g["supplier"] = sups.get(g.pop("supplier_id"))
    return {"count": len(groups), "orphans": out}
