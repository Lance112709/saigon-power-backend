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

from app.auth.deps import UserContext, require_admin, get_current_user
from app.auth.ownership import assert_customer_access, assert_crm_deal_access, assert_lead_access
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


# Statement supplier code -> provider family. NRG pays Discount Power / Direct
# Energy / Cirro / Reliant meters on one residual statement, so those all count
# as the same family; every other REP is its own family.
SUPPLIER_FAMILY = {"NRG": "NRG", "NRGBIZ": "NRG", "NRG_COMM": "NRG",
                   "RELIANT": "NRG", "CIRRO": "NRG"}
# Deal provider label prefix (letters only, lowercase) -> family.
_PROVIDER_PREFIXES = (
    ("discount", "NRG"), ("direct", "NRG"), ("cirro", "NRG"), ("reliant", "NRG"),
    ("nrg", "NRG"), ("budget", "BUDGET"), ("chariot", "CHARIOT"),
    ("cleansky", "CLEANSKY"), ("heritage", "HERITAGE"), ("hudson", "HUDSON"),
    ("ironhorse", "IRONHORSE"), ("apg", "APGE"), ("tara", "TARA"),
    ("pennywise", "PENNYWISE"),
)


def provider_family(label) -> Optional[str]:
    """Family for a deal's provider label ('Discount Power' -> 'NRG'); None if unknown."""
    key = re.sub(r"[^a-z]", "", str(label or "").lower())
    for prefix, fam in _PROVIDER_PREFIXES:
        if key.startswith(prefix):
            return fam
    return None


def supplier_family(code) -> Optional[str]:
    code = str(code or "").strip().upper()
    return SUPPLIER_FAMILY.get(code, code or None)


def split_by_provider(rows: list, family: Optional[str], sup_codes: dict) -> tuple:
    """(rows for this deal's provider family, rows from other providers).
    With no family known, everything is kept — same as before."""
    if not family:
        return rows, []
    keep, other = [], []
    for r in rows:
        fam = supplier_family(sup_codes.get(r.get("supplier_id")))
        (keep if fam == family else other).append(r)
    return keep, other


def _esiids_for(db, customer_id: Optional[str], deal_id: Optional[str],
                lead_id: Optional[str]) -> list:
    return _scope_for(db, customer_id, deal_id, lead_id)[0]


def _scope_for(db, customer_id: Optional[str], deal_id: Optional[str],
               lead_id: Optional[str]) -> tuple:
    """(esiids, provider_label, provider_family). A deal-scoped request also
    carries the deal's provider so statements from other REPs on the same
    meter (e.g. an old Chariot contract) are not shown under it."""
    esiids, label = [], None
    if deal_id:
        for t, col in (("crm_deals", "provider"), ("lead_deals", "supplier")):
            r = db.table(t).select(f"esiid,{col}").eq("id", deal_id).limit(1).execute().data
            if r:
                esiids = [r[0].get("esiid")]
                label = r[0].get(col)
                break
    elif customer_id:
        r = db.table("crm_deals").select("esiid").eq("customer_id", customer_id).execute().data
        esiids = [d.get("esiid") for d in r or []]
    elif lead_id:
        r = db.table("lead_deals").select("esiid").eq("lead_id", lead_id).execute().data
        esiids = [d.get("esiid") for d in r or []]
    return sorted({norm_es(e) for e in esiids if norm_es(e)}), label, provider_family(label)


def _suppliers(db, rows: list) -> tuple:
    """(id -> name, id -> code) for the suppliers referenced by rows."""
    sup_ids = sorted({r["supplier_id"] for r in rows if r.get("supplier_id")})
    if not sup_ids:
        return {}, {}
    sups = db.table("suppliers").select("id,name,code").in_("id", sup_ids).execute().data or []
    return {s["id"]: s["name"] for s in sups}, {s["id"]: s.get("code") for s in sups}


def _hidden_summary(other: list, names: dict) -> dict:
    """What a deal page is not showing: payments on this meter from other REPs."""
    by_sup: dict = {}
    for r in other:
        by_sup[names.get(r.get("supplier_id")) or "Unknown supplier"] = \
            by_sup.get(names.get(r.get("supplier_id")) or "Unknown supplier", 0) + 1
    return {"count": len(other),
            "suppliers": [{"supplier": k, "count": v} for k, v in sorted(by_sup.items(), key=lambda x: -x[1])]}


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
    esiids, provider, family = _scope_for(db, customer_id, deal_id, lead_id)
    if not esiids:
        return {"esi_ids": [], "payments": [], "total": 0, "months": [], "provider": provider}

    rows = _fetch(db, "actual_commissions",
                  "id,raw_esiid,resolved_esiid,billing_month,raw_amount,raw_kwh,raw_rate,"
                  "raw_customer_name,supplier_id,upload_batch_id,is_matched,created_at,raw_row_data",
                  lambda q: q.in_("raw_esiid", esiids))

    sups, codes = _suppliers(db, rows)
    rows, other = split_by_provider(rows, family, codes)
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
        "provider": provider,
        "hidden_other_provider": _hidden_summary(other, sups),
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


@router.get("/usage")
def usage(
    customer_id: Optional[str] = Query(None),
    deal_id: Optional[str] = Query(None),
    lead_id: Optional[str] = Query(None),
    user: UserContext = Depends(get_current_user),
):
    """Metered kWh history for a customer/deal/lead, visible to every CRM user.
    Deliberately excludes dollar amounts, rates, and payment statuses — those
    stay on the admin-only payments endpoint."""
    if not (customer_id or deal_id or lead_id):
        raise HTTPException(status_code=400, detail="Pass customer_id, deal_id, or lead_id")
    db = get_client()

    # Same per-record scoping the CRM pages enforce (sales agents see only their own).
    if customer_id:
        assert_customer_access(db, user, customer_id)
    elif lead_id:
        assert_lead_access(db, user, lead_id)
    elif deal_id:
        crm_hit = db.table("crm_deals").select("id").eq("id", deal_id).limit(1).execute().data
        if crm_hit:
            assert_crm_deal_access(db, user, deal_id)
        else:
            ld = db.table("lead_deals").select("lead_id").eq("id", deal_id).limit(1).execute().data
            if not ld:
                raise HTTPException(status_code=404, detail="Deal not found")
            assert_lead_access(db, user, ld[0]["lead_id"])

    esiids, provider, family = _scope_for(db, customer_id, deal_id, lead_id)
    if not esiids:
        return {"esi_ids": [], "payments": [], "provider": provider}

    rows = _fetch(db, "actual_commissions",
                  "id,raw_esiid,resolved_esiid,billing_month,raw_kwh,supplier_id,raw_row_data",
                  lambda q: q.in_("raw_esiid", esiids))

    sups, codes = _suppliers(db, rows)
    rows, other = split_by_provider(rows, family, codes)

    payments = []
    for r in sorted(rows, key=lambda x: (x.get("billing_month") or ""), reverse=True):
        norm = (r.get("raw_row_data") or {}).get("_norm") or {}
        payments.append({
            "id": r["id"],
            "esi_id": norm_es(r.get("resolved_esiid") or r.get("raw_esiid")),
            "payment_date": r.get("billing_month"),
            "kwh": r.get("raw_kwh"),
            "supplier": sups.get(r.get("supplier_id")),
            "service_start": norm.get("service_start"),
            "service_end": norm.get("service_end"),
        })

    return {"esi_ids": esiids, "payments": payments, "provider": provider,
            "hidden_other_provider": _hidden_summary(other, sups)}
