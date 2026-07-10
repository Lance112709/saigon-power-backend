"""Provider dispute packages.

One click builds everything needed to dispute underpayments with a provider:
a spreadsheet of every affected account (Summary + Accounts sheets), a
professional email draft, and links back to the exception cases it covers.

Disputes are ALWAYS created as drafts — a human reviews and explicitly sends.
Nothing in this module is ever called from a scheduler.
"""
import base64
import io
from datetime import datetime, timezone
from typing import Optional

from app.services.audit import audit
from app.services.customer_email import send_email
from app.services.reconciliation_v2 import fetch_all

DISPUTES_BUCKET = "statements"  # reuse the existing bucket; path disputes/


def _month7(d) -> str:
    return str(d or "")[:7]


def _build_xlsx(supplier_name: str, cases: list, finding: Optional[dict]) -> bytes:
    import pandas as pd

    months = sorted({_month7(c["billing_month"]) for c in cases})
    total = round(sum(float(c.get("estimated_loss") or 0) for c in cases), 2)

    summary = pd.DataFrame([{
        "Provider": supplier_name,
        "Statement Months": ", ".join(months),
        "Accounts": len(cases),
        "Total Claimed $": total,
        "Prepared": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "Prepared By": "Saigon Power LLC — Broker VID 319010",
    }])

    rate_detail = {}
    if finding and (finding.get("details") or {}).get("accounts"):
        for a in finding["details"]["accounts"]:
            rate_detail[a.get("esiid")] = a

    rows = []
    for c in sorted(cases, key=lambda c: -(float(c.get("estimated_loss") or 0))):
        extra = rate_detail.get(c["esiid"], {})
        rows.append({
            "ESI ID": c["esiid"],
            "Customer": c.get("customer_name") or "",
            "Statement Month": _month7(c["billing_month"]),
            "Issue": c.get("issue_type"),
            "Contract Rate": extra.get("rate_from"),
            "Paid Rate": extra.get("rate_to"),
            "kWh": extra.get("kwh"),
            "Amount Claimed $": float(c.get("estimated_loss") or 0),
            "Explanation": c.get("explanation") or "",
        })

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        summary.to_excel(w, sheet_name="Summary", index=False)
        pd.DataFrame(rows).to_excel(w, sheet_name="Accounts", index=False)
    buf.seek(0)
    return buf.getvalue()


def _draft_email(supplier_name: str, cases: list, finding: Optional[dict],
                 months: list, total: float) -> tuple:
    subject = (f"Commission discrepancy — Saigon Power (VID 319010) — "
               f"{', '.join(months)} — ${total:,.2f}")
    if finding:
        issue_line = finding.get("explanation") or finding.get("title") or ""
    else:
        by_type: dict = {}
        for c in cases:
            by_type[c["issue_type"]] = by_type.get(c["issue_type"], 0) + 1
        issue_line = ("Our reconciliation of your commission statements found " +
                      ", ".join(f"{n} account(s) with {t.replace('_', ' ')}"
                                for t, n in sorted(by_type.items())) + ".")
    body = (
        f"Hello,\n\n"
        f"We reconciled the {' and '.join(months)} commission statement(s) for "
        f"Saigon Power LLC (Broker VID 319010) and found discrepancies totaling "
        f"${total:,.2f} across {len(cases)} account(s).\n\n"
        f"{issue_line}\n\n"
        f"The attached spreadsheet lists every affected ESI ID with the amount "
        f"claimed and the reason. Please review and issue a true-up on the next "
        f"statement, or let us know what additional detail you need.\n\n"
        f"Thank you,\n"
        f"Saigon Power LLC\n(832) 937-9999 · power@saigonllc.com")
    return subject, body


def build_dispute_package(db, supplier_id: str, actor: str,
                          case_ids: Optional[list] = None,
                          finding_id: Optional[str] = None,
                          title: Optional[str] = None) -> dict:
    """Create a draft dispute (xlsx in storage + editable email) from a set of
    exception cases or from one grouped audit finding."""
    finding = None
    if finding_id:
        f = db.table("audit_findings").select("*").eq("id", finding_id) \
            .limit(1).execute().data
        if not f:
            raise ValueError("Finding not found")
        finding = f[0]
        cases = fetch_all(db, "exception_cases", "*",
                          filters=[("eq", ("finding_id", finding_id))])
        if not cases:  # fall back to the finding's own account list
            esiids = [a.get("esiid") for a in
                      (finding.get("details") or {}).get("accounts", [])]
            cases = []
            for i in range(0, len(esiids), 100):
                cases += fetch_all(db, "exception_cases", "*",
                                   filters=[("eq", ("supplier_id", supplier_id)),
                                            ("eq", ("billing_month", finding["billing_month"])),
                                            ("in_", ("esiid", esiids[i:i + 100]))])
    elif case_ids:
        cases = []
        for i in range(0, len(case_ids), 100):
            cases += fetch_all(db, "exception_cases", "*",
                               filters=[("in_", ("id", case_ids[i:i + 100]))])
    else:
        raise ValueError("Provide case_ids or finding_id")
    if not cases:
        raise ValueError("No exception cases to dispute")

    sup = db.table("suppliers").select("name,contact_email").eq("id", supplier_id) \
        .limit(1).execute().data
    supplier_name = (sup[0]["name"] if sup else "Provider")
    contact_email = (sup[0].get("contact_email") if sup else None) or ""

    months = sorted({_month7(c["billing_month"]) for c in cases})
    total = round(sum(float(c.get("estimated_loss") or 0) for c in cases), 2)
    subject, body = _draft_email(supplier_name, cases, finding, months, total)

    dispute = db.table("disputes").insert({
        "supplier_id": supplier_id,
        "status": "draft",
        "title": title or (finding["title"] if finding else
                           f"{supplier_name} discrepancies {', '.join(months)}"),
        "email_to": contact_email,
        "email_subject": subject,
        "email_body": body,
        "months": months,
        "total_claimed": total,
        "created_by": actor,
    }).execute().data[0]

    blob = _build_xlsx(supplier_name, cases, finding)
    path = f"disputes/{dispute['id']}.xlsx"
    try:
        db.storage.from_(DISPUTES_BUCKET).upload(
            path, blob,
            {"content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
             "upsert": "true"})
    except Exception:
        db.storage.create_bucket(DISPUTES_BUCKET)
        db.storage.from_(DISPUTES_BUCKET).upload(path, blob, {
            "content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"})
    db.table("disputes").update({"attachment_path": f"{DISPUTES_BUCKET}/{path}"}) \
        .eq("id", dispute["id"]).execute()
    dispute["attachment_path"] = f"{DISPUTES_BUCKET}/{path}"

    items = [{"dispute_id": dispute["id"], "case_id": c["id"], "esiid": c["esiid"],
              "billing_month": c["billing_month"],
              "claimed_amount": round(float(c.get("estimated_loss") or 0), 2)}
             for c in cases]
    for i in range(0, len(items), 200):
        db.table("dispute_items").insert(items[i:i + 200]).execute()
    for i in range(0, len(cases), 100):
        db.table("exception_cases").update({"dispute_id": dispute["id"]}) \
            .in_("id", [c["id"] for c in cases[i:i + 100]]).execute()
    if finding:
        db.table("audit_findings").update({"status": "disputed"}) \
            .eq("id", finding["id"]).eq("status", "open").execute()

    audit(db, "disputes", dispute["id"], "dispute_drafted", None,
          {"supplier": supplier_name, "cases": len(cases), "total": total},
          reason=dispute["title"], actor=actor)
    return {**dispute, "items_count": len(items)}


def get_attachment(db, dispute: dict) -> Optional[bytes]:
    path = (dispute.get("attachment_path") or "")
    if not path:
        return None
    bucket, _, rel = path.partition("/")
    try:
        return db.storage.from_(bucket).download(rel)
    except Exception:
        return None


def send_dispute(db, dispute_id: str, actor: str) -> dict:
    """Send a reviewed draft to the provider. Explicit human action only."""
    rows = db.table("disputes").select("*").eq("id", dispute_id).limit(1).execute().data
    if not rows:
        raise ValueError("Dispute not found")
    d = rows[0]
    if d["status"] != "draft":
        raise ValueError(f"Dispute is {d['status']} — only drafts can be sent")
    if not d.get("email_to"):
        raise ValueError("No recipient — set the provider contact email first")

    attachments = None
    blob = get_attachment(db, d)
    if blob:
        attachments = [{"filename": f"Saigon Power dispute {str(d.get('months'))[:40]}.xlsx"
                        .replace("[", "").replace("]", "").replace("'", ""),
                        "content": base64.b64encode(blob).decode()}]

    html = "<br>".join((d.get("email_body") or "").splitlines())
    result = send_email(d["email_to"], d.get("email_subject") or d["title"], html,
                        attachments=attachments)
    if not result.get("ok"):
        raise ValueError(result.get("error") or "Email failed")

    now = datetime.now(timezone.utc).isoformat()
    db.table("disputes").update({"status": "sent", "sent_at": now}).eq("id", dispute_id).execute()
    db.table("exception_cases").update({
        "workflow_status": "waiting_on_provider", "updated_at": now,
    }).eq("dispute_id", dispute_id).in_("workflow_status", ["open", "investigating"]).execute()
    audit(db, "disputes", dispute_id, "dispute_sent", {"status": "draft"},
          {"status": "sent", "to": d["email_to"]}, actor=actor)
    return {**d, "status": "sent", "sent_at": now}


def record_outcome(db, dispute_id: str, status: str, recovered_amount: float,
                   notes: str, actor: str) -> dict:
    """Record the provider's response and allocate recovered dollars across
    the covered cases (proportional to what each claimed)."""
    if status not in ("provider_responded", "recovered", "rejected"):
        raise ValueError("status must be provider_responded, recovered, or rejected")
    rows = db.table("disputes").select("*").eq("id", dispute_id).limit(1).execute().data
    if not rows:
        raise ValueError("Dispute not found")
    d = rows[0]

    now = datetime.now(timezone.utc).isoformat()
    recovered_amount = float(recovered_amount or 0)
    fields = {"status": status, "responded_at": now, "notes": notes or d.get("notes")}
    if status in ("recovered", "rejected"):
        fields["closed_at"] = now
    if recovered_amount:
        fields["total_recovered"] = round(recovered_amount, 2)
    db.table("disputes").update(fields).eq("id", dispute_id).execute()

    if recovered_amount:
        items = fetch_all(db, "dispute_items", "*",
                          filters=[("eq", ("dispute_id", dispute_id))])
        total_claimed = sum(float(i.get("claimed_amount") or 0) for i in items) or 1.0
        for it in items:
            share = round(recovered_amount * float(it.get("claimed_amount") or 0)
                          / total_claimed, 2)
            db.table("dispute_items").update({
                "recovered_amount": share, "status": "recovered",
            }).eq("id", it["id"]).execute()
            if it.get("case_id"):
                db.table("exception_cases").update({
                    "recovered_amount": share, "workflow_status": "recovered",
                    "updated_at": now,
                }).eq("id", it["case_id"]).execute()
                db.table("reconciliation_items").update({
                    "is_resolved": True, "resolved_at": now,
                    "resolution_notes": f"Recovered ${share:,.2f} via dispute",
                }).eq("case_id", it["case_id"]).execute()

    audit(db, "disputes", dispute_id, "dispute_outcome", {"status": d["status"]},
          {"status": status, "recovered": recovered_amount},
          reason=(notes or "")[:400], actor=actor)
    return {**d, **fields}
