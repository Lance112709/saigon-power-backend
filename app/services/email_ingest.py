"""Automatic commission-statement import straight from the email inbox.

Runs hourly (scheduled in main.py) and on demand (POST /uploads/poll-email):

  1. Connects to the mailbox over IMAP (Gmail app password).
  2. Scans recent messages for Excel attachments.
  3. Every attachment is fingerprinted by the provider parser — ONLY files
     that match a known commission-statement format are imported (insurance
     statements, invoices, random spreadsheets are ignored). Files already
     imported are skipped by hash, so re-runs and re-sent emails are safe.
  4. Recognized statements run the full pipeline: store original → match
     deals → backfill ESI IDs → import → status-sync → reconcile.
  5. Processed messages get the Gmail label "CRM-Imported"; a summary alert
     is written so the office sees what arrived without opening email.

Config (Railway variables):
  GMAIL_USER          the mailbox that receives statements
  GMAIL_APP_PASSWORD  a Google App Password for that account
  INGEST_LOOKBACK_DAYS (optional, default 40)
"""
import email
import email.utils
import imaplib
import os
from datetime import datetime, timedelta, timezone

from app.db.client import get_client
from app.services.audit import audit
from app.services.file_parser.provider_parsers import detect_and_parse
from app.services.reconciliation_v2 import get_or_create_supplier

IMAP_HOST = os.environ.get("INGEST_IMAP_HOST", "imap.gmail.com")
LABEL = "CRM-Imported"
MAX_ATTACHMENTS_PER_RUN = 25


def _config():
    return (os.environ.get("GMAIL_USER", "").strip(),
            os.environ.get("GMAIL_APP_PASSWORD", "").replace(" ", "").strip())


def poll_inbox(actor: str = "email-ingest") -> dict:
    user, password = _config()
    if not user or not password:
        return {"ok": False,
                "error": "Email ingest isn't configured — set GMAIL_USER and GMAIL_APP_PASSWORD in Railway."}

    lookback = int(os.environ.get("INGEST_LOOKBACK_DAYS", "40"))
    since = (datetime.now(timezone.utc) - timedelta(days=lookback)).strftime("%d-%b-%Y")

    db = get_client()
    imported, skipped_known, unrecognized, errors = [], 0, [], []

    try:
        imap = imaplib.IMAP4_SSL(IMAP_HOST)
        imap.login(user, password)
        imap.select("INBOX")
    except Exception as e:
        return {"ok": False, "error": f"Could not log in to {IMAP_HOST} as {user}: {str(e)[:150]}"}

    try:
        # candidate messages: recent, not yet labeled by us
        status, data = imap.search(None, f'(SINCE {since} NOT X-GM-LABELS "{LABEL}")')
        ids = (data[0].split() if status == "OK" and data and data[0] else [])[-100:]

        processed_count = 0
        for mid in reversed(ids):  # newest first
            if processed_count >= MAX_ATTACHMENTS_PER_RUN:
                break
            try:
                status, msg_data = imap.fetch(mid, "(RFC822)")
                if status != "OK" or not msg_data or not msg_data[0]:
                    continue
                msg = email.message_from_bytes(msg_data[0][1])
                sender = email.utils.parseaddr(msg.get("From", ""))[1]
                subject = str(email.header.make_header(email.header.decode_header(msg.get("Subject", ""))))[:150]

                touched = False
                for part in msg.walk():
                    fname = part.get_filename()
                    if not fname:
                        continue
                    fname = str(email.header.make_header(email.header.decode_header(fname)))
                    if not fname.lower().endswith((".xlsx", ".xls", ".pdf")):
                        continue
                    blob = part.get_payload(decode=True)
                    if not blob or len(blob) > 30_000_000:
                        continue
                    processed_count += 1

                    parsed = detect_and_parse(blob, fname)
                    if not parsed:
                        unrecognized.append({"file": fname, "from": sender, "subject": subject})
                        continue

                    # already imported? (hash-level idempotency)
                    exists = db.table("upload_batches").select("id").eq("file_hash", parsed["file_hash"]).limit(1).execute().data
                    if exists:
                        skipped_known += 1
                        touched = True
                        continue

                    from app.api.v1.uploads import _process_rows, _storage_put
                    ext = fname.rsplit(".", 1)[-1].lower()
                    _storage_put(db, f"{parsed['file_hash']}.{ext}", blob, "application/octet-stream")
                    sup_id = get_or_create_supplier(db, parsed["supplier"])
                    batch = db.table("upload_batches").insert({
                        "supplier_id": sup_id, "original_filename": fname,
                        "storage_path": f"statements/{parsed['file_hash']}.{ext}",
                        "file_type": ext, "file_hash": parsed["file_hash"], "status": "parsing",
                        "ai_column_mapping": {"auto": True, "provider_group": parsed["provider_group"],
                                              "statement_label": parsed["statement_label"],
                                              "labels": parsed["labels"], "detector": "fingerprint-v1",
                                              "email_ingest": {"from": sender, "subject": subject,
                                                               "date": msg.get("Date", "")[:40]}},
                        "rows_parsed": parsed["row_count"],
                    }).execute().data[0]

                    result = _process_rows(db, batch["id"], parsed["provider_group"], sup_id,
                                           parsed["rows"], None, actor,
                                           parsed["warnings"], parsed["going_final"])
                    imported.append({
                        "file": fname, "from": sender,
                        "provider": parsed["provider_group"],
                        "months": parsed["labels"],
                        "rows": result["rows_imported"],
                        "total": result["total_affinity_amount"],
                        "issues": sum((r.get("missing", 0) + r.get("short_paid", 0) + r.get("over_paid", 0))
                                      for r in result.get("runs", [])),
                    })
                    touched = True
                    audit(db, "upload_batches", batch["id"], "email_auto_import", None,
                          {"from": sender, "subject": subject, "file": fname,
                           "rows": result["rows_imported"], "total": result["total_affinity_amount"]},
                          reason="Statement auto-imported from email", actor=actor)

                if touched:
                    try:
                        imap.store(mid, "+X-GM-LABELS", f'"{LABEL}"')
                    except Exception:
                        pass
            except Exception as e:
                errors.append(str(e)[:150])
    finally:
        try:
            imap.logout()
        except Exception:
            pass

    # office-visible summary
    if imported:
        lines = "; ".join(f"{i['provider']} {'/'.join(i['months'][:2])}: {i['rows']} rows ${i['total']:,.0f}"
                          f" ({i['issues']} issues)" for i in imported)
        try:
            db.table("ai_alerts").insert({
                "type": "email_ingest", "entity_type": "system", "entity_id": datetime.now(timezone.utc).strftime("%Y%m%d%H%M"),
                "message": f"📬 Auto-imported {len(imported)} statement(s) from email — {lines}. "
                           f"Review any issues on the Reconciliation page.",
                "severity": "low", "status": "open", "metadata": {"imported": imported},
            }).execute()
        except Exception:
            pass

    return {"ok": True, "imported": imported, "already_imported": skipped_known,
            "unrecognized": unrecognized[:10], "errors": errors[:5],
            "checked_messages": len(ids) if 'ids' in dir() else 0}
