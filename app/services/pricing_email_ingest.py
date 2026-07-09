"""Phase 2: automatic pricing-matrix import straight from the inbox.

Runs weekday mornings (scheduled in main.py) and on demand from the pricing
admin page. Flow per message:

  1. Connect over IMAP to the pricing mailbox. NRG sends the daily matrix to
     lance@saigonllc.com; statements go to commission@ — so the mailbox is
     configurable separately and falls back to the statement-ingest account:
       PRICING_GMAIL_USER / PRICING_GMAIL_APP_PASSWORD  (preferred)
       GMAIL_USER / GMAIL_APP_PASSWORD                  (fallback)
  2. Scan recent messages for Excel/CSV attachments not yet labeled.
  3. Every attachment is offered to each active provider's pricing parser —
     the parser's table fingerprint is the gate, so commission statements,
     invoices, and random files are ignored automatically.
  4. Recognized matrices import through the same create_upload_from_bytes()
     the manual wizard uses (hash-idempotent), then auto-publish when the
     provider's auto_publish flag is on — agents' screens pick the new
     version up through their version poll within seconds.
  5. Processed messages get the Gmail label "CRM-Pricing-Imported" and an
     office-visible alert is written.
"""
import email
import email.header
import email.utils
import imaplib
import os
from datetime import datetime, timedelta, timezone

from app.db.client import get_client
from app.services.audit import audit
from app.services.pricing_parser import PRICING_PARSERS

IMAP_HOST = os.environ.get("INGEST_IMAP_HOST", "imap.gmail.com")
LABEL = "CRM-Pricing-Imported"
MAX_ATTACHMENTS_PER_RUN = 10


def _config():
    user = (os.environ.get("PRICING_GMAIL_USER") or os.environ.get("GMAIL_USER") or "").strip()
    pw = (os.environ.get("PRICING_GMAIL_APP_PASSWORD") or os.environ.get("GMAIL_APP_PASSWORD") or "")
    return user, pw.replace(" ", "").strip()


def poll_pricing_inbox(actor: str = "pricing-email-ingest") -> dict:
    from app.api.v1.commercial_pricing import create_upload_from_bytes, publish_upload_internal

    user, password = _config()
    if not user or not password:
        return {"ok": False,
                "error": "Pricing email ingest isn't configured — set PRICING_GMAIL_USER and "
                         "PRICING_GMAIL_APP_PASSWORD (or GMAIL_USER/GMAIL_APP_PASSWORD) in Railway."}

    db = get_client()
    providers = [p for p in (db.table("pricing_providers").select("*").eq("active", True).execute().data or [])
                 if p["code"].upper() in PRICING_PARSERS]
    if not providers:
        return {"ok": False, "error": "No active pricing providers with parsers."}

    since = (datetime.now(timezone.utc) - timedelta(days=3)).strftime("%d-%b-%Y")
    imported, published, skipped_known, ignored, errors = [], [], 0, [], []

    try:
        imap = imaplib.IMAP4_SSL(IMAP_HOST)
        imap.login(user, password)
        imap.select("INBOX")
    except Exception as e:
        return {"ok": False, "error": f"Could not log in to {IMAP_HOST} as {user}: {str(e)[:150]}"}

    try:
        status, data = imap.search(None, f'(SINCE {since} NOT X-GM-LABELS "{LABEL}")')
        ids = (data[0].split() if status == "OK" and data and data[0] else [])[-60:]
        processed = 0
        for mid in reversed(ids):  # newest first — today's matrix wins
            if processed >= MAX_ATTACHMENTS_PER_RUN:
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
                    if not fname.lower().endswith((".xlsx", ".xls", ".xlsm", ".csv")):
                        continue
                    blob = part.get_payload(decode=True)
                    if not blob or len(blob) > 40_000_000:
                        continue
                    processed += 1

                    upload = None
                    for prov in providers:
                        try:
                            upload = create_upload_from_bytes(db, prov, blob, fname, actor)
                            break
                        except ValueError:
                            continue          # not this provider's matrix
                        except Exception as e:
                            detail = getattr(e, "detail", str(e))
                            if "already imported" in str(detail):
                                skipped_known += 1
                                touched = True
                                upload = "dup"
                                break
                            raise
                    if upload is None:
                        ignored.append({"file": fname, "from": sender})
                        continue
                    if upload == "dup":
                        continue

                    touched = True
                    entry = {"file": fname, "from": sender, "subject": subject,
                             "version": upload["version"], "rows": upload["rows_imported"],
                             "provider": next(p["name"] for p in providers if p["id"] == upload["provider_id"])}
                    imported.append(entry)
                    prov = next(p for p in providers if p["id"] == upload["provider_id"])
                    if prov.get("auto_publish", True):
                        publish_upload_internal(db, upload["id"], actor)
                        published.append(entry)

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

    if imported:
        lines = "; ".join(f"{i['provider']} v{i['version']} ({i['rows']:,} rates"
                          f"{', published' if i in published else ', draft — needs publish'})"
                          for i in imported)
        try:
            db.table("ai_alerts").insert({
                "type": "pricing_ingest", "entity_type": "system",
                "entity_id": datetime.now(timezone.utc).strftime("%Y%m%d%H%M"),
                "message": f"💲 Today's pricing imported from email — {lines}.",
                "severity": "low", "status": "open", "metadata": {"imported": imported},
            }).execute()
        except Exception:
            pass

    return {"ok": True, "imported": imported, "published": [e["version"] for e in published],
            "already_imported": skipped_known, "ignored": ignored[:10], "errors": errors[:5]}
