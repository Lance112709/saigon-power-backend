"""Bank deposits → commission statements.

Chase emails a "Your $X direct deposit posted to account ending in (...NNNN)"
alert for every incoming credit. The alert carries amount, posted date and
account only (no payer name). That is enough: almost every provider deposit
equals a statement's expected deposit to the cent, and APG&E / the NRG
residual statement are paid as 2–3 same-day deposits that sum to it.

Daily flow (scheduler): poll_chase_alerts() reads new alerts from the lance@
mailbox into bank_deposits, then match_deposits() records each matched deposit
on its statement (same fields the Payments page "Mark received" sets) and
tags the leftovers with the provider they most likely came from, so the
Payments page can show "deposit landed, statement missing".
"""
import email
import email.utils
import hashlib
import imaplib
import re
from datetime import date, datetime, timedelta, timezone
from itertools import combinations
from typing import Optional

from app.db.client import get_client
from app.services.audit import audit
from app.services.deposits import deposit_status, statement_month, TOLERANCE

ALERT_SENDER = "no.reply.alerts@chase.com"
ALERT_LABEL = "CRM-Deposit"
MIN_CHIP_AMOUNT = 150.0   # unmatched deposits below this stay in the table, off the strip

_AMT = re.compile(r"\$\s?([\d,]+\.\d{2})")
_POSTED = re.compile(r"Posted\s*\|?\s*([A-Z][a-z]{2} \d{1,2}, \d{4})")
_LAST4 = re.compile(r"\(\.\.\.(\d{4})\)")


def _month_add(m: str, n: int) -> str:
    y, mo = int(m[:4]), int(m[5:7]) + n
    while mo > 12:
        mo -= 12; y += 1
    while mo < 1:
        mo += 12; y -= 1
    return f"{y}-{mo:02d}"


# ── 1. ingest ────────────────────────────────────────────────────────────────

def parse_alert(subject: str, body: str, msg_date: str = "") -> Optional[dict]:
    """Amount / posted date / last4 from a Chase deposit alert. None if it is
    not a deposit alert (transfers out, card alerts, statements ...)."""
    s = subject or ""
    if "deposit posted" not in s.lower():
        return None
    m = _AMT.search(s) or _AMT.search(body or "")
    if not m:
        return None
    amount = float(m.group(1).replace(",", ""))
    posted = None
    pm = _POSTED.search(body or "")
    if pm:
        try:
            posted = datetime.strptime(pm.group(1), "%b %d, %Y").date()
        except ValueError:
            posted = None
    if not posted and msg_date:
        try:
            posted = email.utils.parsedate_to_datetime(msg_date).date()
        except Exception:
            posted = None
    if not posted:
        return None
    lm = _LAST4.search(s) or _LAST4.search(body or "")
    return {"amount": round(amount, 2), "posted_at": posted.isoformat(),
            "account_last4": lm.group(1) if lm else None}


def _body_text(msg) -> str:
    parts = []
    for part in msg.walk():
        ctype = part.get_content_type()
        if ctype in ("text/plain", "text/html"):
            try:
                txt = part.get_payload(decode=True).decode(part.get_content_charset() or "utf-8", "ignore")
            except Exception:
                continue
            if ctype == "text/html":
                txt = re.sub(r"<[^>]+>", " ", txt)
            parts.append(txt)
    return re.sub(r"\s+", " ", " ".join(parts))


def poll_chase_alerts(actor: str = "bank-alerts", lookback_days: int = 14, mailbox: str = "pricing") -> dict:
    """Read new Chase deposit alerts from the mailbox into bank_deposits, then match."""
    from app.services.email_ingest import _config, IMAP_HOST
    user, password = _config(mailbox)
    if not user or not password:
        return {"ok": False, "error": "Mailbox credentials not configured (PRICING_GMAIL_USER / PRICING_GMAIL_APP_PASSWORD)."}
    db = get_client()
    since = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).strftime("%d-%b-%Y")
    added, seen, skipped = [], 0, 0
    try:
        imap = imaplib.IMAP4_SSL(IMAP_HOST)
        imap.login(user, password)
        imap.select('"[Gmail]/All Mail"')
    except Exception as e:
        return {"ok": False, "error": f"Could not log in as {user}: {str(e)[:150]}"}
    try:
        status, data = imap.search(None, f'(FROM "{ALERT_SENDER}" SINCE {since} NOT X-GM-LABELS "{ALERT_LABEL}")')
        ids = (data[0].split() if status == "OK" and data and data[0] else [])[-300:]
        for mid in ids:
            try:
                st, msg_data = imap.fetch(mid, "(RFC822)")
                if st != "OK" or not msg_data or not msg_data[0]:
                    continue
                msg = email.message_from_bytes(msg_data[0][1])
                subject = str(email.header.make_header(email.header.decode_header(msg.get("Subject", ""))))
                seen += 1
                parsed = parse_alert(subject, _body_text(msg), msg.get("Date", ""))
                if not parsed:
                    skipped += 1
                    imap.store(mid, "+X-GM-LABELS", f'"{ALERT_LABEL}"')  # not a deposit alert; never re-read
                    continue
                ext = (msg.get("Message-ID") or "").strip() or hashlib.sha256(f"{subject}|{msg.get('Date','')}".encode()).hexdigest()
                exists = db.table("bank_deposits").select("id").eq("external_id", ext).limit(1).execute().data
                if not exists:
                    row = db.table("bank_deposits").insert({
                        "source": "chase_alert", "external_id": ext, "posted_at": parsed["posted_at"],
                        "amount": parsed["amount"], "account_last4": parsed["account_last4"],
                        "subject": subject[:200], "status": "unmatched",
                    }).execute().data[0]
                    added.append(row)
                imap.store(mid, "+X-GM-LABELS", f'"{ALERT_LABEL}"')
            except Exception as e:  # one bad message never stops the run
                skipped += 1
    finally:
        try:
            imap.logout()
        except Exception:
            pass
    match = match_deposits(db, actor=actor)
    return {"ok": True, "alerts_seen": seen, "deposits_added": len(added), "non_deposit_skipped": skipped, **match}


# ── 2. match ─────────────────────────────────────────────────────────────────

def _open_statements(db, lo_month: str, hi_month: str) -> list:
    """Confirmed statements with no deposit recorded, month in [lo, hi]."""
    rows, off = [], 0
    while True:
        page = db.table("upload_batches").select("*, suppliers(name, code)").eq("status", "confirmed") \
            .is_("amount_received", "null").order("created_at", desc=True).range(off, off + 999).execute().data or []
        rows.extend(page)
        if len(page) < 1000:
            break
        off += 1000
    out = []
    today = date.today()
    for b in rows:
        m = statement_month(b)
        if m and lo_month <= m <= hi_month:
            dep = deposit_status(b, today, m, db=db)
            if dep.get("expected_deposit") is not None:
                out.append({"id": b["id"], "supplier_id": b["supplier_id"], "provider": (b.get("suppliers") or {}).get("name") or "",
                            "month": m, "expected": float(dep["expected_deposit"]), "total": float(dep["statement_total"] or 0)})
    return out


def _record(db, stmt: dict, deposits: list, how: str, actor: str):
    amt = round(sum(float(d["amount"]) for d in deposits), 2)
    when = max(d["posted_at"] for d in deposits)
    parts = " + ".join(f"${float(d['amount']):,.2f} on {d['posted_at']}" for d in deposits)
    note = f"Chase deposit alert: {parts}. Matched automatically ({how})."
    now = datetime.now(timezone.utc).isoformat()
    db.table("upload_batches").update({"amount_received": amt, "received_at": when, "received_notes": note[:500],
                                       "received_by": actor, "received_recorded_at": now}).eq("id", stmt["id"]).execute()
    db.table("bank_deposits").update({"status": "matched", "upload_batch_id": stmt["id"], "matched_how": how, "updated_at": now}) \
        .in_("id", [d["id"] for d in deposits]).execute()
    audit(db, "upload_batches", stmt["id"], "deposit_recorded", {"amount_received": None},
          {"amount_received": amt, "received_at": when, "deposits": [d["id"] for d in deposits]},
          reason=f"Bank alert matched to {stmt['provider']} {stmt['month']} ({how})", actor=actor)


def match_deposits(db, actor: str = "bank-alerts") -> dict:
    deps = db.table("bank_deposits").select("*").eq("status", "unmatched").order("posted_at").execute().data or []
    if not deps:
        return {"matched": 0, "unmatched": 0}
    months = sorted({d["posted_at"][:7] for d in deps})
    stmts = _open_statements(db, _month_add(months[0], -2), months[-1])
    used = set(); matched = 0

    def cands(dep):
        m = dep["posted_at"][:7]
        return [s for s in stmts if s["id"] not in used and _month_add(m, -2) <= s["month"] <= m]

    # exact single deposits first
    for dep in deps:
        if dep["id"] in used:
            continue
        hits = [s for s in cands(dep) if abs(s["expected"] - float(dep["amount"])) <= TOLERANCE]
        if len(hits) == 1 or (hits and len({h["supplier_id"] for h in hits}) == 1):
            s = sorted(hits, key=lambda x: x["month"])[-1]
            _record(db, s, [dep], "exact amount", actor); used.add(dep["id"]); used.add(s["id"]); matched += 1
    # then 2–3 deposits within 3 days that sum to one statement (APG&E splits, NRG residual brands)
    rest = [d for d in deps if d["id"] not in used]
    for k in (2, 3):
        for combo in combinations(rest, k):
            if any(d["id"] in used for d in combo):
                continue
            days = [date.fromisoformat(d["posted_at"]) for d in combo]
            if (max(days) - min(days)).days > 3:
                continue
            total = round(sum(float(d["amount"]) for d in combo), 2)
            hits = [s for s in cands(combo[-1]) if abs(s["expected"] - total) <= TOLERANCE]
            if len(hits) == 1:
                _record(db, hits[0], list(combo), f"{k} deposits summed", actor)
                used.update(d["id"] for d in combo); used.add(hits[0]["id"]); matched += 1
    # leftovers: guess the provider from amount + pay-day pattern
    _tag_likely(db, [d for d in deps if d["id"] not in used])
    return {"matched": matched, "unmatched": len([d for d in deps if d["id"] not in used])}


def _tag_likely(db, deps: list):
    if not deps:
        return
    rows, off = [], 0
    while True:
        page = db.table("upload_batches").select("supplier_id,total_affinity_amount,total_withheld,received_at,ai_column_mapping,created_at,suppliers(name)") \
            .eq("status", "confirmed").order("created_at", desc=True).range(off, off + 999).execute().data or []
        rows.extend(page)
        if len(page) < 1000:
            break
        off += 1000
    profile = {}
    for b in rows:
        sid = b.get("supplier_id")
        if not sid:
            continue
        p = profile.setdefault(sid, {"name": (b.get("suppliers") or {}).get("name") or "", "amounts": [], "days": [], "months": set()})
        m = statement_month(b)
        if m:
            p["months"].add(m)
        if len(p["amounts"]) < 4 and b.get("total_affinity_amount") is not None:
            p["amounts"].append(float(b["total_affinity_amount"]) - float(b.get("total_withheld") or 0))
        if b.get("received_at"):
            p["days"].append(int(str(b["received_at"])[8:10]))
    now = datetime.now(timezone.utc).isoformat()
    for d in deps:
        amt = float(d["amount"]); day = int(d["posted_at"][8:10]); m = d["posted_at"][:7]
        best, best_score = None, 0.0
        for sid, p in profile.items():
            if not p["amounts"]:
                continue
            if m in p["months"] or _month_add(m, -1) in p["months"] and False:
                pass
            typical = sorted(p["amounts"])[len(p["amounts"]) // 2]
            if typical <= 0:
                continue
            closeness = 1 - min(1.0, abs(amt - typical) / typical)      # 1 = same size
            if closeness < 0.55:
                continue
            usual = sorted(p["days"])[len(p["days"]) // 2] if p["days"] else None
            day_score = 1 - min(1.0, abs(day - usual) / 10) if usual else 0.5
            score = closeness * 0.7 + day_score * 0.3
            if score > best_score:
                best, best_score = sid, score
        db.table("bank_deposits").update({"likely_supplier_id": best, "updated_at": now}).eq("id", d["id"]).execute()


def list_bank_deposits(db, status: Optional[str] = None, limit: int = 200) -> list:
    q = db.table("bank_deposits").select("*, likely:suppliers!bank_deposits_likely_supplier_id_fkey(name), batch:upload_batches(original_filename, ai_column_mapping, suppliers(name))") \
        .order("posted_at", desc=True).limit(limit)
    if status:
        q = q.eq("status", status)
    rows = q.execute().data or []
    for r in rows:
        r["likely_provider"] = (r.pop("likely", None) or {}).get("name")
        b = r.pop("batch", None) or {}
        r["statement"] = {"filename": b.get("original_filename"), "provider": (b.get("suppliers") or {}).get("name"),
                          "month": statement_month(b) if b else None} if b else None
    return rows


def assign_deposit(db, deposit_id: str, batch_id: str, actor: str) -> dict:
    dep = db.table("bank_deposits").select("*").eq("id", deposit_id).limit(1).execute().data
    b = db.table("upload_batches").select("*, suppliers(name)").eq("id", batch_id).limit(1).execute().data
    if not dep or not b:
        raise ValueError("Deposit or statement not found")
    dep, b = dep[0], b[0]
    stmt = {"id": b["id"], "provider": (b.get("suppliers") or {}).get("name") or "", "month": statement_month(b)}
    # add to whatever is already recorded on the statement (a second deposit for the same statement)
    prior = float(b.get("amount_received") or 0)
    amt = round(prior + float(dep["amount"]), 2)
    now = datetime.now(timezone.utc).isoformat()
    note = ((b.get("received_notes") or "") + f" + Chase deposit ${float(dep['amount']):,.2f} on {dep['posted_at']} (assigned by {actor}).").strip()
    db.table("upload_batches").update({"amount_received": amt, "received_at": dep["posted_at"], "received_notes": note[-500:],
                                       "received_by": actor, "received_recorded_at": now}).eq("id", b["id"]).execute()
    db.table("bank_deposits").update({"status": "matched", "upload_batch_id": b["id"], "matched_how": "assigned by admin", "updated_at": now}).eq("id", deposit_id).execute()
    audit(db, "upload_batches", b["id"], "deposit_recorded", {"amount_received": b.get("amount_received")},
          {"amount_received": amt, "received_at": dep["posted_at"], "deposit": deposit_id},
          reason=f"Bank deposit assigned to {stmt['provider']} {stmt['month']}", actor=actor)
    return {"ok": True, "amount_received": amt}
