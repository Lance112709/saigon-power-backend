"""Pre-payout checklist for the admin: everything that needs a decision or a
fix before a month's agent commissions can be paid, as JSON (for the page)
and as one email (scheduled after the provider statements have arrived).
"""
import json
import logging
import os
from datetime import date, datetime, timezone

from app.db.client import get_client
from app.services.agent_commission_engine import calculate_month, norm_name
from app.services.agent_names import agent_name_report

log = logging.getLogger("saigon.commission_digest")
ADMIN_EMAIL = os.environ.get("ADMIN_ALERT_EMAIL", "lance112709@gmail.com")
FROM_EMAIL = os.environ.get("REMINDER_FROM_EMAIL", "hello@saigonllc.com")
FRONTEND = os.environ.get("FRONTEND_URL", "https://saigon-power-frontend.vercel.app")
SWING_PCT = 30.0


def _prev_months(y: int, m: int, n: int) -> list:
    out = []
    for _ in range(n):
        m -= 1
        if m == 0:
            y, m = y - 1, 12
        out.append((y, m))
    return out


def build_digest(year: int, month: int, db=None, result: dict = None) -> dict:
    """Items grouped by what the admin has to do. Each item: {agent, text, link?}."""
    db = db or get_client()
    result = result or calculate_month(db, year, month)
    label = f"{year}-{month:02d}"
    items = {"held": [], "unregistered_agents": [], "no_agent": [], "no_plan": [], "swings": [],
             "name_fixes": [], "unpaid_older": [], "clawbacks": []}
    page = f"{FRONTEND}/admin/commissions"

    # 1. held enrollment bonuses (judgment calls only — mechanical ones are auto-decided)
    for agent, b in result["agents"].items():
        for d in b.get("deals", []):
            if d.get("held") and d.get("hold_reason") not in ("rejected",):
                items["held"].append({"agent": agent, "text": f"{d.get('customer') or 'unknown'} — {d.get('hold_reason')}",
                                      "link": page})
        for d in b.get("deals", []):
            if d.get("kind") == "clawback":
                items["clawbacks"].append({"agent": agent, "text": f"{d.get('customer')} cancelled {d.get('cancelled')} → {d['commission']:+.2f}"})

    # 2. paid accounts credited to names that are not registered agents
    for name, gross in (result.get("unassigned", {}).get("agent_not_registered") or {}).items():
        items["unregistered_agents"].append({"agent": name, "text": f"${gross:,.2f} of provider payments on deals credited to '{name}', who is not a registered agent",
                                             "link": f"{FRONTEND}/crm/agents"})
    na = result.get("unassigned", {}).get("no_agent_on_deal") or {}
    if na.get("esiids"):
        items["no_agent"].append({"agent": "", "text": f"{na['esiids']} paid accounts (${na.get('gross', 0):,.2f}) whose deal has no sales agent"})

    # 3. agents with activity but no plan
    for w in result.get("warnings", []):
        if "NO commission plan" in w:
            items["no_plan"].append({"agent": w.split(" has ")[0], "text": w, "link": f"{FRONTEND}/crm/agents"})

    # 4. payout swings vs the previous 3 months
    prev = {}
    for py, pm in _prev_months(year, month, 3):
        for r in db.table("agent_commissions").select("agent_name,total_commission").eq("year", py).eq("month", pm).execute().data or []:
            prev.setdefault(norm_name(r["agent_name"]), []).append(float(r.get("total_commission") or 0))
    for agent, b in result["agents"].items():
        hist = prev.get(norm_name(agent)) or []
        if len(hist) < 2:
            continue
        avg = sum(hist) / len(hist)
        if avg <= 0:
            continue
        pct = (b["total"] - avg) / avg * 100
        if abs(pct) >= SWING_PCT and abs(b["total"] - avg) >= 25:
            items["swings"].append({"agent": agent, "text": f"${b['total']:,.2f} this month vs ${avg:,.2f} average of the last {len(hist)} — {pct:+.0f}%"})

    # 5. agent spellings that split payouts
    try:
        rep = agent_name_report(db, month_label=label)
        for raw, info in rep["fixable"].items():
            items["name_fixes"].append({"agent": info["canonical"], "text": f"{info['deals']} deal(s) say '{raw}' instead of '{info['canonical']}'",
                                        "fix": {"from": raw, "to": info["canonical"]}})
        for raw, u in rep["unknown"].items():
            sug = u.get("suggested")
            items["name_fixes"].append({"agent": raw,
                                        "text": f"{u['deals']} deal(s) starting {label} credited to '{raw}', not a registered agent"
                                                + (f" — probably {sug}" if sug else ""),
                                        "fix": {"from": raw, "to": sug} if sug else None,
                                        "link": f"{FRONTEND}/crm/agents"})
    except Exception:
        log.exception("agent name report failed")

    # 6. older months still not paid
    rows = db.table("agent_commissions").select("agent_name,year,month,status,total_commission") \
        .neq("status", "paid").execute().data or []
    for r in rows:
        if (r["year"], r["month"]) < (year, month) and float(r.get("total_commission") or 0) > 0:
            items["unpaid_older"].append({"agent": r["agent_name"],
                                          "text": f"{date(r['year'], r['month'], 1).strftime('%b %Y')} ${float(r['total_commission']):,.2f} still '{r['status']}'"})

    total = sum(len(v) for v in items.values())
    return {"label": label, "count": total, "items": items,
            "agents": len(result["agents"]), "payout_total": round(sum(b["total"] for b in result["agents"].values()), 2)}


TITLES = {
    "held": "Held enrollment bonuses — Release or Reject",
    "unregistered_agents": "Deals credited to unregistered agent names",
    "no_agent": "Paid accounts with no agent on the deal",
    "no_plan": "Agents with activity but no commission plan",
    "swings": "Payouts that moved ±30% vs the last 3 months",
    "name_fixes": "Agent name spellings to fix",
    "clawbacks": "Clawbacks this month (early cancellations)",
    "unpaid_older": "Earlier months still not paid",
}


def digest_html(d: dict) -> str:
    parts = [f"<p>Agent commissions for <b>{d['label']}</b>: {d['agents']} agents, ${d['payout_total']:,.2f} calculated.</p>"]
    if d["count"] == 0:
        parts.append("<p>Nothing needs your attention — ready to approve and pay.</p>")
    for key, title in TITLES.items():
        rows = d["items"].get(key) or []
        if not rows:
            continue
        parts.append(f"<h3 style='margin:14px 0 4px'>{title} ({len(rows)})</h3><ul style='margin:0'>")
        for r in rows:
            who = f"<b>{r['agent']}</b> · " if r.get("agent") else ""
            link = f" <a href='{r['link']}'>open</a>" if r.get("link") else ""
            parts.append(f"<li>{who}{r['text']}{link}</li>")
        parts.append("</ul>")
    parts.append(f"<p style='margin-top:16px'><a href='{FRONTEND}/admin/commissions'>Open Commission Management</a></p>")
    return "".join(parts)


def send_digest(year: int = None, month: int = None) -> dict:
    """Build and email the checklist for the month being paid (the previous
    calendar month unless given)."""
    today = date.today()
    if not year or not month:
        (year, month), = _prev_months(today.year, today.month, 1)
    d = build_digest(year, month)
    try:
        import resend
    except Exception:
        resend = None
    sent = False
    if resend is not None:
        if not getattr(resend, "api_key", None):
            resend.api_key = os.environ.get("RESEND_API_KEY", "")
        if resend.api_key:
            try:
                resend.Emails.send({
                    "from": f"Saigon CRM <{FROM_EMAIL}>", "to": [ADMIN_EMAIL],
                    "subject": (f"Commission checklist {d['label']}: {d['count']} item(s) need you"
                                if d["count"] else f"Commission checklist {d['label']}: ready to pay"),
                    "html": digest_html(d),
                })
                sent = True
            except Exception:
                log.exception("digest email failed")
    return {"sent": sent, "to": ADMIN_EMAIL, **d}
