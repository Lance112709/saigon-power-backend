from fastapi import APIRouter, HTTPException, Query, Body, Depends
from typing import Optional
from datetime import datetime, timezone
from app.db.client import get_client
from app.auth.deps import get_current_user, require_manager, UserContext

router = APIRouter()

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

def _notify_signed(db, p: dict) -> None:
    """Email the assigned sales agent + all admins the moment a contract is
    signed. Best-effort — never blocks the customer's signing."""
    try:
        from app.services.customer_email import send_email
        agent_name = None
        if p.get("lead_id"):
            lead = db.table("leads").select("sales_agent").eq("id", p["lead_id"]).limit(1).execute().data
            agent_name = (lead[0].get("sales_agent") if lead else None) or None

        recipients = set()
        for u in (db.table("users").select("email, role, sales_agent_name, status").execute().data or []):
            email = (u.get("email") or "").strip()
            if not email or "@" not in email or (u.get("status") and u.get("status") != "active"):
                continue
            if u.get("role") == "admin":
                recipients.add(email)
            if agent_name and (u.get("sales_agent_name") or "").strip().lower() == agent_name.strip().lower():
                recipients.add(email)
        if not recipients:
            return

        name = p.get("customer_name") or "A customer"
        rate = f"${float(p['rate']):.4f}/kWh" if p.get("rate") is not None else "—"
        term = f"{p['term_months']} months" if p.get("term_months") else "—"
        bill = f"${float(p['est_monthly_bill']):.2f}/mo" if p.get("est_monthly_bill") is not None else "—"
        rows = [
            ("Customer", name), ("Phone", p.get("customer_phone") or "—"),
            ("Email", p.get("customer_email") or "—"),
            ("Address", p.get("customer_address") or p.get("service_address") or "—"),
            ("Provider", p.get("rep_name") or "—"), ("Plan", p.get("plan_name") or "—"),
            ("Rate", rate), ("Term", term), ("Est. bill", bill),
            ("Signed", "just now"),
        ]
        tr = "".join(
            f"<tr><td style='padding:6px 14px;color:#64748b;font-size:13px;'>{k}</td>"
            f"<td style='padding:6px 14px;font-weight:600;color:#0f1d3d;font-size:13px;'>{v}</td></tr>"
            for k, v in rows)
        html = (
            f"<div style='font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;'>"
            f"<h2 style='color:#0f1d3d;'>🎉 {name} just signed the contract</h2>"
            f"<p style='color:#475569;font-size:14px;'>The enrollment is ready to submit. Details:</p>"
            f"<table style='border-collapse:collapse;background:#f8fafc;border-radius:10px;'>{tr}</table>"
            f"<p style='color:#94a3b8;font-size:12px;margin-top:16px;'>Open the CRM → Proposals to view the signed contract.</p></div>")
        subject = f"🎉 Contract signed — {name}"
        for to in recipients:
            send_email(to, subject, html)
    except Exception:
        pass


def _assert_proposal_access(db, user: UserContext, prop: dict) -> None:
    """Sales agents may only touch proposals tied to their own leads."""
    if not user.is_sales_agent:
        return
    agent_name = (user.sales_agent_name or "").strip().lower()
    lead_id = prop.get("lead_id")
    if not agent_name or not lead_id:
        raise HTTPException(status_code=403, detail="Access denied")
    lead = db.table("leads").select("sales_agent").eq("id", lead_id).limit(1).execute()
    if not lead.data or (lead.data[0].get("sales_agent") or "").lower() != agent_name:
        raise HTTPException(status_code=403, detail="Access denied")

# ── Declared BEFORE /{id} routes to avoid path conflicts ──────────────────────

@router.get("/view/{token}")
def view_proposal(token: str):
    db = get_client()
    res = db.table("proposals").select("*").eq("token", token).limit(1).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Proposal not found")
    p = res.data[0]
    if p["status"] == "draft":
        db.table("proposals").update({"status": "sent", "updated_at": _now()}).eq("token", token).execute()
        p["status"] = "sent"
    elif p["status"] == "sent":
        db.table("proposals").update({"status": "viewed", "updated_at": _now()}).eq("token", token).execute()
        p["status"] = "viewed"

    # Enrich with lead data (dob, ssn/anxh) and latest active deal (esiid, service address, start date)
    if p.get("lead_id"):
        try:
            lead = db.table("leads").select("dob, anxh, email, phone").eq("id", p["lead_id"]).limit(1).execute()
            if lead.data:
                l = lead.data[0]
                p.setdefault("dob", l.get("dob"))
                p.setdefault("anxh", l.get("anxh"))
                if not p.get("customer_email"):
                    p["customer_email"] = l.get("email")
                if not p.get("customer_phone"):
                    p["customer_phone"] = l.get("phone")
        except Exception:
            pass
        try:
            deals = db.table("lead_deals").select("esiid, service_address, service_city, service_state, service_zip, start_date") \
                .eq("lead_id", p["lead_id"]).order("created_at", desc=True).limit(1).execute()
            if deals.data:
                d = deals.data[0]
                p.setdefault("esi_id", d.get("esiid"))
                p.setdefault("service_address", ", ".join(filter(None, [
                    d.get("service_address"), d.get("service_city"),
                    d.get("service_state"), d.get("service_zip")
                ])))
                p.setdefault("start_date", d.get("start_date"))
        except Exception:
            pass

    return p

@router.post("/accept/{token}")
def accept_proposal(token: str, data: dict = Body(...)):
    db = get_client()
    res = db.table("proposals").select("*").eq("token", token).limit(1).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Proposal not found")
    p = res.data[0]
    if p["status"] == "accepted":
        raise HTTPException(status_code=400, detail="Proposal already accepted")
    if p["status"] == "rejected":
        raise HTTPException(status_code=400, detail="Proposal has been rejected")

    signature = str(data.get("signature") or "").strip()
    if not signature:
        raise HTTPException(status_code=400, detail="Signature is required")

    deal_id = None
    if p.get("lead_id"):
        try:
            deal_payload = {
                "lead_id":         p["lead_id"],
                "status":          "Future",
                "supplier":        p.get("rep_name"),
                "plan_name":       p.get("plan_name"),
                "rate":            float(p["rate"]) if p.get("rate") else None,
                "contract_term":   f"{p['term_months']} Months" if p.get("term_months") else None,
                "sales_agent":     None,
                "flag_tos":        False,
                "flag_toao":       False,
                "flag_deposit":    False,
                "flag_special_deal": False,
                "flag_promo_10":   False,
                "notes":           f"Created from accepted proposal. Est. monthly bill: ${p.get('est_monthly_bill') or 'N/A'}",
            }
            deal_res = db.table("lead_deals").insert(deal_payload).execute()
            if deal_res.data:
                deal_id = deal_res.data[0]["id"]
        except Exception as e:
            pass

    db.table("proposals").update({
        "status":       "accepted",
        "signature":    signature,
        "accepted_at":  _now(),
        "deal_id":      deal_id,
        "updated_at":   _now(),
    }).eq("token", token).execute()

    # Instantly notify the assigned agent + admins that the customer signed.
    _notify_signed(db, p)

    return {"ok": True, "deal_id": deal_id}

# ── List + Create ─────────────────────────────────────────────────────────────

@router.get("")
def list_proposals(
    status:   Optional[str] = Query(None),
    lead_id:  Optional[str] = Query(None),
    limit:    int           = Query(50),
    offset:   int           = Query(0),
    user: UserContext = Depends(get_current_user),
):
    db = get_client()

    # Sales agents only see proposals tied to their own leads
    if user.is_sales_agent:
        u = db.table("users").select("sales_agent_name").eq("id", user.user_id).limit(1).execute()
        agent_name = (u.data[0].get("sales_agent_name") or "").strip() or None
        if not agent_name:
            return []
        scoped = db.table("leads").select("id").eq("sales_agent", agent_name).execute()
        scoped_ids = [r["id"] for r in (scoped.data or [])]
        if not scoped_ids:
            return []
        if lead_id and lead_id not in scoped_ids:
            return []
        q = db.table("proposals").select("*").in_("lead_id", scoped_ids)
    else:
        q = db.table("proposals").select("*")
        if lead_id:
            q = q.eq("lead_id", lead_id)

    if status:
        q = q.eq("status", status)
    res = q.order("created_at", desc=True).range(offset, offset + limit - 1).execute()
    return res.data or []

@router.post("")
def create_proposal(data: dict = Body(...), user: UserContext = Depends(get_current_user)):
    db = get_client()
    name = str(data.get("customer_name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="customer_name is required")

    def _f(key):
        v = data.get(key)
        try:
            return float(v) if v not in (None, "", "null") else None
        except (ValueError, TypeError):
            return None

    def _i(key):
        v = data.get(key)
        try:
            return int(v) if v not in (None, "", "null") else None
        except (ValueError, TypeError):
            return None

    payload = {
        "lead_id":              data.get("lead_id") or None,
        "customer_id":          data.get("customer_id") or None,
        "customer_name":        name,
        "customer_phone":       str(data.get("customer_phone") or "").strip() or None,
        "customer_email":       str(data.get("customer_email") or "").strip() or None,
        "customer_address":     str(data.get("customer_address") or "").strip() or None,
        "rep_name":             str(data.get("rep_name") or "").strip() or None,
        "plan_name":            str(data.get("plan_name") or "").strip() or None,
        "rate":                 _f("rate"),
        "term_months":          _i("term_months"),
        "est_monthly_bill":     _f("est_monthly_bill"),
        "early_termination_fee": _f("early_termination_fee"),
        "notes":                str(data.get("notes") or "").strip() or None,
        "status":               "draft",
    }
    res = db.table("proposals").insert(payload).execute()
    if not res.data:
        raise HTTPException(status_code=500, detail="Failed to create proposal")
    created = res.data[0]

    if created.get("customer_phone"):
        try:
            from app.services.sms import send_automated
            send_automated(
                "proposal_sent",
                created["customer_phone"],
                {
                    "first_name": (created["customer_name"] or "").split()[0],
                    "plan_name":  created.get("plan_name") or "our energy plan",
                    "rep_name":   created.get("rep_name") or "Your Saigon Power rep",
                },
                lead_id=created.get("lead_id"),
                customer_id=created.get("customer_id"),
            )
        except Exception:
            pass

    return created

# ── Single proposal ───────────────────────────────────────────────────────────

@router.post("/{proposal_id}/email")
def email_contract(proposal_id: str, data: dict = Body(default={}), user: UserContext = Depends(get_current_user)):
    """Email the customer their contract — the signing link, or the signed
    PDF attached once it's been executed."""
    from app.services.customer_email import send_email, contract_email_html, fetch_signed_pdf_attachment
    from app.services.audit import audit
    db = get_client()
    res = db.table("proposals").select("*").eq("id", proposal_id).limit(1).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Proposal not found")
    prop = res.data[0]
    _assert_proposal_access(db, user, prop)

    to = (data.get("email") or prop.get("customer_email") or "").strip()
    if not to or "@" not in to:
        raise HTTPException(status_code=400, detail="No email on file for this customer — add one first.")
    if data.get("email") and data["email"].strip() != (prop.get("customer_email") or ""):
        db.table("proposals").update({"customer_email": to, "updated_at": _now()}).eq("id", proposal_id).execute()

    signed = bool(prop.get("signed_contract_url"))
    attachments = None
    if signed:
        att = fetch_signed_pdf_attachment(db, prop["signed_contract_url"])
        attachments = [att] if att else None

    subject = ("Your signed Saigon Power contract 🎉" if signed
               else f"Your Saigon Power contract is ready to sign{' — ' + prop['plan_name'] if prop.get('plan_name') else ''}")
    result = send_email(to, subject, contract_email_html(prop), attachments)
    if not result.get("ok"):
        raise HTTPException(status_code=503, detail=result.get("error", "Email failed"))

    if not signed and prop.get("status") in (None, "draft", "created"):
        db.table("proposals").update({"status": "sent", "updated_at": _now()}).eq("id", proposal_id).execute()
    audit(db, "proposals", proposal_id, "emailed_contract", None,
          {"to": to, "signed_pdf_attached": bool(attachments)},
          reason="Contract emailed from CRM", actor=user.email or "staff")
    return {"ok": True, "to": to, "attached_signed_pdf": bool(attachments)}


@router.get("/{proposal_id}")
def get_proposal(proposal_id: str, user: UserContext = Depends(get_current_user)):
    db = get_client()
    res = db.table("proposals").select("*").eq("id", proposal_id).limit(1).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Proposal not found")
    p = res.data[0]
    if user.is_sales_agent and p.get("lead_id"):
        u = db.table("users").select("sales_agent_name").eq("id", user.user_id).limit(1).execute()
        agent_name = (u.data[0].get("sales_agent_name") or "").strip() or None
        if not agent_name:
            raise HTTPException(status_code=403, detail="Access denied")
        lead = db.table("leads").select("sales_agent").eq("id", p["lead_id"]).limit(1).execute()
        if not lead.data or (lead.data[0].get("sales_agent") or "").lower() != agent_name.lower():
            raise HTTPException(status_code=403, detail="Access denied")
    return p

@router.patch("/{proposal_id}")
def update_proposal(proposal_id: str, data: dict = Body(...), user: UserContext = Depends(get_current_user)):
    db = get_client()
    existing = db.table("proposals").select("lead_id").eq("id", proposal_id).limit(1).execute()
    if not existing.data:
        raise HTTPException(status_code=404, detail="Proposal not found")
    _assert_proposal_access(db, user, existing.data[0])
    allowed = {"status", "rep_name", "plan_name", "rate", "term_months",
               "est_monthly_bill", "early_termination_fee", "notes"}
    payload = {k: v for k, v in data.items() if k in allowed}
    if not payload:
        raise HTTPException(status_code=400, detail="No valid fields to update")
    payload["updated_at"] = _now()
    res = db.table("proposals").update(payload).eq("id", proposal_id).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Proposal not found")
    return res.data[0]


@router.delete("/{proposal_id}")
def delete_proposal(proposal_id: str, user: UserContext = Depends(require_manager)):
    """Delete a proposal (admin/manager only) — e.g. to clear out test entries."""
    db = get_client()
    existing = db.table("proposals").select("id").eq("id", proposal_id).limit(1).execute()
    if not existing.data:
        raise HTTPException(status_code=404, detail="Proposal not found")
    db.table("proposals").delete().eq("id", proposal_id).execute()
    return {"ok": True}
