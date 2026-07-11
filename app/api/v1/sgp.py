"""SGP Agent Commission Structure — admin API.

Agreement lifecycle, tier progress, permanent promotions, manual overrides,
idempotent recalculation, settings, and exports. Every mutation is audited.
Agent-facing data lives in agent_portal.overview (the `sgp` payload).
"""
import io
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.auth.deps import require_admin, UserContext
from app.db.client import get_client
from app.services.audit import audit
from app.services.reconciliation_v2 import fetch_all
from app.services.sgp_tiers import (
    _alert, evaluate_all, get_settings, is_eligible, load_tiers,
)

router = APIRouter()

CLASSIFICATIONS = ("SGP_AGENT", "REFERRAL_PARTNER", "INTERNAL_EMPLOYEE",
                   "TEAM_LEADER", "INACTIVE_AGENT")
AGREEMENT_STATUSES = ("NOT_SENT", "SENT", "PENDING_SIGNATURE", "SIGNED",
                      "APPROVED", "REJECTED", "EXPIRED", "TERMINATED")

_AGENT_COLS = ("id,name,email,phone,agent_type,agent_code,classification,agreement_status,"
               "agreement_version,agreement_signed_at,agreement_approved_at,agreement_effective_at,"
               "agreement_terminated_at,agreement_notes,agreement_doc_url,current_tier,"
               "tier_effective_from,sgp_suspended")


def _progress_counts(db, agent_ids: list) -> dict:
    out: dict = {}
    for i in range(0, len(agent_ids), 100):
        for r in fetch_all(db, "sgp_tier_progress", "agent_id,tier_order,qualifying_month,eligible_gp",
                           filters=[("in_", ("agent_id", agent_ids[i:i + 100]))]):
            out.setdefault(r["agent_id"], []).append(r)
    return out


@router.get("/tiers")
def get_tiers(user: UserContext = Depends(require_admin)):
    return load_tiers(get_client())


@router.get("/settings")
def read_settings(user: UserContext = Depends(require_admin)):
    return get_settings(get_client())


class SettingsBody(BaseModel):
    qualification_basis: Optional[str] = None
    promotion_effective_rule: Optional[str] = None


@router.patch("/settings")
def update_settings(body: SettingsBody, user: UserContext = Depends(require_admin)):
    db = get_client()
    fields = {}
    if body.qualification_basis:
        if body.qualification_basis not in ("PROVIDER_PAID_GP", "FINALIZED_GP"):
            raise HTTPException(status_code=422, detail="Invalid qualification_basis")
        fields["qualification_basis"] = body.qualification_basis
    if body.promotion_effective_rule:
        if body.promotion_effective_rule not in ("IMMEDIATE", "NEXT_DEAL",
                                                 "NEXT_COMMISSION_PERIOD", "NEXT_CALENDAR_MONTH"):
            raise HTTPException(status_code=422, detail="Invalid promotion_effective_rule")
        fields["promotion_effective_rule"] = body.promotion_effective_rule
    if not fields:
        raise HTTPException(status_code=422, detail="Nothing to update")
    old = get_settings(db)
    fields["updated_at"] = datetime.now(timezone.utc).isoformat()
    fields["updated_by"] = user.email or "admin"
    db.table("sgp_settings").update(fields).eq("id", 1).execute()
    audit(db, "sgp_settings", "1", "sgp_settings_updated",
          {k: old.get(k) for k in fields}, fields, actor=user.email or "admin")
    return get_settings(db)


@router.get("/agents")
def list_agents(classification: Optional[str] = Query(None),
                agreement_status: Optional[str] = Query(None),
                tier: Optional[int] = Query(None),
                q: Optional[str] = Query(None),
                user: UserContext = Depends(require_admin)):
    db = get_client()
    agents = fetch_all(db, "sales_agents", _AGENT_COLS)
    if classification:
        agents = [a for a in agents if (a.get("classification") or "") == classification]
    if agreement_status:
        agents = [a for a in agents if (a.get("agreement_status") or "NOT_SENT") == agreement_status]
    if tier is not None:
        agents = [a for a in agents if a.get("current_tier") == tier]
    if q:
        needle = q.lower()
        agents = [a for a in agents if needle in (a.get("name") or "").lower()]

    tiers = {t["tier_order"]: t for t in load_tiers(db)}
    progress = _progress_counts(db, [a["id"] for a in agents])
    out = []
    for a in agents:
        cur = a.get("current_tier")
        nxt = tiers.get((cur or 0) + 1) if cur else (tiers.get(2) if a.get("classification") == "SGP_AGENT" else None)
        rows = progress.get(a["id"], [])
        have_next = sum(1 for r in rows if nxt and r["tier_order"] == nxt["tier_order"])
        eligible, reason = is_eligible(a)
        flags = []
        if (a.get("classification") or "") == "REFERRAL_PARTNER" and cur:
            flags.append("referral partner holds an SGP tier — review classification")
        if (a.get("classification") or "") == "SGP_AGENT" and not eligible:
            flags.append(reason)
        out.append({**a,
                    "tier_name": tiers.get(cur, {}).get("name") if cur else None,
                    "agent_split": float(tiers[cur]["agent_split"]) if cur in tiers else None,
                    "company_split": float(tiers[cur]["company_split"]) if cur in tiers else None,
                    "eligible": eligible,
                    "next_tier": ({"tier": nxt["tier_order"], "name": nxt["name"],
                                   "split": float(nxt["agent_split"]),
                                   "threshold": float(nxt["monthly_gp_threshold"]),
                                   "have": have_next,
                                   "needed": int(nxt["required_qualifying_months"])} if nxt else None),
                    "qualifying_months_total": len(rows),
                    "flags": flags})
    out.sort(key=lambda a: (-(a.get("current_tier") or 0), a.get("name") or ""))
    return out


@router.get("/agents/{agent_id}")
def agent_detail(agent_id: str, user: UserContext = Depends(require_admin)):
    db = get_client()
    rows = db.table("sales_agents").select(_AGENT_COLS).eq("id", agent_id).limit(1).execute().data
    if not rows:
        raise HTTPException(status_code=404, detail="Agent not found")
    agent = rows[0]
    tiers = load_tiers(db)
    progress = fetch_all(db, "sgp_tier_progress", "*",
                         filters=[("eq", ("agent_id", agent_id))])
    history = db.table("sgp_tier_history").select("*").eq("agent_id", agent_id) \
        .order("created_at", desc=True).limit(100).execute().data or []
    by_tier = {}
    for p in progress:
        by_tier.setdefault(p["tier_order"], []).append(
            {"month": str(p["qualifying_month"])[:7], "gp": float(p["eligible_gp"]),
             "basis": p.get("basis")})
    for months in by_tier.values():
        months.sort(key=lambda m: m["month"])
    eligible, reason = is_eligible(agent)
    return {"agent": agent, "eligible": eligible, "eligibility_reason": reason,
            "tiers": tiers,
            "progress": [{"tier": t["tier_order"], "name": t["name"],
                          "split": float(t["agent_split"]),
                          "threshold": float(t["monthly_gp_threshold"]),
                          "needed": int(t["required_qualifying_months"]),
                          "months": by_tier.get(t["tier_order"], []),
                          "completed": (agent.get("current_tier") or 0) >= t["tier_order"]}
                         for t in tiers],
            "history": history}


class AgentPatch(BaseModel):
    classification: Optional[str] = None
    agreement_status: Optional[str] = None
    agreement_version: Optional[str] = None
    agreement_signed_at: Optional[str] = None
    agreement_effective_at: Optional[str] = None
    agreement_notes: Optional[str] = None
    agreement_doc_url: Optional[str] = None
    sgp_suspended: Optional[bool] = None


@router.patch("/agents/{agent_id}")
def update_agent(agent_id: str, body: AgentPatch, user: UserContext = Depends(require_admin)):
    db = get_client()
    rows = db.table("sales_agents").select(_AGENT_COLS).eq("id", agent_id).limit(1).execute().data
    if not rows:
        raise HTTPException(status_code=404, detail="Agent not found")
    agent = rows[0]
    actor = user.email or "admin"

    fields = {k: v for k, v in body.model_dump().items() if v is not None}
    if not fields:
        raise HTTPException(status_code=422, detail="Nothing to update")
    if "classification" in fields and fields["classification"] not in CLASSIFICATIONS:
        raise HTTPException(status_code=422, detail=f"classification must be one of {CLASSIFICATIONS}")
    if "agreement_status" in fields and fields["agreement_status"] not in AGREEMENT_STATUSES:
        raise HTTPException(status_code=422, detail=f"agreement_status must be one of {AGREEMENT_STATUSES}")

    new_status = fields.get("agreement_status")
    if new_status == "APPROVED":
        classification = fields.get("classification") or agent.get("classification")
        if classification != "SGP_AGENT":
            raise HTTPException(status_code=422,
                                detail="Only agents classified SGP_AGENT can have an approved SGP agreement.")
        effective = fields.get("agreement_effective_at") or agent.get("agreement_effective_at")
        if not effective:
            raise HTTPException(status_code=422,
                                detail="Set agreement_effective_at before approving — tier history starts there.")
        fields["agreement_approved_at"] = datetime.now(timezone.utc).isoformat()
        fields["agreement_terminated_at"] = None
        if not agent.get("current_tier"):
            fields["current_tier"] = 1
            fields["tier_effective_from"] = str(effective)[:10]
            db.table("sgp_tier_history").insert({
                "agent_id": agent_id, "previous_tier": None, "new_tier": 1,
                "reason": "SGP Agent Agreement approved — starting tier",
                "effective_from": str(effective)[:10],
                "promoted_by": actor, "automatic": False,
            }).execute()
        _alert(db, "sgp_agreement_approved", agent_id,
               f"{agent.get('name')} is now an approved SGP Agent — starting at the "
               f"Partner tier (50/50), effective {str(effective)[:10]}.")
    elif new_status == "TERMINATED":
        fields["agreement_terminated_at"] = datetime.now(timezone.utc).isoformat()

    db.table("sales_agents").update(fields).eq("id", agent_id).execute()
    audit(db, "sales_agents", agent_id, "sgp_agent_updated",
          {k: agent.get(k) for k in fields}, fields,
          reason=body.agreement_notes or "", actor=actor)
    return db.table("sales_agents").select(_AGENT_COLS).eq("id", agent_id).limit(1).execute().data[0]


class OverrideBody(BaseModel):
    tier: int
    reason: str
    effective_from: Optional[str] = None


@router.post("/agents/{agent_id}/override-tier")
def override_tier(agent_id: str, body: OverrideBody, user: UserContext = Depends(require_admin)):
    db = get_client()
    if not (body.reason or "").strip():
        raise HTTPException(status_code=422, detail="A reason is required for manual tier changes.")
    tiers = {t["tier_order"]: t for t in load_tiers(db)}
    if body.tier not in tiers:
        raise HTTPException(status_code=422,
                            detail=f"Tier must be 1–{max(tiers)} — the 70/30 split is the hard maximum.")
    rows = db.table("sales_agents").select(_AGENT_COLS).eq("id", agent_id).limit(1).execute().data
    if not rows:
        raise HTTPException(status_code=404, detail="Agent not found")
    agent = rows[0]
    if (agent.get("classification") or "") != "SGP_AGENT":
        raise HTTPException(status_code=422, detail="Only SGP_AGENT-classified agents can hold tiers.")
    actor = user.email or "admin"
    eff = (body.effective_from or datetime.now(timezone.utc).date().isoformat())[:10]

    db.table("sales_agents").update({"current_tier": body.tier,
                                     "tier_effective_from": eff}).eq("id", agent_id).execute()
    db.table("sgp_tier_history").insert({
        "agent_id": agent_id, "previous_tier": agent.get("current_tier"),
        "new_tier": body.tier, "reason": f"MANUAL OVERRIDE: {body.reason.strip()}",
        "effective_from": eff, "promoted_by": actor, "automatic": False,
    }).execute()
    audit(db, "sales_agents", agent_id, "sgp_tier_override",
          {"current_tier": agent.get("current_tier")},
          {"current_tier": body.tier, "effective_from": eff},
          reason=body.reason.strip(), actor=actor)
    _alert(db, "sgp_tier_unlocked", f"{agent_id}:override:{body.tier}",
           f"{agent.get('name')}'s tier was manually set to "
           f"{tiers[body.tier]['name']} ({float(tiers[body.tier]['agent_split']):g}%) by {actor}.",
           severity="warning")
    return {"ok": True, "tier": body.tier, "effective_from": eff}


class EvaluateBody(BaseModel):
    agent_id: Optional[str] = None
    backfill_from: Optional[str] = None  # "YYYY-MM" — admin-approved history migration


@router.post("/evaluate")
def evaluate(body: EvaluateBody = EvaluateBody(), user: UserContext = Depends(require_admin)):
    """Idempotent recalculation of qualifying months + promotions."""
    db = get_client()
    actor = user.email or "admin"
    if body.backfill_from:
        audit(db, "sgp_tier_progress", body.agent_id or "all", "sgp_backfill_evaluation",
              None, {"backfill_from": body.backfill_from},
              reason="Admin-approved history migration", actor=actor)
    return evaluate_all(db, agent_id=body.agent_id,
                        backfill_from=body.backfill_from, actor=actor)


@router.get("/export")
def export(user: UserContext = Depends(require_admin)):
    import pandas as pd
    db = get_client()
    agents = list_agents(user=user)  # reuse enriched rows
    tiers = load_tiers(db)
    progress = fetch_all(db, "sgp_tier_progress", "*")
    history = fetch_all(db, "sgp_tier_history", "*")
    names = {a["id"]: a.get("name") for a in agents}

    agents_df = pd.DataFrame([{
        "Agent": a.get("name"), "Classification": a.get("classification"),
        "Agreement": a.get("agreement_status"),
        "Approved": str(a.get("agreement_approved_at") or "")[:10],
        "Tier": a.get("current_tier"), "Tier Name": a.get("tier_name"),
        "Agent Split %": a.get("agent_split"), "Saigon Split %": a.get("company_split"),
        "Tier Effective": str(a.get("tier_effective_from") or "")[:10],
        "Qualifying Months": a.get("qualifying_months_total"),
        "Flags": "; ".join(a.get("flags") or []),
    } for a in agents])
    prog_df = pd.DataFrame([{
        "Agent": names.get(p["agent_id"], p["agent_id"]), "Toward Tier": p["tier_order"],
        "Month": str(p["qualifying_month"])[:7], "Eligible GP $": float(p["eligible_gp"]),
        "Basis": p.get("basis"),
    } for p in progress]) if progress else pd.DataFrame(
        columns=["Agent", "Toward Tier", "Month", "Eligible GP $", "Basis"])
    hist_df = pd.DataFrame([{
        "Agent": names.get(h["agent_id"], h["agent_id"]), "From": h.get("previous_tier"),
        "To": h["new_tier"], "Effective": str(h.get("effective_from") or "")[:10],
        "By": h.get("promoted_by"), "Automatic": h.get("automatic"), "Reason": h.get("reason"),
    } for h in history]) if history else pd.DataFrame(
        columns=["Agent", "From", "To", "Effective", "By", "Automatic", "Reason"])
    tiers_df = pd.DataFrame([{
        "Tier": t["tier_order"], "Name": t["name"], "Monthly GP Threshold $": float(t["monthly_gp_threshold"]),
        "Months Required": t["required_qualifying_months"],
        "Agent %": float(t["agent_split"]), "Saigon %": float(t["company_split"]),
    } for t in tiers])

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        agents_df.to_excel(w, sheet_name="SGP Agents", index=False)
        prog_df.to_excel(w, sheet_name="Qualifying Months", index=False)
        hist_df.to_excel(w, sheet_name="Tier History", index=False)
        tiers_df.to_excel(w, sheet_name="Tier Ladder", index=False)
    buf.seek(0)
    return StreamingResponse(
        buf, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="sgp_agent_commission.xlsx"'})
