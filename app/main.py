from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from app.api.v1.router import router
from app.config import settings
from app.core.security import SecurityHeadersMiddleware

def _run_reminders():
    try:
        from app.services.email_reminders import send_task_reminders
        send_task_reminders()
    except Exception:
        pass

def _run_ai_daily():
    try:
        from app.services.ai_agent import generate_daily_report
        generate_daily_report()
    except Exception:
        pass

def _run_ai_monthly():
    try:
        from app.services.ai_agent import generate_monthly_report
        generate_monthly_report()
    except Exception:
        pass

def _run_renewal_sms():
    try:
        from app.services.sms import send_automated
        from app.db.client import get_client
        from datetime import datetime, timedelta, timezone
        db = get_client()
        today = datetime.now(timezone.utc).date()
        for days_out in (60, 30):
            target = (today + timedelta(days=days_out)).isoformat()
            deals = db.table("lead_deals").select("id, lead_id, end_date, rate_type, plan_name, contract_term, leads(first_name, phone)").eq("status", "Active").eq("end_date", target).execute().data or []
            from app.utils.deals import is_month_to_month
            for deal in deals:
                if is_month_to_month(deal.get("rate_type"), deal.get("plan_name"), deal.get("contract_term")):
                    continue  # nothing to renew on month-to-month
                lead = deal.get("leads") or {}
                phone = lead.get("phone")
                if not phone:
                    continue
                send_automated(
                    f"renewal_{days_out}d",
                    phone,
                    {
                        "first_name": lead.get("first_name") or "Valued Customer",
                        "days":       str(days_out),
                        "end_date":   target,
                    },
                    lead_id=deal.get("lead_id"),
                    deal_id=deal.get("id"),
                )
    except Exception:
        pass

def _run_email_ingest():
    """Monthly: pull new commission statements straight from the inbox."""
    try:
        from app.services.email_ingest import poll_inbox
        poll_inbox()
    except Exception:
        pass

def _run_email_ingest_daily():
    """Daily: the commission@ inbox too (NRG Commercial lands ~21st, Tara/Reliant/
    APG&E mid-month) — 10-day lookback, hash-idempotent, so nothing waits for the
    monthly run on the 8th any more."""
    try:
        from app.services.email_ingest import poll_inbox
        poll_inbox(actor="email-ingest-daily", lookback_days=10)
    except Exception:
        pass

def _run_lance_statements():
    """Daily: statements that providers mail to lance@ (Heritage, NRG residual,
    Hudson, Iron Horse, Chariot, Budget, NRG backpay)."""
    try:
        from app.services.email_ingest import poll_lance_statements
        poll_lance_statements()
    except Exception:
        pass

def _run_bank_alerts():
    """Daily: Chase deposit alerts → bank_deposits → matched onto statements."""
    try:
        from app.services.bank_deposits import poll_chase_alerts
        poll_chase_alerts()
    except Exception:
        pass

def _run_email_campaigns():
    """Auto-drip: send the next batch of any active bulk-email campaign,
    bounded by the Resend plan's daily cap (EMAIL_DAILY_CAP)."""
    try:
        from app.services.email_campaigns import process_campaigns
        process_campaigns()
    except Exception:
        pass

def _run_pricing_ingest():
    try:
        from app.services.pricing_email_ingest import poll_pricing_inbox
        poll_pricing_inbox()
    except Exception:
        pass


def _run_statement_watchdog():
    """Providers pay by the 7th. On the 10th, alert if any provider's
    statement for last month has not been uploaded and reconciled."""
    try:
        from app.services.statement_watchdog import check_missing_statements
        check_missing_statements()
    except Exception:
        pass


def _run_lead_conversion_heal():
    """Nightly: any lead with an Active deal that is still marked 'lead'
    (conversion step failed silently) gets converted + an SGP ID."""
    try:
        from app.services.lead_conversion import heal_stuck_leads
        heal_stuck_leads()
    except Exception:
        import logging
        logging.getLogger("saigon.lead_conversion").exception("nightly lead self-heal crashed")


def _run_commission_autocalc():
    """Daily: refresh 'calculated' agent-commission rows for this month and the
    two before it (approved/paid rows are locked)."""
    try:
        from app.services.commission_autorun import auto_calculate
        auto_calculate()
    except Exception:
        import logging
        logging.getLogger("saigon.commission_autorun").exception("auto-calculate crashed")


def _run_commission_digest():
    """Monthly (day 8, after the statement ingest): email Lance the pre-payout
    checklist for the previous month."""
    try:
        from app.services.commission_digest import send_digest
        send_digest()
    except Exception:
        import logging
        logging.getLogger("saigon.commission_digest").exception("digest crashed")


def _run_sgp_evaluation():
    """Fold last month's provider-paid GP into SGP tier progress and apply
    any permanently earned promotions (idempotent)."""
    try:
        from app.services.sgp_tiers import run_monthly_evaluation
        run_monthly_evaluation()
    except Exception:
        pass

try:
    scheduler = BackgroundScheduler(timezone="America/Chicago")
    scheduler.add_job(_run_reminders, "cron", hour=8, minute=0)
    scheduler.add_job(_run_ai_daily, "cron", hour=6, minute=0)
    scheduler.add_job(_run_ai_monthly, "cron", day=1, hour=6, minute=30)
    scheduler.add_job(_run_renewal_sms, "cron", hour=9, minute=0)
    scheduler.add_job(_run_statement_watchdog, "cron", day=10, hour=9, minute=30)
    # Day 10, after the watchdog: fold last month's provider-paid GP into SGP
    # tier progress and apply any permanently earned promotions.
    scheduler.add_job(_run_sgp_evaluation, "cron", day=10, hour=10, minute=0)
    scheduler.add_job(_run_commission_autocalc, "cron", hour=5, minute=45)
    scheduler.add_job(_run_commission_digest, "cron", day=8, hour=11, minute=0)
    scheduler.add_job(_run_lead_conversion_heal, "cron", hour=5, minute=30)
    # Monthly: commission statements arrive once a month (providers pay by the
    # 7th) — pull the commission@ inbox on the 8th. The 40-day lookback plus
    # hash-idempotent poll_inbox means late statements are caught by the
    # watchdog's own precheck poll on the 10th or by next month's run, and
    # nothing is ever imported twice. "Check Email Now" still pulls on demand.
    scheduler.add_job(_run_email_ingest, "cron", day=8, hour=9, minute=15)
    # Daily: providers that mail lance@ pay on their own days through the month
    # (Heritage ~5th, Chariot ~16th, Budget ~18th, Iron Horse ~19th, NRG ~21st,
    # Hudson ~25th) — one sender-filtered pass each morning catches every one
    # the day it lands; then the Chase deposit alerts are matched to statements.
    scheduler.add_job(_run_email_ingest_daily, "cron", hour=10, minute=15)
    scheduler.add_job(_run_lance_statements, "cron", hour=10, minute=30)
    scheduler.add_job(_run_bank_alerts, "cron", hour=10, minute=45)
    # Phase 2 pricing automation: NRG emails the matrix each business morning;
    # poll weekday mornings so agents have fresh rates by the time they log in.
    scheduler.add_job(_run_pricing_ingest, "cron", day_of_week="mon-fri", hour="6-12", minute="*/20")
    # Bulk email auto-drip: every 30 min through the day, send the next batch of
    # any active campaign up to the plan's daily cap. Spreads a large blast over
    # days instead of hitting the Resend limit all at once.
    scheduler.add_job(_run_email_campaigns, "cron", hour="8-20", minute="*/30")
    _scheduler_ok = True
except Exception:
    _scheduler_ok = False

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Finish any statement import a previous container was killed in the middle
    # of (deploys land while imports run; the batch row + stored file survive).
    def _resume_imports_later():
        import time
        time.sleep(25)  # let the app settle first
        try:
            from app.api.v1.uploads import resume_stuck_imports
            resume_stuck_imports()
        except Exception:
            pass
    try:
        import threading
        threading.Thread(target=_resume_imports_later, daemon=True).start()
    except Exception:
        pass
    if _scheduler_ok:
        try:
            scheduler.start()
        except Exception:
            pass
    try:  # pre-build the revenue forecast so the first /forecast visit is instant
        from app.api.v1.dashboard import warm_revenue_forecast
        warm_revenue_forecast()
    except Exception:
        pass
    yield
    if _scheduler_ok:
        try:
            scheduler.shutdown()
        except Exception:
            pass

app = FastAPI(
    lifespan=lifespan,
    title="Saigon Power Commission API",
    description="Commission tracking and reconciliation system for Saigon Power LLC",
    version="1.0.0"
)

_origins = [o.strip() for o in settings.frontend_url.split(",") if o.strip()]
for _always in ["http://localhost:3000", "https://saigon-power-frontend.vercel.app",
                "https://saigonpowertx.com", "https://www.saigonpowertx.com"]:
    if _always not in _origins:
        _origins.append(_always)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PATCH", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
    max_age=600,
)
app.add_middleware(SecurityHeadersMiddleware)

app.include_router(router)

@app.get("/health")
def health():
    return {"status": "ok", "service": "Saigon Power API", "version": "giadienre-v19"}
