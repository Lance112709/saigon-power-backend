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
    # Daily: providers pay through the month — pull statement emails, import,
    # audit, and alert every morning (poll_inbox is hash-idempotent, so a
    # statement is never processed twice).
    scheduler.add_job(_run_email_ingest, "cron", hour=9, minute=15)
    # Phase 2 pricing automation: NRG emails the matrix each business morning;
    # poll weekday mornings so agents have fresh rates by the time they log in.
    scheduler.add_job(_run_pricing_ingest, "cron", day_of_week="mon-fri", hour="6-12", minute="*/20")
    _scheduler_ok = True
except Exception:
    _scheduler_ok = False

@asynccontextmanager
async def lifespan(app: FastAPI):
    if _scheduler_ok:
        try:
            scheduler.start()
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
for _always in ["http://localhost:3000", "https://saigon-power-frontend.vercel.app"]:
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
    return {"status": "ok", "service": "Saigon Power API", "version": "giadienre-v9"}
