from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from app.api.v1.router import router
from app.config import settings

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
            deals = db.table("lead_deals").select("id, lead_id, end_date, leads(first_name, phone)").eq("status", "Active").eq("end_date", target).execute().data or []
            for deal in deals:
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

scheduler = BackgroundScheduler(timezone="America/Chicago")
scheduler.add_job(_run_reminders, "cron", hour=8, minute=0)
scheduler.add_job(_run_ai_daily, "cron", hour=6, minute=0)
scheduler.add_job(_run_ai_monthly, "cron", day=1, hour=6, minute=30)
scheduler.add_job(_run_renewal_sms, "cron", hour=9, minute=0)

@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(
    lifespan=lifespan,
    title="Saigon Power Commission API",
    description="Commission tracking and reconciliation system for Saigon Power LLC",
    version="1.0.0"
)

_origins = [o.strip() for o in settings.frontend_url.split(",") if o.strip()]
if "http://localhost:3000" not in _origins:
    _origins.append("http://localhost:3000")

app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)

@app.get("/health")
def health():
    return {"status": "ok", "service": "Saigon Power API"}
