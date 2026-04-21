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

scheduler = BackgroundScheduler(timezone="America/Chicago")
scheduler.add_job(_run_reminders, "cron", hour=8, minute=0)

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
