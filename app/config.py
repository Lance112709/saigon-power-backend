"""Central configuration.

All Supabase credentials are read from the environment only — no secrets live in
this file. SUPABASE_SERVICE_KEY is the new `sb_secret_...` key (verified locally
against the DB with supabase-py 2.31.0 before deploy). The old legacy
anon/service_role JWTs were removed here; disable them in the Supabase dashboard
once this is live and healthy.

Required Railway env vars: SUPABASE_URL, SUPABASE_SERVICE_KEY, JWT_SECRET.
SUPABASE_ANON_KEY is optional (the CRM authenticates with the service key), so a
missing anon var can never crash boot. Missing required vars fail fast at boot.
"""
import os


def _require(name: str) -> str:
    val = (os.environ.get(name) or "").strip()
    if not val:
        raise RuntimeError(
            f"Required environment variable {name} is not set. "
            f"Configure it in Railway (or backend/.env for local dev)."
        )
    return val


class Settings:
    supabase_url: str         = _require("SUPABASE_URL")
    supabase_anon_key: str    = os.environ.get("SUPABASE_ANON_KEY", "").strip()
    supabase_service_key: str = _require("SUPABASE_SERVICE_KEY")
    jwt_secret: str           = _require("JWT_SECRET")
    anthropic_api_key: str    = os.environ.get("ANTHROPIC_API_KEY", "")
    frontend_url: str         = os.environ.get("FRONTEND_URL", "https://saigon-power-frontend.vercel.app")


settings = Settings()
