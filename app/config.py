"""Central configuration — secrets are read from the environment ONLY.

Nothing sensitive is hardcoded here anymore. On Railway these are set as
service variables; for local development put them in backend/.env (which is
gitignored) and load it before importing the app (the test scripts already do
`load_dotenv('.env')`).

If a required secret is missing the process fails fast at import time rather
than silently falling back to a known/committed value — a missing secret is a
deploy misconfiguration we want to surface loudly, never paper over.
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
    supabase_anon_key: str    = _require("SUPABASE_ANON_KEY")
    supabase_service_key: str = _require("SUPABASE_SERVICE_KEY")
    jwt_secret: str           = _require("JWT_SECRET")
    anthropic_api_key: str    = os.environ.get("ANTHROPIC_API_KEY", "")
    frontend_url: str         = os.environ.get("FRONTEND_URL", "https://saigon-power-frontend.vercel.app")


settings = Settings()
