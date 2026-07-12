"""Central configuration.

All Supabase credentials are read from the environment only — no secrets live in
this file or in git history any more (rotation completed 2026-07-12; the old
legacy anon/service_role JWTs were replaced with a new `sb_secret_` key and the
legacy keys were disabled in the Supabase dashboard).

Required Railway env vars: SUPABASE_URL, SUPABASE_ANON_KEY, SUPABASE_SERVICE_KEY,
JWT_SECRET. SUPABASE_SERVICE_KEY should be the new `sb_secret_...` key (it grants
the same RLS-bypassing server access the old service_role JWT did). Missing vars
fail fast at boot with a clear message rather than silently falling back.

History note: an earlier env-only version failed because Railway's
SUPABASE_SERVICE_KEY held a wrong/truncated value. Before/after deploying this,
confirm the Railway value is the full, freshly-copied secret key.
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
    # The CRM authenticates to PostgREST with the service key; the anon key is
    # optional so a missing SUPABASE_ANON_KEY can never crash boot (outage-safe).
    supabase_anon_key: str    = os.environ.get("SUPABASE_ANON_KEY", "").strip()
    supabase_service_key: str = _require("SUPABASE_SERVICE_KEY")
    jwt_secret: str           = _require("JWT_SECRET")
    anthropic_api_key: str    = os.environ.get("ANTHROPIC_API_KEY", "")
    frontend_url: str         = os.environ.get("FRONTEND_URL", "https://saigon-power-frontend.vercel.app")


settings = Settings()
