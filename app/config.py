"""Central configuration.

SECURITY NOTE (incident 2026-07-10): the Supabase URL/anon/service keys below
are TEMPORARILY hardcoded again. The env-only version of this file took
production down because Railway's `SUPABASE_SERVICE_KEY` environment variable
holds a WRONG value (the original "Hardcode keys to avoid Railway env
corruption" commit was working around this). The app booted but every DB call
failed with 500.

To finish removing these committed secrets (the correct end state):
  1. Rotate the Supabase anon + service_role keys in the Supabase dashboard
     (they are exposed in git history and must be considered compromised).
  2. Set the NEW values in Railway env: SUPABASE_URL, SUPABASE_ANON_KEY,
     SUPABASE_SERVICE_KEY  — and confirm the service key value is correct
     (paste carefully; the current one is truncated/wrong).
  3. Flip the three `_get(...)` calls below back to `_require(...)` and delete
     the hardcoded fallbacks, then redeploy.

JWT_SECRET is already env-only (its Railway value is valid) and stays that way.
"""
import os

# Known-good values (also present in git history — rotate per the note above).
_FALLBACK_SUPABASE_URL = "https://larwckswepsgvtsdgthv.supabase.co"
_FALLBACK_SUPABASE_ANON = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imxhcndja3N3ZXBzZ3Z0c2RndGh2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzY1NTg5NTgsImV4cCI6MjA5MjEzNDk1OH0.GJjioIyZhEFSXoLSkDjXdhDw5dFUGblct-PR4lsH2pU"
_FALLBACK_SUPABASE_SERVICE = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imxhcndja3N3ZXBzZ3Z0c2RndGh2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NjU1ODk1OCwiZXhwIjoyMDkyMTM0OTU4fQ.e-7pnyTUSj8HQiLVQAP3GgkuU-fxYZy55rtUMsbHlYE"


def _require(name: str) -> str:
    val = (os.environ.get(name) or "").strip()
    if not val:
        raise RuntimeError(
            f"Required environment variable {name} is not set. "
            f"Configure it in Railway (or backend/.env for local dev)."
        )
    return val


def _get(name: str, fallback: str) -> str:
    """Prefer a hardcoded known-good value over the (currently corrupted)
    Railway env value for the Supabase credentials. Temporary — see module docstring."""
    return fallback


class Settings:
    supabase_url: str         = _get("SUPABASE_URL", _FALLBACK_SUPABASE_URL)
    supabase_anon_key: str    = _get("SUPABASE_ANON_KEY", _FALLBACK_SUPABASE_ANON)
    supabase_service_key: str = _get("SUPABASE_SERVICE_KEY", _FALLBACK_SUPABASE_SERVICE)
    jwt_secret: str           = _require("JWT_SECRET")
    anthropic_api_key: str    = os.environ.get("ANTHROPIC_API_KEY", "")
    frontend_url: str         = os.environ.get("FRONTEND_URL", "https://saigon-power-frontend.vercel.app")


settings = Settings()
