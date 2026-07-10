"""Test bootstrap: load backend/.env so config's required env vars are present.

Secrets are env-only (never committed); the local .env holds the dev values.
If .env lacks a var, fall back to a harmless dummy so pure-logic tests that
never touch the network can still import app.config.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

for _k, _dummy in (
    ("SUPABASE_URL", "https://test.supabase.co"),
    ("SUPABASE_ANON_KEY", "test-anon"),
    ("SUPABASE_SERVICE_KEY", "test-service"),
    ("JWT_SECRET", "test-jwt-secret-not-for-production"),
):
    os.environ.setdefault(_k, _dummy)
