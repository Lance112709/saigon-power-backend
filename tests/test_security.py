"""Security regression tests for the hardening pass.

Pure-logic only — no network. Covers password hashing/upgrade, PostgREST
search sanitization, the rate limiter, and record ownership checks.
"""
import sys
from pathlib import Path

import bcrypt
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi import FastAPI
from starlette.testclient import TestClient

from app.auth.core import hash_password, verify_password, needs_rehash
from app.core.security import sanitize_search, RateLimiter, SecurityHeadersMiddleware
from app.auth.ownership import _agent_name


# ── security headers middleware (exercises the real ASGI path) ────────────────

def _client_with_headers():
    app = FastAPI()
    app.add_middleware(SecurityHeadersMiddleware)

    @app.get("/ping")
    def ping():
        return {"ok": True}

    return TestClient(app)


def test_security_headers_middleware_does_not_500():
    # Regression: MutableHeaders has no .pop(); the middleware must not crash.
    r = _client_with_headers().get("/ping")
    assert r.status_code == 200
    assert r.headers["content-security-policy"]
    assert r.headers["strict-transport-security"]
    assert r.headers["x-frame-options"] == "DENY"
    assert r.headers["x-content-type-options"] == "nosniff"


# ── password hashing ──────────────────────────────────────────────────────────

def test_argon2_hash_roundtrip():
    h = hash_password("Sup3r$ecret!")
    assert h.startswith("$argon2")
    assert verify_password("Sup3r$ecret!", h)
    assert not verify_password("wrong", h)
    assert not needs_rehash(h)


def test_legacy_bcrypt_still_verifies_and_flags_rehash():
    legacy = bcrypt.hashpw(b"OldPass123", bcrypt.gensalt()).decode()
    assert verify_password("OldPass123", legacy)
    assert not verify_password("nope", legacy)
    assert needs_rehash(legacy)  # legacy hash should be upgraded on next login


def test_verify_rejects_empty_or_garbage_hash():
    assert not verify_password("x", "")
    assert not verify_password("x", "not-a-hash")


# ── PostgREST search sanitization ─────────────────────────────────────────────

@pytest.mark.parametrize("evil", [
    "zzz,phone.not.is.null",          # inject an extra OR predicate
    "a,dob.eq.1990-01-01",            # blind-infer a hidden column
    "x.ilike.*",
    "'; drop--",
    "a(b)c:d",
])
def test_sanitize_search_strips_postgrest_metacharacters(evil):
    cleaned = sanitize_search(evil)
    for ch in ",.()*:\"'\\%":
        assert ch not in cleaned


def test_sanitize_search_keeps_plain_terms():
    assert sanitize_search("  Nguyen  ") == "Nguyen"
    assert sanitize_search("HOUSTON 77002").replace(" ", "") == "HOUSTON77002"


def test_sanitize_search_truncates():
    assert len(sanitize_search("a" * 500)) == 100


# ── rate limiter ──────────────────────────────────────────────────────────────

def test_rate_limiter_allows_then_blocks():
    rl = RateLimiter()
    for _ in range(5):
        assert rl.check("k", limit=5, window_seconds=60) is True
    assert rl.check("k", limit=5, window_seconds=60) is False  # 6th over limit


def test_rate_limiter_is_per_key():
    rl = RateLimiter()
    assert rl.check("a", 1, 60) is True
    assert rl.check("a", 1, 60) is False
    assert rl.check("b", 1, 60) is True  # different key unaffected


# ── ownership resolution ──────────────────────────────────────────────────────

class _U:
    def __init__(self, is_sales_agent, name):
        self.is_sales_agent = is_sales_agent
        self.sales_agent_name = name


def test_agent_name_none_for_staff():
    assert _agent_name(_U(False, None)) is None  # admin/manager/csr → unrestricted


def test_agent_name_lowercased_for_agent():
    assert _agent_name(_U(True, "Jane Doe")) == "jane doe"


def test_agent_with_no_name_owns_nothing():
    # sentinel that can never equal a real (lowercased) agent name
    assert _agent_name(_U(True, "")) == "\x00"
