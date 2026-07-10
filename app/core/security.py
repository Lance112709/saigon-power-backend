"""Shared security primitives: rate limiting, security headers, and PostgREST
filter-string sanitization.

Deliberately dependency-free and in-process. Railway runs a single replica, so
an in-memory sliding-window limiter is effective; it resets on redeploy, which
is acceptable for brute-force mitigation (an attacker can't force redeploys).
If the service is ever scaled horizontally, move these counters to Redis.
"""
import time
import threading
from collections import defaultdict, deque

from fastapi import HTTPException, Request
from starlette.middleware.base import BaseHTTPMiddleware

# ── PostgREST filter sanitization ─────────────────────────────────────────────
# Values interpolated into `.or_("col.ilike.%{v}%")` strings must not contain
# PostgREST metacharacters, or a caller can break out of the intended term and
# inject arbitrary boolean predicates. Bound-argument calls like `.ilike(col, v)`
# are already safe; this is only for the `.or_()` string-building call sites.
_PGREST_META = str.maketrans({c: " " for c in ",.()*:\"'\\%"})


def sanitize_search(value: str, max_len: int = 100) -> str:
    """Strip PostgREST filter metacharacters from free-text search input.
    Returns a plain token safe to interpolate into an ilike pattern."""
    if not value:
        return ""
    cleaned = str(value).translate(_PGREST_META).strip()
    return cleaned[:max_len]


# ── In-memory sliding-window rate limiter ─────────────────────────────────────

class RateLimiter:
    def __init__(self):
        self._hits: dict[str, deque] = defaultdict(deque)
        self._lock = threading.Lock()

    def check(self, key: str, limit: int, window_seconds: int) -> bool:
        """Record a hit for `key`; return True if within limit, False if over."""
        now = time.time()
        cutoff = now - window_seconds
        with self._lock:
            dq = self._hits[key]
            while dq and dq[0] < cutoff:
                dq.popleft()
            if len(dq) >= limit:
                return False
            dq.append(now)
            return True

    def retry_after(self, key: str, window_seconds: int) -> int:
        with self._lock:
            dq = self._hits.get(key)
            if not dq:
                return 0
            return max(1, int(window_seconds - (time.time() - dq[0])))


limiter = RateLimiter()


def client_ip(request: Request) -> str:
    """Best-effort client IP, honoring the proxy chain Railway/Vercel set."""
    fwd = request.headers.get("x-forwarded-for", "")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def rate_limit(request: Request, bucket: str, limit: int, window_seconds: int):
    """Raise 429 if the caller's IP exceeds `limit` hits in the window."""
    key = f"{bucket}:{client_ip(request)}"
    if not limiter.check(key, limit, window_seconds):
        raise HTTPException(
            status_code=429,
            detail="Too many attempts. Please wait a moment and try again.",
            headers={"Retry-After": str(limiter.retry_after(key, window_seconds))},
        )


# ── Security headers ──────────────────────────────────────────────────────────

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Attach standard hardening headers to every response. The API serves
    JSON only, so a restrictive CSP that forbids scripts/embedding is safe."""

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers["Strict-Transport-Security"] = "max-age=63072000; includeSubDomains; preload"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
        response.headers["Content-Security-Policy"] = (
            "default-src 'none'; frame-ancestors 'none'; base-uri 'none'"
        )
        # Starlette's MutableHeaders has no .pop(); delete via guarded __delitem__.
        for _h in ("server", "x-powered-by"):
            if _h in response.headers:
                del response.headers[_h]
        return response
