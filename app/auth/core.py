"""Authentication primitives: password hashing and JWT issue/verify.

Passwords are hashed with Argon2id (the current OWASP-recommended algorithm).
Legacy bcrypt hashes are still verified transparently so existing accounts keep
working; they are transparently upgraded to Argon2id on next successful login.

The JWT signing secret comes from config.settings (environment-only, no
committed fallback).
"""
import bcrypt
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError, InvalidHashError
from jose import jwt
from datetime import datetime, timedelta, timezone

from app.config import settings

SECRET_KEY = settings.jwt_secret
ALGORITHM = "HS256"
TOKEN_EXPIRE_HOURS = 12

# Argon2id with sensible interactive parameters.
_ph = PasswordHasher(time_cost=3, memory_cost=64 * 1024, parallelism=2)


def hash_password(password: str) -> str:
    return _ph.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    """Verify against Argon2id or a legacy bcrypt hash."""
    if not hashed:
        return False
    if hashed.startswith("$argon2"):
        try:
            _ph.verify(hashed, plain)
            return True
        except (VerifyMismatchError, InvalidHashError):
            return False
    # legacy bcrypt
    try:
        return bcrypt.checkpw(plain.encode(), hashed.encode())
    except (ValueError, TypeError):
        return False


def needs_rehash(hashed: str) -> bool:
    """True if the stored hash is legacy bcrypt or uses outdated params, so the
    caller can transparently upgrade it to current Argon2id on next login."""
    if not hashed or not hashed.startswith("$argon2"):
        return True
    try:
        return _ph.check_needs_rehash(hashed)
    except InvalidHashError:
        return True


def create_access_token(user_id: str, email: str, role: str, name: str, sales_agent_name: str = None) -> str:
    expire = datetime.now(timezone.utc) + timedelta(hours=TOKEN_EXPIRE_HOURS)
    payload = {"sub": user_id, "email": email, "role": role, "name": name, "exp": expire}
    if sales_agent_name:
        payload["sales_agent_name"] = sales_agent_name
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def decode_token(token: str) -> dict:
    return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
