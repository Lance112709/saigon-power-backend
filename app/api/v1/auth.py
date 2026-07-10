from fastapi import APIRouter, HTTPException, Body, Depends, Request
from app.db.client import get_client
from app.auth.core import hash_password, verify_password, needs_rehash, create_access_token
from app.auth.deps import get_current_user, UserContext
from app.core.security import rate_limit
from datetime import datetime, timezone

router = APIRouter()

# Password policy: enforced on every place a password is set.
MIN_PASSWORD_LEN = 10


def _validate_password_strength(pw: str):
    if len(pw) < MIN_PASSWORD_LEN:
        raise HTTPException(status_code=400,
                            detail=f"Password must be at least {MIN_PASSWORD_LEN} characters.")
    classes = sum([
        any(c.islower() for c in pw),
        any(c.isupper() for c in pw),
        any(c.isdigit() for c in pw),
        any(not c.isalnum() for c in pw),
    ])
    if classes < 3:
        raise HTTPException(
            status_code=400,
            detail="Password must include at least 3 of: lowercase, uppercase, number, symbol.")

@router.post("/setup")
def setup_admin(data: dict = Body(...)):
    """Create the first admin account — only works if no users exist."""
    db = get_client()
    existing = db.table("users").select("id", count="exact").execute()
    if existing.count and existing.count > 0:
        raise HTTPException(status_code=403, detail="Setup already complete. Use admin panel to create users.")

    email = str(data.get("email") or "").strip().lower()
    password = str(data.get("password") or "").strip()
    first_name = str(data.get("first_name") or "").strip()
    last_name = str(data.get("last_name") or "").strip()

    if not email or not password or not first_name or not last_name:
        raise HTTPException(status_code=400, detail="first_name, last_name, email, and password are required")
    _validate_password_strength(password)

    res = db.table("users").insert({
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
        "password_hash": hash_password(password),
        "role": "admin",
        "status": "active",
        "must_reset_password": False,
    }).execute()
    user = res.data[0]
    token = create_access_token(user["id"], user["email"], "admin", f"{first_name} {last_name}")
    return {"message": "Admin account created", "access_token": token, "role": "admin"}

@router.post("/login")
def login(data: dict = Body(...), request: Request = None):
    email = str(data.get("email") or "").strip().lower()
    password = str(data.get("password") or "").strip()
    if not email or not password:
        raise HTTPException(status_code=400, detail="Email and password required")

    # Brute-force protection: throttle per IP and per targeted email.
    if request is not None:
        rate_limit(request, "login", limit=10, window_seconds=300)
        rate_limit(request, f"login_email:{email}", limit=10, window_seconds=900)

    db = get_client()
    res = db.table("users").select("*").eq("email", email).eq("status", "active").execute()
    if not res.data:
        raise HTTPException(status_code=401, detail="Invalid email or password")

    user = res.data[0]
    if not verify_password(password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    # Transparently upgrade legacy bcrypt hashes to Argon2id on successful login.
    if needs_rehash(user["password_hash"]):
        try:
            db.table("users").update({"password_hash": hash_password(password)}).eq("id", user["id"]).execute()
        except Exception:
            pass

    name = f"{user['first_name']} {user['last_name']}"
    token = create_access_token(
        user["id"], user["email"], user["role"], name,
        sales_agent_name=user.get("sales_agent_name")
    )
    return {
        "access_token": token,
        "token_type": "bearer",
        "must_reset_password": user.get("must_reset_password", False),
        "user": {
            "id": user["id"],
            "name": name,
            "email": user["email"],
            "role": user["role"],
        }
    }

@router.post("/change-password")
def change_password(data: dict = Body(...), user: UserContext = Depends(get_current_user)):
    new_password = str(data.get("new_password") or "").strip()
    current_password = str(data.get("current_password") or "")
    _validate_password_strength(new_password)

    db = get_client()
    res = db.table("users").select("password_hash, must_reset_password").eq("id", user.user_id).limit(1).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="User not found")
    row = res.data[0]

    # Verify the current password — except on a forced first-login reset, where
    # the user only holds an admin-issued temp password they're replacing.
    if not row.get("must_reset_password"):
        if not current_password or not verify_password(current_password, row["password_hash"]):
            raise HTTPException(status_code=403, detail="Current password is incorrect.")
    if verify_password(new_password, row["password_hash"]):
        raise HTTPException(status_code=400, detail="New password must be different from the current one.")

    db.table("users").update({
        "password_hash": hash_password(new_password),
        "must_reset_password": False,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }).eq("id", user.user_id).execute()
    return {"message": "Password updated"}

@router.get("/me")
def get_me(user: UserContext = Depends(get_current_user)):
    db = get_client()
    res = db.table("users").select("id, first_name, last_name, email, role, status, created_at").eq("id", user.user_id).limit(1).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="User not found")
    return res.data[0]

@router.get("/permissions")
def get_my_permissions(user: UserContext = Depends(get_current_user)):
    """Returns the current user's role permissions — used by frontend on load."""
    db = get_client()
    res = db.table("role_permissions").select("permission, granted").eq("role", user.role).execute()
    return {row["permission"]: row["granted"] for row in (res.data or [])}
