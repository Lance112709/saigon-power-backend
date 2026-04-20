from fastapi import APIRouter, HTTPException, Body, Depends
from app.db.client import get_client
from app.auth.core import hash_password, verify_password, create_access_token
from app.auth.deps import get_current_user, UserContext
from datetime import datetime, timezone

router = APIRouter()

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
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")

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
def login(data: dict = Body(...)):
    email = str(data.get("email") or "").strip().lower()
    password = str(data.get("password") or "").strip()
    if not email or not password:
        raise HTTPException(status_code=400, detail="Email and password required")

    db = get_client()
    res = db.table("users").select("*").eq("email", email).eq("status", "active").execute()
    if not res.data:
        raise HTTPException(status_code=401, detail="Invalid email or password")

    user = res.data[0]
    if not verify_password(password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid email or password")

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
    if len(new_password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")

    db = get_client()
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
