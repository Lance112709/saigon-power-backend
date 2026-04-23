from fastapi import APIRouter, HTTPException, Body, Depends
from app.db.client import get_client
from app.auth.core import hash_password
from app.auth.deps import require_admin, UserContext
from datetime import datetime, timezone
import secrets
import string

router = APIRouter()

def _gen_temp_password(length=12) -> str:
    chars = string.ascii_letters + string.digits + "!@#$"
    return "".join(secrets.choice(chars) for _ in range(length))

@router.get("")
def list_users(user: UserContext = Depends(require_admin)):
    db = get_client()
    res = db.table("users").select("id, first_name, last_name, email, role, status, sales_agent_name, created_at").order("created_at", desc=True).execute()
    return res.data or []

@router.post("")
def create_user(data: dict = Body(...), user: UserContext = Depends(require_admin)):
    db = get_client()
    email = str(data.get("email") or "").strip().lower()
    first_name = str(data.get("first_name") or "").strip()
    last_name = str(data.get("last_name") or "").strip()
    role = str(data.get("role") or "").strip()
    sales_agent_name = str(data.get("sales_agent_name") or "").strip() or None

    if not email or not first_name or not last_name or not role:
        raise HTTPException(status_code=400, detail="first_name, last_name, email, and role are required")
    if role not in ("admin", "manager", "csr", "sales_agent"):
        raise HTTPException(status_code=400, detail="Invalid role")

    existing = db.table("users").select("id").eq("email", email).execute()
    if existing.data:
        raise HTTPException(status_code=409, detail="Email already exists")

    temp_password = _gen_temp_password()
    res = db.table("users").insert({
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
        "password_hash": hash_password(temp_password),
        "role": role,
        "status": "active",
        "must_reset_password": True,
        "sales_agent_name": sales_agent_name,
    }).execute()
    new_user = res.data[0]
    return {
        **{k: new_user[k] for k in ("id", "first_name", "last_name", "email", "role", "status")},
        "temp_password": temp_password,
    }

# ── Role Permissions — MUST be before /{id} routes ────────────────────────────

@router.get("/permissions")
def get_all_permissions(user: UserContext = Depends(require_admin)):
    db = get_client()
    res = db.table("role_permissions").select("role, permission, granted").execute()
    matrix: dict = {}
    for row in (res.data or []):
        r = row["role"]
        if r not in matrix:
            matrix[r] = {}
        matrix[r][row["permission"]] = row["granted"]
    return matrix

@router.patch("/permissions")
def update_permission(data: dict = Body(...), user: UserContext = Depends(require_admin)):
    role = str(data.get("role") or "").strip()
    permission = str(data.get("permission") or "").strip()
    granted = bool(data.get("granted"))
    if not role or not permission:
        raise HTTPException(status_code=400, detail="role and permission required")
    if role == "admin" and permission == "manage_users" and not granted:
        raise HTTPException(status_code=400, detail="Cannot remove manage_users from admin role")
    db = get_client()
    db.table("role_permissions").upsert({"role": role, "permission": permission, "granted": granted}).execute()
    return {"ok": True, "role": role, "permission": permission, "granted": granted}

# ── User CRUD — /{id} routes after /permissions ────────────────────────────────

@router.patch("/{id}")
def update_user(id: str, data: dict = Body(...), user: UserContext = Depends(require_admin)):
    if id == user.user_id and data.get("role") and data["role"] != "admin":
        raise HTTPException(status_code=400, detail="Cannot remove admin role from yourself")
    db = get_client()
    allowed = {"first_name", "last_name", "email", "role", "status", "sales_agent_name"}
    payload = {k: v for k, v in data.items() if k in allowed}
    if not payload:
        raise HTTPException(status_code=400, detail="No valid fields")
    payload["updated_at"] = datetime.now(timezone.utc).isoformat()
    res = db.table("users").update(payload).eq("id", id).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="User not found")
    return res.data[0]

@router.post("/{id}/reset-password")
def reset_password(id: str, user: UserContext = Depends(require_admin)):
    temp = _gen_temp_password()
    db = get_client()
    db.table("users").update({
        "password_hash": hash_password(temp),
        "must_reset_password": True,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }).eq("id", id).execute()
    return {"temp_password": temp}

@router.delete("/{id}")
def delete_user(id: str, user: UserContext = Depends(require_admin)):
    if id == user.user_id:
        raise HTTPException(status_code=400, detail="Cannot delete yourself")
    db = get_client()
    db.table("users").delete().eq("id", id).execute()
    return {"ok": True}
