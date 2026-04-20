from fastapi import HTTPException, Security, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from app.auth.core import decode_token
from jose import JWTError

security = HTTPBearer()

class UserContext:
    def __init__(self, data: dict):
        self.user_id: str = data["sub"]
        self.email: str = data["email"]
        self.role: str = data["role"]
        self.name: str = data["name"]
        self.sales_agent_name: str | None = data.get("sales_agent_name")

    @property
    def is_admin(self): return self.role == "admin"
    @property
    def is_manager(self): return self.role in ("admin", "manager")
    @property
    def is_csr(self): return self.role in ("admin", "manager", "csr")
    @property
    def is_sales_agent(self): return self.role == "sales_agent"

def get_current_user(credentials: HTTPAuthorizationCredentials = Security(security)) -> UserContext:
    try:
        payload = decode_token(credentials.credentials)
        return UserContext(payload)
    except (JWTError, KeyError):
        raise HTTPException(status_code=401, detail="Invalid or expired token")

def require_admin(user: UserContext = Depends(get_current_user)) -> UserContext:
    if not user.is_admin:
        raise HTTPException(status_code=403, detail="Admin access required")
    return user

def require_manager(user: UserContext = Depends(get_current_user)) -> UserContext:
    if user.role not in ("admin", "manager"):
        raise HTTPException(status_code=403, detail="Manager or Admin access required")
    return user

def require_no_export(user: UserContext = Depends(get_current_user)) -> UserContext:
    if not user.is_admin:
        raise HTTPException(status_code=403, detail="Only admins can export data")
    return user
