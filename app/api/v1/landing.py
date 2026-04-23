from fastapi import APIRouter, Depends, HTTPException
from app.db.client import get_client
from app.auth.deps import get_current_user, UserContext

router = APIRouter()


@router.get("")
def get_landing_plans():
    db = get_client()
    plans = db.table("landing_plans").select("*").order("sort_order").execute().data or []
    return plans


@router.patch("/{plan_id}")
def update_landing_plan(
    plan_id: int,
    data: dict,
    user: UserContext = Depends(get_current_user),
):
    if user.role not in ("admin", "manager"):
        raise HTTPException(status_code=403, detail="Admin or Manager only")

    allowed = {"plan_name", "provider", "rate", "badge"}
    payload = {k: v for k, v in data.items() if k in allowed}
    if not payload:
        raise HTTPException(status_code=400, detail="No valid fields")

    db = get_client()
    result = db.table("landing_plans").update(payload).eq("id", plan_id).execute()
    return result.data[0] if result.data else {}
