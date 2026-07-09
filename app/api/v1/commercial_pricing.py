"""Commercial Pricing Engine (Phase 1: NRG, manual upload).

Versioned pricing: every upload is a new immutable version; publish archives
the provider's previous published version. Parsed rate rows live as gzipped
JSON blobs in storage and are served from an in-process cache, filtered
server-side. Agents only ever receive the customer rate (provider rate +
provider margin); provider cost and margin are admin-only, enforced here.

Phase 2 (email automation) plugs in by calling create_upload_from_bytes()
with an attachment — the rest of the pipeline is identical.
"""
import gzip
import io
import json
from datetime import date, datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile

from app.auth.deps import UserContext, get_current_user, require_admin
from app.db.client import get_client
from app.services.audit import audit
from app.services.pricing_parser import PRICING_PARSERS, parse_pricing_file

router = APIRouter()

BUCKET = "statements"          # reuse the existing private bucket
BLOB_PREFIX = "pricing"
_cache: dict = {}              # upload_id -> parsed rows (list of dicts)


def _put_blob(db, path: str, rows: list):
    blob = gzip.compress(json.dumps(rows, separators=(",", ":")).encode())
    db.storage.from_(BUCKET).upload(path, blob, {"content-type": "application/gzip", "upsert": "true"})


def _get_rows(db, upload: dict) -> list:
    uid = upload["id"]
    if uid in _cache:
        return _cache[uid]
    blob = db.storage.from_(BUCKET).download(upload["storage_path"])
    rows = json.loads(gzip.decompress(blob))
    if len(_cache) > 6:          # keep memory bounded; blobs reload on demand
        _cache.clear()
    _cache[uid] = rows
    return rows


def _provider(db, code: str) -> dict:
    r = db.table("pricing_providers").select("*").eq("code", (code or "NRG").upper()).limit(1).execute().data
    if not r:
        raise HTTPException(status_code=404, detail=f"Unknown provider '{code}'")
    return r[0]


def _published_upload(db, provider_id: str) -> Optional[dict]:
    r = db.table("pricing_uploads").select("*").eq("provider_id", provider_id) \
        .eq("status", "published").order("published_at", desc=True).limit(1).execute().data
    return r[0] if r else None


# ── Providers & settings (admin) ─────────────────────────────────────────────

@router.get("/providers")
def list_providers(user: UserContext = Depends(require_admin)):
    db = get_client()
    provs = db.table("pricing_providers").select("*").order("name").execute().data
    for p in provs:
        p["has_parser"] = p["code"].upper() in PRICING_PARSERS
    return provs


@router.post("/providers")
def add_provider(payload: dict, user: UserContext = Depends(require_admin)):
    db = get_client()
    code = (payload.get("code") or "").strip().upper().replace(" ", "_")
    name = (payload.get("name") or "").strip()
    if not code or not name:
        raise HTTPException(status_code=400, detail="code and name are required")
    row = db.table("pricing_providers").insert({
        "code": code, "name": name,
        "margin": float(payload.get("margin") or 0.003),
    }).execute().data[0]
    audit(db, "pricing_providers", row["id"], "create", None, row,
          reason="Pricing provider added", actor=user.email or "admin")
    return row


@router.patch("/providers/{provider_id}")
def update_provider(provider_id: str, payload: dict, user: UserContext = Depends(require_admin)):
    db = get_client()
    old = db.table("pricing_providers").select("*").eq("id", provider_id).limit(1).execute().data
    if not old:
        raise HTTPException(status_code=404, detail="Provider not found")
    upd = {}
    if "margin" in payload:
        m = float(payload["margin"])
        if not (0 <= m <= 0.05):
            raise HTTPException(status_code=400, detail="Margin must be between 0 and 0.05 $/kWh")
        upd["margin"] = m
    if "active" in payload:
        upd["active"] = bool(payload["active"])
    if "name" in payload and str(payload["name"]).strip():
        upd["name"] = str(payload["name"]).strip()
    if "auto_publish" in payload:
        upd["auto_publish"] = bool(payload["auto_publish"])
    row = db.table("pricing_providers").update(upd).eq("id", provider_id).execute().data[0]
    audit(db, "pricing_providers", provider_id, "update", old[0], upd,
          reason="Pricing provider settings changed", actor=user.email or "admin")
    return row


# ── Upload / preview / publish (admin) ───────────────────────────────────────

def create_upload_from_bytes(db, provider: dict, file_bytes: bytes, filename: str,
                             actor: str) -> dict:
    """Shared by manual upload today and email automation in Phase 2."""
    parsed = parse_pricing_file(provider["code"], file_bytes, filename)
    dup = db.table("pricing_uploads").select("id,version,status").eq("file_hash", parsed["file_hash"]) \
        .eq("provider_id", provider["id"]).limit(1).execute().data
    if dup:
        raise HTTPException(status_code=409,
                            detail=f"This exact file was already imported as version {dup[0]['version']}.")

    last = db.table("pricing_uploads").select("version").eq("provider_id", provider["id"]) \
        .order("version", desc=True).limit(1).execute().data
    version = (last[0]["version"] + 1) if last else 1
    margin = float(provider["margin"])

    upload = db.table("pricing_uploads").insert({
        "provider_id": provider["id"], "version": version, "status": "draft",
        "original_filename": filename, "file_hash": parsed["file_hash"],
        "storage_path": f"{BLOB_PREFIX}/{provider['code']}_v{version}.json.gz",
        "effective_date": parsed["effective_date"],
        "expiration_at": parsed["expiration_at"],
        "rows_imported": parsed["row_count"], "margin_used": margin,
        "uploaded_by": actor,
        "import_log": {"warnings": parsed["warnings"], "filename": filename},
        "summary": parsed["dims"],
    }).execute().data[0]
    _put_blob(db, upload["storage_path"], parsed["rows"])
    _cache[upload["id"]] = parsed["rows"]
    audit(db, "pricing_uploads", upload["id"], "import", None,
          {"provider": provider["code"], "version": version, "rows": parsed["row_count"],
           "file": filename, "margin": margin},
          reason="Pricing matrix imported", actor=actor)
    return upload


@router.post("/upload")
async def upload_matrix(
    provider_code: str = Form("NRG"),
    file: UploadFile = File(...),
    user: UserContext = Depends(require_admin),
):
    ext = (file.filename or "").rsplit(".", 1)[-1].lower()
    if ext not in ("xlsx", "xls", "xlsm", "csv"):
        raise HTTPException(status_code=400, detail="Upload an Excel (.xlsx/.xls/.xlsm) or CSV file.")
    db = get_client()
    provider = _provider(db, provider_code)
    file_bytes = await file.read()
    try:
        upload = create_upload_from_bytes(db, provider, file_bytes, file.filename or "pricing", user.email or "admin")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"upload": upload, "preview": _preview_payload(db, upload, provider)}


def _preview_payload(db, upload: dict, provider: dict, limit: int = 40) -> dict:
    rows = _get_rows(db, upload)
    margin = float(upload["margin_used"])
    sample = [{**r, "customer_rate": round(r["rate"] + margin, 6)} for r in rows[:limit]]
    return {"sample": sample, "margin": margin, "rows_imported": upload["rows_imported"],
            "dims": upload.get("summary"), "warnings": (upload.get("import_log") or {}).get("warnings", [])}


@router.get("/uploads/{upload_id}/preview")
def preview_upload(upload_id: str, user: UserContext = Depends(require_admin)):
    db = get_client()
    u = db.table("pricing_uploads").select("*").eq("id", upload_id).limit(1).execute().data
    if not u:
        raise HTTPException(status_code=404, detail="Upload not found")
    prov = db.table("pricing_providers").select("*").eq("id", u[0]["provider_id"]).limit(1).execute().data[0]
    return {"upload": u[0], "preview": _preview_payload(db, u[0], prov)}


def publish_upload_internal(db, upload_id: str, actor: str) -> dict:
    """Publish a version and archive the provider's previous published one.
    Shared by the admin endpoint and the Phase 2 email ingest."""
    u = db.table("pricing_uploads").select("*").eq("id", upload_id).limit(1).execute().data
    if not u:
        raise HTTPException(status_code=404, detail="Upload not found")
    u = u[0]
    if u["status"] == "published":
        return u
    db.table("pricing_uploads").update({"status": "archived"}) \
        .eq("provider_id", u["provider_id"]).eq("status", "published").execute()
    row = db.table("pricing_uploads").update({
        "status": "published", "published_at": datetime.now(timezone.utc).isoformat(),
    }).eq("id", upload_id).execute().data[0]
    audit(db, "pricing_uploads", upload_id, "publish", {"status": u["status"]},
          {"status": "published", "version": u["version"]},
          reason="Pricing published to sales agents", actor=actor)
    return row


@router.post("/uploads/{upload_id}/publish")
def publish_upload(upload_id: str, user: UserContext = Depends(require_admin)):
    return publish_upload_internal(get_client(), upload_id, user.email or "admin")


@router.post("/poll-email")
def poll_email(user: UserContext = Depends(require_admin)):
    """Manual trigger for the Phase 2 inbox check (also runs on a schedule)."""
    from app.services.pricing_email_ingest import poll_pricing_inbox
    return poll_pricing_inbox(actor=user.email or "admin")


@router.post("/uploads/{upload_id}/archive")
def archive_upload(upload_id: str, user: UserContext = Depends(require_admin)):
    db = get_client()
    row = db.table("pricing_uploads").update({"status": "archived"}).eq("id", upload_id).execute().data
    if not row:
        raise HTTPException(status_code=404, detail="Upload not found")
    audit(db, "pricing_uploads", upload_id, "archive", None, {"status": "archived"},
          reason="Pricing version archived", actor=user.email or "admin")
    return row[0]


@router.get("/dashboard")
def pricing_dashboard(user: UserContext = Depends(require_admin)):
    db = get_client()
    provs = db.table("pricing_providers").select("*").order("name").execute().data
    out = []
    for p in provs:
        cur = _published_upload(db, p["id"])
        drafts = db.table("pricing_uploads").select("id,version,created_at,rows_imported,uploaded_by") \
            .eq("provider_id", p["id"]).eq("status", "draft").order("created_at", desc=True).limit(3).execute().data
        out.append({"provider": {**p, "has_parser": p["code"].upper() in PRICING_PARSERS},
                    "current": cur, "drafts": drafts})
    return out


@router.get("/history")
def pricing_history(provider_code: Optional[str] = Query(None), limit: int = Query(50),
                    user: UserContext = Depends(require_admin)):
    db = get_client()
    q = db.table("pricing_uploads").select("*, pricing_providers(code,name)")
    if provider_code:
        q = q.eq("provider_id", _provider(db, provider_code)["id"])
    return q.order("created_at", desc=True).limit(limit).execute().data


# ── Agent-facing pricing (any authenticated user) ────────────────────────────

@router.get("/current/version")
def current_version(provider_code: str = Query("NRG"), user: UserContext = Depends(get_current_user)):
    """Cheap poll target: agents refetch when the id changes."""
    db = get_client()
    provider = _provider(db, provider_code)
    cur = _published_upload(db, provider["id"])
    if not cur:
        return {"upload_id": None}
    return {"upload_id": cur["id"], "version": cur["version"], "published_at": cur["published_at"]}


@router.get("/current")
def current_pricing(
    provider_code: str = Query("NRG"),
    utility: Optional[str] = Query(None),
    zone: Optional[str] = Query(None),
    term: Optional[int] = Query(None),
    product: Optional[str] = Query(None),
    usage_tier: Optional[str] = Query(None),
    start_month: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    limit: int = Query(200),
    offset: int = Query(0),
    user: UserContext = Depends(get_current_user),
):
    db = get_client()
    provider = _provider(db, provider_code)
    cur = _published_upload(db, provider["id"])
    if not cur:
        return {"meta": None, "rows": [], "total": 0, "dims": {}}
    rows = _get_rows(db, cur)
    margin = float(cur["margin_used"])

    def keep(r):
        if utility and (r.get("utility") or "").upper() != utility.upper():
            return False
        if zone and (r.get("zone") or "").upper() != zone.upper():
            return False
        if term and r.get("term") != term:
            return False
        if product and (r.get("product") or "").upper() != product.upper():
            return False
        if usage_tier and (r.get("usage_tier") or "") != usage_tier:
            return False
        if start_month and (r.get("start_month") or "") != start_month:
            return False
        if search:
            s = search.lower()
            hay = " ".join(str(r.get(k) or "") for k in ("product", "utility", "zone", "load_profile", "usage_tier")).lower()
            if s not in hay:
                return False
        return True

    filtered = [r for r in rows if keep(r)]
    page = filtered[offset:offset + limit]
    is_admin = user.is_admin
    out_rows = []
    for r in page:
        row = {
            "utility": r.get("utility"), "zone": r.get("zone"),
            "load_profile": r.get("load_profile"), "product": r.get("product"),
            "usage_tier": r.get("usage_tier"), "start_month": r.get("start_month"),
            "term": r.get("term"),
            "customer_rate": round(r["rate"] + margin, 6),
        }
        if is_admin:                       # provider cost & margin are admin-only
            row["provider_rate"] = r["rate"]
        out_rows.append(row)

    meta = {"provider": provider["name"], "provider_code": provider["code"],
            "version": cur["version"], "published_at": cur["published_at"],
            "effective_date": cur["effective_date"], "expiration_at": cur["expiration_at"],
            "upload_id": cur["id"]}
    if is_admin:
        meta["margin"] = margin
    return {"meta": meta, "rows": out_rows, "total": len(filtered), "dims": cur.get("summary") or {}}
