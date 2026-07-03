"""Append-only audit trail for every data change the system makes.

Writes to the audit_log table when it exists (see migrations/001_audit_log.sql).
If the table has not been created yet, falls back to writing JSON objects into
the 'audit' storage bucket so no change ever goes unrecorded.
"""
import json
import uuid
from datetime import datetime, timezone

_table_ok = None  # tri-state cache: None unknown, True usable, False missing


def audit(db, table: str, record_id: str, action: str, old_value=None,
          new_value=None, reason: str = "", actor: str = "system"):
    global _table_ok
    entry = {
        "table_name": table,
        "record_id": str(record_id),
        "action": action,
        "old_value": old_value if old_value is None or isinstance(old_value, (dict, list)) else {"value": old_value},
        "new_value": new_value if new_value is None or isinstance(new_value, (dict, list)) else {"value": new_value},
        "reason": reason[:500],
        "actor": actor[:200],
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    if _table_ok is not False:
        try:
            db.table("audit_log").insert(entry).execute()
            _table_ok = True
            return
        except Exception:
            _table_ok = False
    try:
        blob = json.dumps(entry, default=str).encode()
        name = f"{entry['created_at'][:10]}/{uuid.uuid4()}.json"
        try:
            db.storage.from_("audit").upload(name, blob, {"content-type": "application/json"})
        except Exception:
            db.storage.create_bucket("audit")
            db.storage.from_("audit").upload(name, blob, {"content-type": "application/json"})
    except Exception:
        pass  # auditing must never break the main operation
