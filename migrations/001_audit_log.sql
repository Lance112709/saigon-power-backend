-- ============================================================
-- OPTIONAL UPGRADE: proper audit_log table
-- Paste into Supabase → SQL Editor → Run.
--
-- The app works without this: until the table exists, audit
-- entries are written as JSON files to the 'audit' storage
-- bucket. Once this table is created, entries go here instead
-- (faster to query, joinable, indexed).
-- ============================================================

CREATE TABLE IF NOT EXISTS audit_log (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    table_name  TEXT NOT NULL,
    record_id   TEXT NOT NULL,
    action      TEXT NOT NULL,
    old_value   JSONB,
    new_value   JSONB,
    reason      TEXT,
    actor       TEXT NOT NULL DEFAULT 'system',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_log_table_record ON audit_log(table_name, record_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_created ON audit_log(created_at DESC);
