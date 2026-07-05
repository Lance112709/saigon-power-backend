-- ============================================================
-- Self-service enrollment system
-- Paste into Supabase → SQL Editor → Run. Additive & re-runnable.
--
-- enrollments: customer-submitted signups from the public website
-- provider_integrations: per-provider API config — set endpoint +
--   credentials + payload mapping and enrollments auto-submit
-- ============================================================

CREATE TABLE IF NOT EXISTS enrollments (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    status               TEXT NOT NULL DEFAULT 'submitted',
    -- submitted | needs_review | sent_to_provider | accepted | rejected | cancelled | active
    source               TEXT NOT NULL DEFAULT 'web',

    first_name           TEXT NOT NULL,
    last_name            TEXT NOT NULL,
    email                TEXT,
    phone                TEXT NOT NULL,
    language             TEXT,

    service_address      TEXT NOT NULL,
    service_city         TEXT NOT NULL,
    service_state        TEXT NOT NULL DEFAULT 'TX',
    service_zip          TEXT NOT NULL,
    esiid                TEXT,
    enrollment_type      TEXT,            -- switch | move_in
    requested_start_date DATE,

    plan_id              BIGINT,
    plan_name            TEXT,
    provider             TEXT,
    rate                 NUMERIC(10, 4),
    term_months          INTEGER,

    terms_accepted_at    TIMESTAMPTZ,
    client_ip            TEXT,

    lead_id              UUID,
    deal_id              UUID,
    provider_confirmation TEXT,
    submission_log       JSONB NOT NULL DEFAULT '[]'::jsonb,
    notes                TEXT,

    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_enrollments_status  ON enrollments(status);
CREATE INDEX IF NOT EXISTS idx_enrollments_created ON enrollments(created_at DESC);

CREATE TABLE IF NOT EXISTS provider_integrations (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    provider_name     TEXT NOT NULL UNIQUE,
    integration_type  TEXT NOT NULL DEFAULT 'manual',   -- manual | rest
    endpoint_url      TEXT,
    http_method       TEXT NOT NULL DEFAULT 'POST',
    auth_type         TEXT NOT NULL DEFAULT 'none',     -- none | bearer | basic | api_key_header
    auth_credentials  JSONB NOT NULL DEFAULT '{}'::jsonb,
    extra_headers     JSONB NOT NULL DEFAULT '{}'::jsonb,
    field_mapping     JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active         BOOLEAN NOT NULL DEFAULT FALSE,
    test_mode         BOOLEAN NOT NULL DEFAULT TRUE,
    last_result       JSONB,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
