-- 017: bank deposits ingested from Chase "direct deposit posted" alert emails
-- (or a CSV drop). Only provider-relevant fields are kept: date, amount, last4.
CREATE TABLE IF NOT EXISTS bank_deposits (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source             TEXT NOT NULL DEFAULT 'chase_alert',   -- chase_alert | csv | manual
    external_id        TEXT UNIQUE,                            -- email Message-ID / CSV row hash
    posted_at          DATE NOT NULL,
    amount             NUMERIC(12,2) NOT NULL,
    account_last4      TEXT,
    payer              TEXT,                                   -- bank payer name when known (CSV); null for alerts
    subject            TEXT,
    status             TEXT NOT NULL DEFAULT 'unmatched',      -- unmatched | matched | ignored
    upload_batch_id    UUID REFERENCES upload_batches(id) ON DELETE SET NULL,
    matched_how        TEXT,
    likely_supplier_id UUID REFERENCES suppliers(id) ON DELETE SET NULL,
    notes              TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_bank_deposits_status_posted ON bank_deposits (status, posted_at);
