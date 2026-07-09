-- 005_giadienre.sql — GiaDienRe website subscription intake
-- Run this in the Supabase SQL editor BEFORE deploying the backend that uses it.
--
-- Design notes:
--   * crm_customers stays the single source of truth for customer identity.
--     Every subscription is linked via crm_customer_id (created or deduped
--     by email/phone at intake time).
--   * Future customer-portal ready: portal auth can key off crm_customer_id
--     + email/phone on this record. No credentials are stored here.

CREATE TABLE IF NOT EXISTS giadienre_subscriptions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    crm_customer_id UUID REFERENCES crm_customers(id) ON DELETE SET NULL,

    -- contact / service info captured on the website
    full_name TEXT NOT NULL,
    email TEXT,
    phone TEXT,
    phone_digits TEXT,                 -- normalized digits-only, for dedupe
    service_address TEXT,
    city TEXT,
    state TEXT DEFAULT 'TX',
    zip TEXT,
    utility_provider TEXT,             -- TDU, e.g. CenterPoint
    current_provider TEXT,             -- current REP, e.g. TXU
    contract_end_date DATE,

    -- subscription details
    plan_id TEXT,                      -- managed | managed-plus
    plan_name TEXT,
    billing_cycle TEXT,                -- monthly | annual
    form_type TEXT NOT NULL DEFAULT 'signup',   -- signup | bill_analysis
    status TEXT NOT NULL DEFAULT 'NEW',         -- NEW | CONTACTED | ACTIVE | CANCELLED
    lead_source TEXT NOT NULL DEFAULT 'GiaDienRe Website',
    assigned_agent TEXT,

    -- anything else the website collects, schema-free
    extra JSONB DEFAULT '{}'::jsonb,

    -- intake bookkeeping
    client_ip TEXT,
    submission_count INT DEFAULT 1,    -- bumped when the same person re-submits
    subscribed_at TIMESTAMPTZ DEFAULT now(),
    last_submission_at TIMESTAMPTZ DEFAULT now(),
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_gdr_subs_email ON giadienre_subscriptions (lower(email));
CREATE INDEX IF NOT EXISTS idx_gdr_subs_phone_digits ON giadienre_subscriptions (phone_digits);
CREATE INDEX IF NOT EXISTS idx_gdr_subs_status ON giadienre_subscriptions (status);
CREATE INDEX IF NOT EXISTS idx_gdr_subs_created ON giadienre_subscriptions (created_at);
CREATE INDEX IF NOT EXISTS idx_gdr_subs_customer ON giadienre_subscriptions (crm_customer_id);

CREATE TABLE IF NOT EXISTS giadienre_subscription_notes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscription_id UUID NOT NULL REFERENCES giadienre_subscriptions(id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    author_name TEXT,
    is_internal BOOLEAN DEFAULT false, -- true = internal comment, false = customer-facing note
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_gdr_notes_sub ON giadienre_subscription_notes (subscription_id);
