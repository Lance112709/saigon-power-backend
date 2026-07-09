-- 006_giadienre_billing.sql — Helcim card-on-file + billing for GiaDienRe memberships
-- Run in the Supabase SQL editor BEFORE deploying the backend that uses it.
--
-- Card data itself never touches our systems: Helcim tokenizes the card in
-- their iframe (HelcimPay.js) and we store only the customer/card tokens and
-- display metadata (last4/brand/expiry).

ALTER TABLE giadienre_subscriptions
    ADD COLUMN IF NOT EXISTS helcim_customer_code TEXT,
    ADD COLUMN IF NOT EXISTS helcim_customer_id TEXT,
    ADD COLUMN IF NOT EXISTS helcim_card_token TEXT,
    ADD COLUMN IF NOT EXISTS helcim_subscription_id TEXT,
    ADD COLUMN IF NOT EXISTS card_last4 TEXT,
    ADD COLUMN IF NOT EXISTS card_brand TEXT,
    ADD COLUMN IF NOT EXISTS card_expiry TEXT,          -- MM/YY for display only
    ADD COLUMN IF NOT EXISTS card_updated_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_payment_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_payment_amount NUMERIC(10,2),
    ADD COLUMN IF NOT EXISTS next_billing_date DATE;

CREATE INDEX IF NOT EXISTS idx_gdr_subs_helcim_customer
    ON giadienre_subscriptions (helcim_customer_code);
