-- ============================================================
-- Provider-reported account status on deals
-- Paste into Supabase → SQL Editor → Run. Additive & re-runnable.
--
-- Filled automatically when commission statements are imported:
--   provider_status        e.g. Active / Inactive / Going Final
--   provider_status_date   statement month it came from
--   provider_status_source provider + file that reported it
-- ============================================================

ALTER TABLE lead_deals ADD COLUMN IF NOT EXISTS provider_status TEXT;
ALTER TABLE lead_deals ADD COLUMN IF NOT EXISTS provider_status_date DATE;
ALTER TABLE lead_deals ADD COLUMN IF NOT EXISTS provider_status_source TEXT;

ALTER TABLE crm_deals ADD COLUMN IF NOT EXISTS provider_status TEXT;
ALTER TABLE crm_deals ADD COLUMN IF NOT EXISTS provider_status_date DATE;
ALTER TABLE crm_deals ADD COLUMN IF NOT EXISTS provider_status_source TEXT;

CREATE INDEX IF NOT EXISTS idx_lead_deals_provider_status ON lead_deals(provider_status);
CREATE INDEX IF NOT EXISTS idx_crm_deals_provider_status ON crm_deals(provider_status);
