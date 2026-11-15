-- 023: per-deal business name on lead deals (crm_deals already has one).
-- One owner can hold several businesses on one account; the deal keeps its own
-- business name and the lead page falls back to leads.business_name when empty.
ALTER TABLE lead_deals ADD COLUMN IF NOT EXISTS business_name TEXT;
