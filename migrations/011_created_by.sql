-- 011: Track which user created each CRM customer and deal.
-- Nullable: rows created before this migration have no recorded creator.
ALTER TABLE crm_customers ADD COLUMN IF NOT EXISTS created_by TEXT;
ALTER TABLE crm_deals ADD COLUMN IF NOT EXISTS created_by TEXT;
