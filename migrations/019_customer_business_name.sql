-- 019: Account-level business name on CRM customers.
-- Business names lived only on deals (crm_deals.business_name); commercial
-- accounts like BLISS NAILS SPA had no place to record the business on the
-- customer itself. Shown under the customer's name and searchable.
ALTER TABLE crm_customers ADD COLUMN IF NOT EXISTS business_name TEXT;
CREATE INDEX IF NOT EXISTS idx_crm_customers_business_name ON crm_customers (business_name);
