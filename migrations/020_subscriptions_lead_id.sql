-- 020: Let a SmartCare (POWER PLUS) membership attach to a lead as well as a CRM customer.
-- Converted leads live in `leads` (not crm_customers), so the manual SmartCare badge
-- on the lead page needs its own link column. Nullable; real web signups still link
-- by crm_customer_id.
ALTER TABLE giadienre_subscriptions ADD COLUMN IF NOT EXISTS lead_id UUID REFERENCES leads(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_giadienre_subscriptions_lead_id ON giadienre_subscriptions (lead_id);
