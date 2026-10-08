-- 022: Account Flag on imported CRM customers (same values as leads.account_flag).
-- Leads already had VIP Client / Red Flag / 65+ / Payment Assistance Programs;
-- adds GHOST on both and gives crm_customers its own flag column.
ALTER TABLE crm_customers ADD COLUMN IF NOT EXISTS account_flag TEXT;
