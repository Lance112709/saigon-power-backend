-- 014: Renew Deal modal parity with the lead deal form.
-- Adds the lead-deal fields crm_deals was missing so a renewal can capture
-- the full contract details (plan, rate type, service order, usage, city/state/zip).

alter table crm_deals add column if not exists plan_name text;
alter table crm_deals add column if not exists rate_type text;
alter table crm_deals add column if not exists service_order_type text;
alter table crm_deals add column if not exists est_kwh numeric;
alter table crm_deals add column if not exists service_city text;
alter table crm_deals add column if not exists service_state text;
alter table crm_deals add column if not exists service_zip text;
