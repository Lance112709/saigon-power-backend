-- 013: Separate termination date from contract end date.
-- The Terminate button used to overwrite contract_end_date with the termination
-- date; store it in its own column instead so the contract end date is preserved.

alter table crm_deals  add column if not exists terminated_date date;
alter table lead_deals add column if not exists terminated_date date;

-- Backfill: for deals already terminated, the value previously written into the
-- end-date column IS the termination date (the original end date is not recoverable).
update crm_deals
   set terminated_date = contract_end_date
 where deal_status = 'INACTIVE'
   and terminated_date is null
   and contract_end_date is not null;

update lead_deals
   set terminated_date = end_date
 where lower(status) = 'inactive'
   and terminated_date is null
   and end_date is not null;

-- Restore the Iron Horse deal (Tai Phan, 20602 TUPELO RIDGE DR) whose end date
-- was clobbered to the termination date (2026-08-17) before being reactivated.
-- Original: start 2024-08-28 + 36-month term.
update crm_deals
   set contract_end_date = '2027-08-28'
 where esiid = '1008901022900941410115'
   and deal_status = 'ACTIVE'
   and contract_end_date = '2026-08-17';
