-- 004: commission payments view + lookup indexes
-- The payments ledger is actual_commissions (populated by the statement
-- import pipeline). This exposes it under the commission_payments name with
-- the agreed field shape, and adds the indexes per-customer lookups need.

create index if not exists idx_actual_commissions_raw_esiid
  on actual_commissions (raw_esiid);

create index if not exists idx_actual_commissions_supplier_month
  on actual_commissions (supplier_id, billing_month);

create index if not exists idx_reconciliation_items_esiid
  on reconciliation_items (esiid);

create or replace view commission_payments as
select
  ac.id,
  coalesce(ac.resolved_esiid, ac.raw_esiid) as esi_id,
  ac.billing_month                          as payment_date,
  ac.raw_amount                             as amount,
  s.name                                    as supplier,
  ac.upload_batch_id                        as statement_reference,
  (ac.raw_row_data -> '_norm' ->> 'deal_id') as deal_id,
  ac.is_matched,
  ac.created_at
from actual_commissions ac
left join suppliers s on s.id = ac.supplier_id;
