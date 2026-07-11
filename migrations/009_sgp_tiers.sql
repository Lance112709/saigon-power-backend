-- SGP Agent Commission Structure (tier-based permanent splits 50/50 -> 70/30).
-- Additive only: the whole program is opt-in per agent — nothing changes for
-- any agent until an admin sets classification = 'SGP_AGENT' and approves the
-- agreement. Applied via the Supabase SQL editor.
--
-- Rollback (export /api/v1/sgp/export first — history is not recoverable):
--   drop table if exists sgp_tier_progress, sgp_tier_history, sgp_settings, sgp_tiers;
--   alter table sales_agents
--     drop column if exists classification, drop column if exists agreement_status,
--     drop column if exists agreement_version, drop column if exists agreement_signed_at,
--     drop column if exists agreement_approved_at, drop column if exists agreement_effective_at,
--     drop column if exists agreement_terminated_at, drop column if exists agreement_notes,
--     drop column if exists agreement_doc_url, drop column if exists current_tier,
--     drop column if exists tier_effective_from, drop column if exists sgp_suspended;

alter table sales_agents
  add column if not exists classification text
      check (classification in ('SGP_AGENT','REFERRAL_PARTNER','INTERNAL_EMPLOYEE','TEAM_LEADER','INACTIVE_AGENT')),
  add column if not exists agreement_status text not null default 'NOT_SENT'
      check (agreement_status in ('NOT_SENT','SENT','PENDING_SIGNATURE','SIGNED','APPROVED','REJECTED','EXPIRED','TERMINATED')),
  add column if not exists agreement_version text,
  add column if not exists agreement_signed_at timestamptz,
  add column if not exists agreement_approved_at timestamptz,
  add column if not exists agreement_effective_at date,
  add column if not exists agreement_terminated_at timestamptz,
  add column if not exists agreement_notes text,
  add column if not exists agreement_doc_url text,
  add column if not exists current_tier int,
  add column if not exists tier_effective_from date,
  add column if not exists sgp_suspended boolean not null default false;

-- The tier ladder. agent_split is HARD-CAPPED at 70 by the check constraint;
-- names are configurable here without code changes.
create table if not exists sgp_tiers (
  id uuid primary key default gen_random_uuid(),
  tier_order int not null unique,
  name text not null,
  monthly_gp_threshold numeric(12,2) not null default 0,
  required_qualifying_months int not null default 3,
  agent_split numeric(5,2) not null check (agent_split <= 70),
  company_split numeric(5,2) not null,
  is_max boolean not null default false,
  active boolean not null default true,
  created_at timestamptz not null default now()
);
insert into sgp_tiers (tier_order, name, monthly_gp_threshold, required_qualifying_months, agent_split, company_split, is_max) values
  (1, 'Partner',         0,     0, 50, 50, false),
  (2, 'Growth Partner',  5000,  3, 55, 45, false),
  (3, 'Senior Partner',  10000, 3, 60, 40, false),
  (4, 'Premier Partner', 15000, 3, 65, 35, false),
  (5, 'Elite Partner',   20000, 3, 70, 30, true)
on conflict (tier_order) do nothing;

-- One row per (agent, target tier, calendar month). The unique key makes
-- re-evaluation idempotent and double-counting structurally impossible.
create table if not exists sgp_tier_progress (
  id uuid primary key default gen_random_uuid(),
  agent_id uuid not null references sales_agents(id) on delete cascade,
  tier_order int not null,
  qualifying_month date not null,
  eligible_gp numeric(12,2) not null,
  basis text not null,
  evaluated_at timestamptz not null default now(),
  unique (agent_id, tier_order, qualifying_month)
);
create index if not exists idx_sgp_progress_agent on sgp_tier_progress (agent_id, tier_order);

-- Append-only promotion record (tiers are permanent — rows are never deleted).
create table if not exists sgp_tier_history (
  id uuid primary key default gen_random_uuid(),
  agent_id uuid not null references sales_agents(id) on delete cascade,
  previous_tier int,
  new_tier int not null,
  reason text,
  effective_from date not null,
  promoted_by text,
  automatic boolean not null default true,
  created_at timestamptz not null default now()
);
create index if not exists idx_sgp_history_agent on sgp_tier_history (agent_id, created_at desc);

create table if not exists sgp_settings (
  id int primary key default 1 check (id = 1),
  qualification_basis text not null default 'PROVIDER_PAID_GP'
      check (qualification_basis in ('PROVIDER_PAID_GP','FINALIZED_GP')),
  promotion_effective_rule text not null default 'NEXT_COMMISSION_PERIOD'
      check (promotion_effective_rule in ('IMMEDIATE','NEXT_DEAL','NEXT_COMMISSION_PERIOD','NEXT_CALENDAR_MONTH')),
  updated_at timestamptz not null default now(),
  updated_by text
);
insert into sgp_settings (id) values (1) on conflict (id) do nothing;
