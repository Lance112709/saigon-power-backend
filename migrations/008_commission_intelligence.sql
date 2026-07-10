-- Commission Intelligence & Audit Engine (Phase 1).
-- Versioned provider commission rules, permanent expected-vs-paid snapshots,
-- grouped audit findings, durable exception-case workflow, and disputes.
-- Applied via the Supabase SQL editor.

-- Per-provider commission rules. Never deleted: each change closes the current
-- version (effective_to + superseded_by) and inserts version n+1.
create table if not exists commission_rules (
    id             uuid primary key default gen_random_uuid(),
    supplier_id    uuid not null references suppliers(id),
    name           text not null,
    rule_type      text not null check (rule_type in ('rate_per_kwh','flat_fee','tiered','hybrid')),
    config         jsonb not null default '{}',  -- {rate, rate_source:'fixed'|'deal_adder', flat_amount, tiers:[{min_kwh,max_kwh,rate}]}
    effective_from date not null,
    effective_to   date,                          -- null = current version
    version        int not null default 1,
    superseded_by  uuid references commission_rules(id),
    notes          text,
    created_by     text,
    created_at     timestamptz not null default now()
);
create index if not exists idx_comm_rules_supplier on commission_rules (supplier_id, effective_from desc);

-- Every expected-vs-actual calculation, kept forever. Reconciliation re-runs
-- append new rows; nothing here is ever updated or deleted.
create table if not exists expected_commission_snapshots (
    id                     uuid primary key default gen_random_uuid(),
    reconciliation_run_id  uuid references reconciliation_runs(id) on delete set null,
    supplier_id            uuid not null,
    billing_month          date not null,
    esiid                  text not null,
    deal_source            text,
    deal_id                uuid,
    rule_id                uuid,
    rule_version           int,
    expected_amount        numeric(12,4),
    actual_amount          numeric(12,4),
    variance_amount        numeric(12,4),
    kwh                    numeric(12,2),
    rate_expected          numeric(10,6),
    rate_paid              numeric(10,6),
    calc_method            text not null,   -- 'rule' | 'adder' | 'actual_plus_loss' | 'estimate'
    calc_detail            jsonb,
    status                 text not null,   -- matched | short_paid | over_paid | missing | unexpected
    created_at             timestamptz not null default now()
);
create index if not exists idx_snapshots_esiid on expected_commission_snapshots (esiid, billing_month desc);
create index if not exists idx_snapshots_month on expected_commission_snapshots (supplier_id, billing_month);

-- Grouped/systemic audit detections (one row per pattern, not per account).
-- fingerprint makes re-runs idempotent: the same detection updates in place.
create table if not exists audit_findings (
    id                     uuid primary key default gen_random_uuid(),
    supplier_id            uuid not null,
    billing_month          date not null,
    reconciliation_run_id  uuid references reconciliation_runs(id) on delete set null,
    finding_type           text not null check (finding_type in
        ('systemic_rate_change','churned_still_paid','clawback_anomaly',
         'term_mismatch','payment_stopped','out_of_range')),
    severity               text not null default 'high',
    title                  text not null,
    explanation            text not null,   -- plain-English WHY
    affected_count         int not null default 0,
    estimated_impact       numeric(12,2) not null default 0,
    details                jsonb,           -- per-esiid breakdown
    fingerprint            text not null unique,
    status                 text not null default 'open' check (status in
        ('open','investigating','disputed','resolved','dismissed')),
    created_at             timestamptz not null default now(),
    updated_at             timestamptz not null default now(),
    resolved_at            timestamptz,
    resolved_by            text
);
create index if not exists idx_findings_month on audit_findings (supplier_id, billing_month desc);
create index if not exists idx_findings_status on audit_findings (status);

-- Durable per-account exception workflow. Survives reconciliation re-runs
-- (runs/items are deleted and recreated; cases upsert on the natural key and
-- keep their workflow status, notes, and recovered dollars).
create table if not exists exception_cases (
    id                 uuid primary key default gen_random_uuid(),
    supplier_id        uuid not null,
    billing_month      date not null,
    esiid              text not null,
    issue_type         text not null,   -- missing | short_paid | over_paid | unexpected | finding types
    workflow_status    text not null default 'open' check (workflow_status in
        ('open','investigating','waiting_on_provider','resolved','recovered','ignored')),
    priority_score     numeric(10,2) not null default 0,
    estimated_loss     numeric(12,2) not null default 0,
    recovered_amount   numeric(12,2) not null default 0,
    recommended_action text,
    explanation        text,
    customer_name      text,
    agent              text,
    finding_id         uuid references audit_findings(id) on delete set null,
    dispute_id         uuid,
    first_seen_run_id  uuid,
    last_seen_run_id   uuid,
    last_seen_at       timestamptz,
    notes              text,
    created_at         timestamptz not null default now(),
    updated_at         timestamptz not null default now(),
    unique (supplier_id, billing_month, esiid, issue_type)
);
create index if not exists idx_cases_status on exception_cases (workflow_status, supplier_id);
create index if not exists idx_cases_month on exception_cases (billing_month desc);

-- Provider dispute packages. Drafted by the engine, sent only after a human
-- reviews and clicks Send. Tracks provider responses and recovered dollars.
create table if not exists disputes (
    id               uuid primary key default gen_random_uuid(),
    supplier_id      uuid not null references suppliers(id),
    status           text not null default 'draft' check (status in
        ('draft','sent','provider_responded','recovered','rejected')),
    title            text not null,
    email_to         text,
    email_subject    text,
    email_body       text,            -- editable while draft
    attachment_path  text,            -- storage: disputes/{id}.xlsx
    months           jsonb,
    total_claimed    numeric(12,2) not null default 0,
    total_recovered  numeric(12,2) not null default 0,
    created_by       text,
    created_at       timestamptz not null default now(),
    sent_at          timestamptz,
    responded_at     timestamptz,
    closed_at        timestamptz,
    notes            text
);
create index if not exists idx_disputes_supplier on disputes (supplier_id, created_at desc);

create table if not exists dispute_items (
    id                uuid primary key default gen_random_uuid(),
    dispute_id        uuid not null references disputes(id) on delete cascade,
    case_id           uuid references exception_cases(id) on delete set null,
    esiid             text,
    billing_month     date,
    claimed_amount    numeric(12,2) not null default 0,
    recovered_amount  numeric(12,2) not null default 0,
    status            text not null default 'open'
);
create index if not exists idx_dispute_items_dispute on dispute_items (dispute_id);

-- Link items produced by a run to their durable case/finding (nullable — no
-- impact on existing reconciliation code paths).
alter table reconciliation_items add column if not exists case_id uuid;
alter table reconciliation_items add column if not exists finding_id uuid;
