-- Bulk email campaigns: a saved audience + message that the auto-drip
-- scheduler sends out over time, respecting the Resend plan's daily cap.
-- Applied via the Supabase SQL editor.

create table if not exists email_campaigns (
    id               uuid primary key default gen_random_uuid(),
    name             text not null,
    subject          text not null,          -- may contain {{merge}} tags
    body             text not null,          -- plain text w/ {{merge}} tags; branded on send
    status           text not null default 'sending',  -- sending | paused | completed | canceled
    daily_cap        int,                    -- optional per-campaign override of EMAIL_DAILY_CAP
    audience         jsonb,                  -- snapshot of how the list was built (mode + filters)
    total_recipients int  not null default 0,
    sent_count       int  not null default 0,
    failed_count     int  not null default 0,
    skipped_no_email int  not null default 0,
    created_by       uuid,
    created_by_name  text,
    created_at       timestamptz not null default now(),
    updated_at       timestamptz not null default now(),
    last_run_at      timestamptz,
    completed_at     timestamptz
);

create table if not exists email_campaign_recipients (
    id                   uuid primary key default gen_random_uuid(),
    campaign_id          uuid not null references email_campaigns(id) on delete cascade,
    lead_id              uuid,
    customer_id          uuid,
    to_email             text not null,
    variables            jsonb not null default '{}'::jsonb,  -- frozen merge values for this contact
    status               text not null default 'pending',      -- pending | sent | failed
    provider_message_id  text,
    error                text,
    sent_at              timestamptz,
    created_at           timestamptz not null default now()
);

create index if not exists idx_campaign_recipients_campaign on email_campaign_recipients (campaign_id);
create index if not exists idx_campaign_recipients_status   on email_campaign_recipients (campaign_id, status);
create index if not exists idx_campaigns_status             on email_campaigns (status);

-- Link individual sends back to their campaign (nullable; one-off sends stay null).
alter table email_messages add column if not exists campaign_id uuid;
create index if not exists idx_email_messages_campaign on email_messages (campaign_id);
