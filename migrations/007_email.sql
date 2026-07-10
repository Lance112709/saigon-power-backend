-- Customer email: reusable templates + a send log (mirrors the SMS tables).
-- Applied via the Supabase SQL editor.

create table if not exists email_templates (
    id            uuid primary key default gen_random_uuid(),
    name          text not null,
    subject       text not null,
    body          text not null,           -- plain text w/ {{merge}} tags; newlines become <br> on send
    description   text,
    is_active     boolean not null default true,
    created_at    timestamptz not null default now(),
    updated_at    timestamptz not null default now()
);

create table if not exists email_messages (
    id                   uuid primary key default gen_random_uuid(),
    user_id              uuid,             -- staff member who sent it
    lead_id              uuid,
    customer_id          uuid,
    deal_id              uuid,
    to_email             text not null,
    subject              text not null,
    body                 text not null,    -- rendered HTML actually sent
    status               text not null default 'failed',   -- sent | failed
    provider_message_id  text,
    error                text,
    created_at           timestamptz not null default now()
);

create index if not exists idx_email_messages_lead      on email_messages (lead_id);
create index if not exists idx_email_messages_customer  on email_messages (customer_id);
create index if not exists idx_email_messages_deal      on email_messages (deal_id);
create index if not exists idx_email_messages_created   on email_messages (created_at desc);
