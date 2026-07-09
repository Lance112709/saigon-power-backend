-- 006: per-provider auto-publish for the Phase 2 email automation.
-- true  = matrices imported from email publish to agents immediately
-- false = they land as drafts awaiting admin approval on /pricing/admin
alter table pricing_providers
  add column if not exists auto_publish boolean not null default true;
