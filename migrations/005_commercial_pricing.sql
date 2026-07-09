-- 005: Commercial Pricing Engine (Phase 1 - NRG)
-- Provider registry with per-provider margins, and versioned pricing uploads.
-- Parsed rate rows (360k+/day) live as immutable gzipped blobs in storage;
-- these tables hold configuration, versions, and import metadata.

create table if not exists pricing_providers (
  id          uuid primary key default gen_random_uuid(),
  code        text not null unique,
  name        text not null,
  margin      numeric(8,5) not null default 0.003,   -- $/kWh added to every rate
  active      boolean not null default true,
  created_at  timestamptz not null default now()
);

create table if not exists pricing_uploads (
  id                uuid primary key default gen_random_uuid(),
  provider_id       uuid not null references pricing_providers(id),
  version           int not null,
  status            text not null default 'draft',   -- draft | published | archived
  original_filename text,
  file_hash         text,
  storage_path      text,                            -- gzipped parsed rows blob
  effective_date    date,
  expiration_at     timestamptz,
  rows_imported     int not null default 0,
  margin_used       numeric(8,5) not null,
  uploaded_by       text,
  published_at      timestamptz,
  import_log        jsonb,                           -- parser warnings & stats
  summary           jsonb,                           -- distinct utilities/zones/terms/products
  created_at        timestamptz not null default now()
);

create index if not exists idx_pricing_uploads_provider_status
  on pricing_uploads (provider_id, status, created_at desc);

insert into pricing_providers (code, name, margin) values
  ('NRG', 'NRG', 0.003),
  ('ENGIE', 'ENGIE', 0.003),
  ('CONSTELLATION', 'Constellation', 0.003),
  ('DIRECT_ENERGY', 'Direct Energy', 0.003),
  ('SHELL', 'Shell Energy', 0.003)
on conflict (code) do nothing;
