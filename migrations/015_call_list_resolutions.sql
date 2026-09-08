-- 015: "Resolved" button on the Who To Call list.
-- The call list is computed on the fly from active deals, so a resolved mark
-- has to be stored separately. A resolution is tied to the customer AND the
-- contract end date that put them on the list: once the customer renews (new
-- end date) they legitimately come back for the next cycle.

CREATE TABLE IF NOT EXISTS call_list_resolutions (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_key       TEXT NOT NULL,            -- 'lead:<lead_id>' | 'crm:<customer_id>' | 'crmdeal:<deal_id>'
    end_date         DATE,                     -- contract end date shown on the list when resolved
    resolved_by      TEXT,                     -- user id
    resolved_by_name TEXT,
    resolved_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    note             TEXT
);

-- One resolution per customer per contract cycle (NULL end dates collapse to one).
CREATE UNIQUE INDEX IF NOT EXISTS call_list_resolutions_cycle_uidx
    ON call_list_resolutions (entity_key, COALESCE(end_date, DATE '1900-01-01'));

CREATE INDEX IF NOT EXISTS call_list_resolutions_resolved_at_idx
    ON call_list_resolutions (resolved_at DESC);
