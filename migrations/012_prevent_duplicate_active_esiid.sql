-- 012: Block duplicate ACTIVE deals per meter at the database level.
-- The July 2026 duplicate wave came from a client writing straight to
-- PostgREST, bypassing every application-side guard — so the guard has to
-- live in the database. A trigger (not a unique index) because ~42 meters
-- still carry two active deals pending the provider-switch cleanup; the
-- trigger only rejects NEW violations and tolerates existing rows.
-- Swap for a partial unique index once those are resolved:
--   CREATE UNIQUE INDEX uq_crm_deals_active_esiid ON crm_deals(esiid)
--     WHERE deal_status = 'ACTIVE' AND esiid IS NOT NULL;

CREATE OR REPLACE FUNCTION prevent_duplicate_active_esiid()
RETURNS trigger AS $$
BEGIN
    IF NEW.esiid IS NOT NULL AND btrim(NEW.esiid) <> ''
       AND upper(coalesce(NEW.deal_status, '')) = 'ACTIVE' THEN
        IF EXISTS (
            SELECT 1 FROM crm_deals
            WHERE esiid = NEW.esiid
              AND deal_status = 'ACTIVE'
              AND id <> NEW.id
        ) THEN
            RAISE EXCEPTION
                'ESI ID % already has an active deal in crm_deals — link or deactivate the existing deal instead of creating a duplicate',
                NEW.esiid
                USING ERRCODE = '23505';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_duplicate_active_esiid ON crm_deals;
CREATE TRIGGER trg_prevent_duplicate_active_esiid
    BEFORE INSERT OR UPDATE OF esiid, deal_status ON crm_deals
    FOR EACH ROW
    EXECUTE FUNCTION prevent_duplicate_active_esiid();
