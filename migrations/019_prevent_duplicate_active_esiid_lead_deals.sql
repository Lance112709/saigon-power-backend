-- 019: Same backstop as 012, for the pipeline deals table. An Active lead deal
-- may not share its ESI ID with another Active lead deal OR an ACTIVE crm deal.
-- Trigger, not a unique index, so pre-existing rows are tolerated and only new
-- violations are rejected (mirrors 012). Rollback:
--   DROP TRIGGER IF EXISTS trg_prevent_duplicate_active_esiid_lead ON lead_deals;
--   DROP FUNCTION IF EXISTS prevent_duplicate_active_esiid_lead();

CREATE OR REPLACE FUNCTION prevent_duplicate_active_esiid_lead()
RETURNS trigger AS $$
BEGIN
    IF NEW.esiid IS NOT NULL AND btrim(NEW.esiid) <> ''
       AND lower(coalesce(NEW.status, '')) = 'active' THEN
        IF EXISTS (
            SELECT 1 FROM lead_deals
            WHERE esiid = NEW.esiid
              AND status = 'Active'
              AND id <> NEW.id
        ) THEN
            RAISE EXCEPTION
                'ESI ID % already has an active deal in lead_deals — link or deactivate the existing deal instead of creating a duplicate',
                NEW.esiid
                USING ERRCODE = '23505';
        END IF;
        IF EXISTS (
            SELECT 1 FROM crm_deals
            WHERE esiid = NEW.esiid
              AND deal_status = 'ACTIVE'
        ) THEN
            RAISE EXCEPTION
                'ESI ID % already has an active deal in crm_deals — renew or terminate that deal first',
                NEW.esiid
                USING ERRCODE = '23505';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_duplicate_active_esiid_lead ON lead_deals;
CREATE TRIGGER trg_prevent_duplicate_active_esiid_lead
    BEFORE INSERT OR UPDATE OF esiid, status ON lead_deals
    FOR EACH ROW
    EXECUTE FUNCTION prevent_duplicate_active_esiid_lead();
