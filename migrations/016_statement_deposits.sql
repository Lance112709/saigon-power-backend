-- 016: bank-deposit check per commission statement.
-- amount_received already exists on upload_batches (typed at upload time, rarely
-- used because most statements arrive by email). These columns let the deposit be
-- recorded AFTER it lands in the bank, and store the statement's own withholding
-- so a deposit that is short by exactly the withheld amount is auto-explained.
ALTER TABLE upload_batches
    ADD COLUMN IF NOT EXISTS total_withheld       NUMERIC(12,2),   -- from the statement (NRG Summary "Total Withheld")
    ADD COLUMN IF NOT EXISTS expected_pay_date    DATE,            -- from the statement (NRG Summary "Pay Date") when present
    ADD COLUMN IF NOT EXISTS received_at          DATE,            -- bank posting date of the deposit
    ADD COLUMN IF NOT EXISTS received_notes       TEXT,            -- e.g. "wire fee $15", "covers Jun+Jul"
    ADD COLUMN IF NOT EXISTS received_by          TEXT,            -- CRM user who recorded it
    ADD COLUMN IF NOT EXISTS received_recorded_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_upload_batches_received_at ON upload_batches (received_at);
