-- 018: Per-agent "book from" date for the My Business portal.
-- When set, the agent's portal (and any staff login linked to that agent)
-- only shows enrollments whose contract start is on/after this date, and
-- payouts from that month onward. NULL = full history.
ALTER TABLE sales_agents ADD COLUMN IF NOT EXISTS portal_book_from DATE;
COMMENT ON COLUMN sales_agents.portal_book_from IS
  'My Business portal shows only enrollments starting on/after this date (NULL = all).';

-- Nga Nguyen: portal shows enrollments from June 2026 onward (2026-09-10).
UPDATE sales_agents SET portal_book_from = '2026-06-01' WHERE name = 'Nga Nguyen';
