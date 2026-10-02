-- 021: Track which user created each task, so task emails can go to the
-- creator as well as the assignee.
-- Nullable: tasks created before this migration (and system-generated tasks)
-- have no recorded creator.
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS created_by TEXT;      -- display name
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS created_by_id TEXT;   -- users.id
