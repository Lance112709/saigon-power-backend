# Database Backups

Two layers:

1. **Supabase Pro daily backups** — 7 days, restore from the Supabase dashboard
   (Database → Backups). First choice for "undo last night".
2. **Nightly off-site dump** — `.github/workflows/db-backup.yml`, runs ~3:17am
   Central on GitHub Actions. `pg_dump` of the `public` schema → Google Drive
   folder **Saigon Power CRM Backups**:
   - `daily/` — last 30 nights (`saigon-crm-YYYY-MM-DD.dump`, UTC date)
   - `monthly/` — the 1st of each month, kept ~13 months
   - `storage/` — mirror of the Supabase Storage buckets (only if the S3
     secrets below are set)

The job fails loudly (GitHub emails the repo owner) if the dump is under 5 MB,
has fewer than 20 tables, is missing a core table, or didn't land in Drive.
Run it on demand: GitHub → Actions → *Nightly database backup* → Run workflow.

## One-time setup (GitHub → repo Settings → Secrets and variables → Actions)

| Secret | Value |
| --- | --- |
| `SUPABASE_DB_URL` | Supabase → **Connect** → *Session pooler* URI (port 5432, host `aws-…pooler.supabase.com`, user `postgres.larwckswepsgvtsdgthv`) with the database password filled in. **Not** the direct connection — it is IPv6-only and GitHub runners are IPv4. URL-encode special characters in the password. |
| `GDRIVE_RCLONE_TOKEN` | The JSON blob (`{"access_token":…}`) printed by `rclone authorize "drive" --drive-scope drive.file` run on your own machine, signed in to the Google account that should hold the backups. `drive.file` scope = the token can only touch files this job created. |

Optional, to also back up uploaded files (contracts, attachments, statements) —
Supabase → Storage → Settings → S3 Connection → enable + *New access key*:

| Secret | Value |
| --- | --- |
| `SUPABASE_S3_ENDPOINT` | `https://larwckswepsgvtsdgthv.storage.supabase.co/storage/v1/s3` |
| `SUPABASE_S3_REGION` | Region shown on that page |
| `SUPABASE_S3_ACCESS_KEY_ID` / `SUPABASE_S3_SECRET_ACCESS_KEY` | The generated key pair |

## Restore

Download the `.dump` from Drive. Needs Postgres 17 client tools (`pg_restore`).

**Look inside / pull one table back** (the usual case — a bad bulk edit):

```bash
pg_restore --list saigon-crm-2026-09-21.dump | grep "TABLE DATA"
# restore into a scratch database, then copy the rows you need back
createdb crm_restore
pg_restore --no-owner --no-privileges -d crm_restore saigon-crm-2026-09-21.dump
```

**Full restore into a fresh Supabase project** (disaster):

```bash
pg_restore --no-owner --no-privileges --clean --if-exists \
  -d "<new project session-pooler URI>" saigon-crm-2026-09-21.dump
```

Then point Railway's `SUPABASE_URL` / key env vars at the new project and
re-upload `storage/` into the matching buckets.

Never `--clean` restore over the live database without taking a fresh dump first.

## Not covered

- Supabase roles/RLS grants (`--no-privileges`) — the app uses the service key,
  so nothing depends on them.
- The dump in Drive is **not encrypted**; it contains customer PII. Keep the
  Drive folder unshared.
