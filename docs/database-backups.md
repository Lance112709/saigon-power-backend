# Database Backups

Two layers:

1. **Supabase Pro daily backups** — 7 days, restore from the Supabase dashboard
   (Database → Backups). First choice for "undo last night".
2. **Nightly off-site dump** — `.github/workflows/db-backup.yml`, runs ~3:17am
   Central on GitHub Actions. `pg_dump` of the `public` schema, **encrypted**,
   then kept as a GitHub Actions artifact for 30 days (GitHub → Actions →
   *Nightly database backup* → pick a run → Artifacts → `saigon-crm-YYYY-MM-DD`).
   If `GDRIVE_RCLONE_TOKEN` is set it also goes to Google Drive folder
   **Saigon Power CRM Backups** (`daily/` 30 nights, `monthly/` ~13 months,
   `storage/` = Supabase Storage mirror when the S3 secrets are set).

Each backup is three files: `<name>.dump.enc` (AES-256 encrypted dump),
`<name>.dump.key.enc` (the AES key, RSA-wrapped), `<name>.dump.sha256`.
The repo is public, so nothing unencrypted ever leaves the runner.

The job fails loudly (GitHub emails the repo owner) if the dump is under 5 MB,
has fewer than 20 tables, or is missing a core table.
Run it on demand: GitHub → Actions → *Nightly database backup* → Run workflow.

## Secrets (GitHub → repo Settings → Secrets and variables → Actions)

| Secret | Required | Value |
| --- | --- | --- |
| `SUPABASE_DB_PASSWORD` | **yes** | The Postgres password (Supabase → Database → Settings → *Reset database password* if unknown; nothing else uses it — the app connects via the service key). Host/user/port are hardcoded in the workflow (session pooler, project `larwckswepsgvtsdgthv`). |
| `GDRIVE_RCLONE_TOKEN` | no | JSON printed by `rclone authorize "drive" --drive-scope drive.file` on your own machine. Enables the Drive copy. |
| `SUPABASE_S3_ENDPOINT` / `_REGION` / `_ACCESS_KEY_ID` / `_SECRET_ACCESS_KEY` | no | Supabase → Storage → Settings → S3 Connection. Enables the Storage mirror (needs Drive too). |

## Encryption keys

- Public key: `.github/backup-public.pem` (in repo, used by the job).
- **Private key: `~/.saigon-backup/backup-private.pem` on Lance's Mac**, with a
  copy in Google Drive (`saigon-crm-backup-private-key.pem`). Without it the
  backups cannot be read. Never commit it.

To rotate: `openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:4096 -out backup-private.pem`,
`openssl pkey -in backup-private.pem -pubout -out .github/backup-public.pem`, commit the
public key, keep the old private key for old backups.

## Restore

Download the three files (artifact zip from GitHub, or from Drive). Needs
Postgres 17 client tools (`pg_restore`) and `openssl` (built into macOS).

```bash
F=saigon-crm-2026-10-02.dump
sha256sum -c "$F.sha256"                                   # (macOS: shasum -a 256 -c)
openssl pkeyutl -decrypt -inkey ~/.saigon-backup/backup-private.pem \
  -pkeyopt rsa_padding_mode:oaep -in "$F.key.enc" -out dump.key
openssl enc -d -aes-256-cbc -pbkdf2 -iter 100000 -in "$F.enc" -out "$F" -pass file:dump.key
rm dump.key
```

**Look inside / pull one table back** (the usual case — a bad bulk edit):

```bash
pg_restore --list "$F" | grep "TABLE DATA"
createdb crm_restore                      # scratch DB, then copy rows you need
pg_restore --no-owner --no-privileges -d crm_restore "$F"
```

**Full restore into a fresh Supabase project** (disaster):

```bash
pg_restore --no-owner --no-privileges --clean --if-exists \
  -d "<new project session-pooler URI>" "$F"
```

Then point Railway's `SUPABASE_URL` / key env vars at the new project and
re-upload `storage/` into the matching buckets.

Never `--clean` restore over the live database without taking a fresh dump first.

## Not covered

- Supabase roles/RLS grants (`--no-privileges`) — the app uses the service key,
  so nothing depends on them.
- Uploaded files (contracts, attachments, statements) unless the Storage mirror
  is enabled.
