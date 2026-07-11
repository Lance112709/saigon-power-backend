# Smart Meter Texas Integration — Design Seam (Phase 2, not built)

Blocked on prerequisites only the business can complete: an SMT account with a
signed data-sharing agreement, customer consent per ESIID, and API credentials.
Once those exist, the integration slots in as follows — no schema changes needed.

## What it adds

Compare **actual metered usage** (SMT) against **provider-reported usage**
(`actual_commissions.raw_kwh`) per ESIID per month. A provider paying on fewer
kWh than the meter recorded is underpaying even at the correct rate — a
discrepancy class the current audit cannot see.

## Where it plugs in

1. **Sync job** — new `app/services/smt_sync.py` with `sync_usage(db, esiids, month)`
   calling the SMT API (OAuth + XML/REST per their spec). Schedule daily at
   ~07:00 in `app/main.py` next to the other cron jobs.
2. **Storage** — new table `smt_usage (esiid, usage_month, metered_kwh, synced_at)`;
   add via a `009_smt_usage.sql` migration in the established paste-into-Supabase style.
3. **Detection** — new detector in `app/services/audit_detections.py`:
   `detect_usage_mismatch(rows_by_esiid, smt_by_esiid, ...)` emitting a grouped
   `audit_findings` row (add `usage_mismatch` to the table's `finding_type`
   CHECK constraint) with per-account `paid_kwh` vs `metered_kwh` and the
   dollar impact at the account's rate. It runs inside `run_extended_audit`
   like every other detector — findings, cases, disputes, and alerting all
   work unchanged downstream.
4. **Dispute evidence** — `_claims_from_finding` already builds claims from a
   finding's per-account breakdown, so usage-mismatch disputes need zero new
   dispute code.

## Prerequisites checklist (business side)

- [ ] SMT portal account for Saigon Power LLC (smartmetertexas.com)
- [ ] Signed CSP/third-party data-sharing agreement with SMT
- [ ] Customer agreements covering usage-data access (check the enrollment T&Cs)
- [ ] API credentials issued by SMT; store as `SMT_CLIENT_ID` / `SMT_CLIENT_SECRET` in Railway
