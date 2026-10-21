# CORRECTION to my report on the saved `RDS_DATABASE_URL` (`report-saved-rds-database-url-in-ddp-sync-env-stopped-authenticating-20261021.md`): this is the expected 7-day rotation, not a problem

The operator pointed out that nothing uses that saved credential any more (everything moved to Secrets Manager, with a 7-day rotation), and asked me to correct them if wrong. I checked the code: **they are right, and my report overstated it.**

* **Every ddp-sync consumer resolves the credential live.** `scrape`, `archive`, `backfill`, `vote_person_backfill`, `cloud_scrape_trigger` (the loader call) and `replica_freshness` all call `rds_credentials.resolve_rds_database_url()` at the moment of use (OPEN-260). The module's docstring states that RDS rotates the managed credential **every 7 days** (rotation enabled, 7-day schedule, confirmed in the console), and records the 2026-09-09 incident that was the same failure on the old cached value.
* **The saved line is only a render-time snapshot:** `infrastructure/render-env.sh` still writes `RDS_DATABASE_URL` into `.env`; nothing re-renders it after a rotation, so it goes stale about weekly by design and no scheduled path reads it.
* **What did read it:** only my own read-only helper scripts on this host (and one comment in `quality_check.py`). My 03:10 to 03:35 UTC failure today fits a rotation; I still cannot date it (no `secretsmanager:DescribeSecret` for this role), so the timing is consistent with, not proof of, a rotation.
* **So:** nothing to fix, no owner needed, no impact on loads. Please ignore the three options in my report (refresh the line, remove it, or grant `DescribeSecret`); the only optional cleanup would be to stop `render-env.sh` writing a line nobody should read. From now on I will resolve the live secret for any read-only database check and not use the saved line.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
