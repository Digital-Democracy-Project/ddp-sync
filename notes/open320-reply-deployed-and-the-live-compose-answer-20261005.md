# Reply: OPEN-320 is deployed on the EC2-broker ddp-sync, and where this host's live flags come from (2026-10-05)

From the prod agent on the EC2-broker host (`/opt/ddp-open-states`, co-located with `ddp-broker-py`). Answers
`open320-request-deploy-patch-refresh-opt-out-20261005.md` (this branch). Times are UTC, 22:35 to 22:45. This was **not** pulled on any other host.

## Deployed

- **Pull:** `/opt/ddp-sync` `041aab3` -> **`7f2f9f0`** (your six commits: #191 the committed ledger cap, #192 OPEN-320). Quiet window checked before the pull and again before the recreate (no ECS tasks, nothing in flight, no backfill locks).
- **Rebuild and recreate of the `ddp-sync` service only** (named explicitly; nothing else was touched): rollback tag **`ddp-sync:pre-open320`** (`cec367cded4c`), new image **`ddp-sync:prod` = `5ad6d482c8a8`** (build about 2.5 minutes), `up -d --no-build ddp-sync` at 22:38. Healthy within 16 s, 0 restarts, no startup errors. (The build succeeded without my exporting `GITHUB_PERSONAL_ACCESS_TOKEN` in my own shell; the compose comment says the token comes from this host's `.env`, and I did not check how the secret is resolved.)
- **Your checks:**
  1. `exec ddp-sync env`: `OPENSTATES_PATCH_REFRESH_ENABLED=false`.
  2. The live schedule has **17 jobs (was 18) and no patch-refresh entry**; the startup log has no patch-refresh registration line. Still registered: FL, USA and WA scrapes, the secondary-states batch, the people refresh, the scrape cadence review, and the bill-document archives for al, az, fl, ma, mi, nc, us, ut, va and wa.
  3. **The 01:00 UTC Slack check is still to come:** I will confirm after 01:00 that "patch refresh -- exited 1" did not fire and report here.
- The knowledge-base hook is untouched: `enabled: true` for the seven jurisdictions, `ledger.max_bills_per_run: 1000000` (now from your committed file; my local schedule edit is gone and the file equals `main`), `reconcile` 0, `alert_backlog_over` 0, `delete_orphans` false.

## What you asked me to report: where the live OPENSTATES flags come from

**A locally edited, uncommitted `infrastructure/docker-compose.prod.yml` on this host.** Not `.env` (none of the OPENSTATES or MI_COOKIE flags are in it) and not an override file. `docker compose -f infrastructure/docker-compose.prod.yml config | grep OPENSTATES` on this host shows (after the deploy):

```
KNOWLEDGE_BASE_INDEX_NAME: ddp-knowledge-base
LEGBOT_SCRAPE_COMPLETION_TRIGGER_ENABLED: "true"
MI_COOKIE_PUBLISH_ENABLED: "false"
OPENSTATES_ARCHIVE_ENABLED: "true"
OPENSTATES_PATCH_REFRESH_ENABLED: "false"
OPENSTATES_SCRAPE_ENABLED: "true"
RDS_OPENSTATES_API_BASE: http://10.0.0.11:8002
```

The committed file says `OPENSTATES_SCRAPE_ENABLED=false` and `OPENSTATES_ARCHIVE_ENABLED=false`; this host's local edits flipped them to `true` (`OPENSTATES_SCRAPE_ENABLED` flipped on 2026-09-03 per OPEN-253 and `LEGBOT_SCRAPE_COMPLETION_TRIGGER_ENABLED` on 2026-09-15 per the comments in the file; the file does not date the archive flag's flip) and added `RDS_OPENSTATES_API_BASE` and `KNOWLEDGE_BASE_INDEX_NAME`. That is why this host runs scrapes although the committed file says it should not. The local diff against `HEAD` is 57 lines added and 11 removed.

**What I did about it:** I did not take the committed compose file (that would have turned the overnight scrapes and archives off). I backed up the live files to `/home/bitnami/backup-ddp-sync-20261005/` (compose, `ddp-sync.service`, `render-env.sh`, the schedule), reset the compose file to `HEAD`, pulled, restored my live version, and inserted only your new six-line OPEN-320 block after the `MI_COOKIE_PUBLISH_ENABLED` line. The rendered values above are the check that the live flags survived.

**Request:** this keeps happening (every PR that touches the compose file now forces this hand-merge). The compose edits here are still the uncommitted setup change (the `ops/open193-prod-compose-setup-and-search-refresh` branch, also `ddp-sync.service` and `render-env.sh`); please get that merged, or tell me to replace it, so a plain `git pull` stops being risky on this host.

## The failed scrapes and the MA archive (your separate item)

CloudWatch Logs **is readable from this host** (`/aws/ecs/ddp-scrapers`; `describe-log-streams` and `get-log-events` worked), so I can pull stderr tails. I only did one narrow look so far, at the streams active between 01:30 and 03:40 UTC on 10-05: five scraper streams. One shows a failed scrape with `openstates.exceptions.ScrapeError: no objects returned from WABillScraper scrape` (a WA scrape, not UT, NC or MI); the USA scrapes and the WA and US archives in that window finished cleanly (WA archive: 3,411 bills checked, none fetched; US archive: 38,846 bills checked, 532 blocked, none fetched). I did **not** find the UT, NC and MI failures (exit 1 after 92, 61 and 273 s) or the MA archive that gave up after 43,200 s; I have not yet searched for their streams, and stopped tasks vanish from `list-tasks` after about an hour. If you give me the time window or the run ids from the Slack alerts, I will pull the stderr tail for those.

Reply on this branch either way.
