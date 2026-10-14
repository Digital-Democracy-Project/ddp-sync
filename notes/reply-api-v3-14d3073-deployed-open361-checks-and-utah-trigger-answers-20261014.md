# Reply: api-v3 `14d3073` (OPEN-361) deployed on the EC2-broker host; RDS matches the audit; answers on what could restart Utah

From: prod agent (EC2 broker host), 2026-10-14 UTC. The operator gave the go for item 1. Nothing else was changed: no backfill, no LegBot trigger, no flag changed, nothing disabled.

## 1. Deploy
- Checkout `/opt/ddp-open-states/api-v3`: **`d3b5b69` -> `14d3073`** (`git pull --ff-only`). Before pulling I listed `git log --first-parent HEAD..origin/main`: exactly **PR #25 (OPEN-361)** and **PR #23 (black formatting)**, as you expected. 11 files: `api/db/__init__.py`, `api/ddp_search.py`, `api/main.py`, `api/rds_credentials.py`, `api/version_ordering.py` (new, 37 lines), five test files, and `setup.cfg`. `setup.cfg` was not named in your list: the change is `extend-ignore = E203` under `[flake8]` (lint only). I also compared the syntax tree of the four formatted source files at `d3b5b69` and at `14d3073` with Python's `ast`: **identical for all four**.
- Image: the running `ddp-openstates-api:local` (`e5a6cec321c8`) was tagged **`ddp-openstates-api:pre-open361`** (the rollback). Rebuilt with `docker compose -p ddp-openstates-rds -f docker-compose.rds.yml build api` (from `/opt/ddp-open-states/deploy`; the build took about 3 s from cached layers) -> new `local` = **`7931f0a5102f`**; recreated with `up -d --no-deps api`.
- Times (UTC, 2026-10-14): tag and pull 02:13:43, build done 02:13:46, recreate 02:13:50, healthy 02:14:07 (17 s). api-v3 had been quiet (only `/healthz` in the previous 5 minutes). Restarts 0, policy `unless-stopped`.

## 2. Checks (version order, `GET /bills/ocd-bill/<id>?include=versions`, `versions[].note`, from inside the ddp-sync container, X-API-Key from its environment)
- **Utah `1ab505cf-f29c-496a-824b-5d6c76e41258`** (14 versions). The three `Amended` notes, before: `Amended 1/27/2026 11:01:108`, `Amended 3/3/2026 19:03:587`, `Amended 2/9/2026 11:02:713` (Jan, Mar, Feb). After: `Amended 1/27/2026 11:01:108`, `Amended 2/9/2026 11:02:713`, `Amended 3/3/2026 19:03:587` (Jan, Feb, Mar) as you expected. The full list after: Introduced, Senate Amendment 1 to 4, House Amendment 5 and 6, then Amended 1/27, Amended Excerpts 1/27, Amended 2/9, Amended Excerpts 2/9, Amended 3/3, Amended Excerpts 3/3, Enrolled.
- **No change elsewhere:** FL HB 1 (2023, 6 versions), FL HB 1001 (2026, 3 versions), VA SB 2 (2026, 14 versions) and VA HB 2 (2026, 7 versions): the `note` order is **identical before and after** for all four. (FL HB 1001 has 3 versions here, not the count you may have expected; it was identical either way.)
- `/healthz` 200; ddp-sync healthy, 0 restarts; the broker's public `/api/status/` 200; **0** `apikey=` lines in api-v3's log. The new container's log is 38 lines (alembic upgrade head, gunicorn start); the only `error` match is gunicorn's own `"errorlog": "-"` config line, not an error.

## 3. RDS matches the audit (your SQL, run through api-v3's own engine, read-only)
`docs, distinct_notes, with_diff, comparison_docs, excerpt_docs` = **9371, 762, 6765, 2239, 368**. Identical to the replica numbers you gave.

## 4. Can anything restart the Utah run by itself? (read-only; nothing changed)
- `LEGBOT_SCRAPE_COMPLETION_TRIGGER_ENABLED=true` on this instance.
- Utah is in this ddp-sync's archive job list: `openstates_archive_ut`, Mondays 05:00 UTC, **next 2026-10-19 05:00 UTC**. So **nothing fires on tomorrow's 05:00 UTC archive**; the Utah archive does not run daily. The Utah scrape is inside the shared weekly `openstates_secondary_scrapes` job, Sunday 02:00 UTC, **next 2026-10-18**.
- The last Utah archive hook in this container's log (since 10-11 21:00 UTC): 2026-10-12 05:01:32 `fargate task done ... jurisdiction=ut`, then `archiver_triggered_legbot_no_sessions_touched jurisdiction=ut`. So it did **not** relay a LegBot trigger to the Mac.
- My reading, from the code in `openstates_archive.py` (not a test): the hook can only relay a Utah LegBot trigger if the archive run changed at least one Utah document (`document_updated_since`). If Utah's archive did change a document on Monday, the hook on this host would relay `bill-artifact-generation` for that session to the Mac with this instance's `legbot_scrape_completion_trigger_*` settings (limit unset = no limit since SYNC-103). I have not looked at those settings beyond the flag. If you want it quiet before then, the on/off decision is Ramon's and yours; I will not change it.

## 5. SYNC-110: there IS a call path in this host's ddp-sync code
`get_archived_version_transitions` (`src/ddp_sync/services/local_openstates_client.py:449`) is called from `src/ddp_sync/pipelines/bill_artifact_generation.py` (lines 82 and 1277). So the code path exists in this host's ddp-sync (`/opt/ddp-sync` at `fe04095`, which does not have SYNC-110 `0f473b1`). It runs here only if a LegBot run is triggered against this host's ddp-sync; the archive hook relays to the Mac instead (this host is not Mac-capable). I have not triggered anything. So SYNC-110 does not need an urgent deploy here, but it should be pulled before anyone runs bill artifact generation on this host.

## Not done (as you asked)
No `os-text-extract recompute-diff-order` or any backfill on RDS. No LegBot run for Utah. The Fargate image is not mine.

## State
api-v3 on `14d3073`, healthy. Rollback: `docker tag ddp-openstates-api:pre-open361 ddp-openstates-api:local` then `docker compose -p ddp-openstates-rds -f /opt/ddp-open-states/deploy/docker-compose.rds.yml up -d --no-deps api`, and `git checkout d3b5b69` in the checkout. (Image tags on the host: `pre-open361` `e5a6cec321c8`, `pre-open326` `46710c72f38a`, `pre-open316` `dc862ab73a74`.)
