# Request (GO): release ddp-sync `ad68c4f` on EC2 tonight, before Pennsylvania's 04:00 UTC nightly (OPEN-477 stall check; carries SYNC-160)

**Ramon, 2026-11-15 ~21:00 UTC (/ask-me), asked what to do before Pennsylvania's next nightly: "Release the stall check tonight".** That is the go for this release on EC2.

**Why:** Pennsylvania's nightly `pa-bfb6773bf3ba` failed on ddp-sync's 12 h limit at 16:00 UTC today while still saving bills (your `abcdf7a`), so nothing was loaded. OPEN-477 replaces that limit for **scrapes** with a stall check. A scrape is stopped only when its own CloudWatch log stream (`/aws/ecs/ddp-scrapers`, `scraper/scraper/<task id>`) has had no new line for 45 min (`stall_seconds: 2700`). Past `max_wait_seconds` (12 h, or the manifest's `scrape.fargate_max_wait_s`, 24 h for MA) a run that is still logging gets **one "still running" alert and is not stopped**. Archive and backfill waits keep their hard stop. PR ddp-sync #277, reviewed and merged 2026-11-15 as `ad68c4f`: the 46 stall-check tests pass, and the full suite has 2,292 passed with 1 failure that is already on `main` (`test_retry_failed_is_passed_through`).

## What I need from you

### 0. Gates (stop and report if any fails)
- **G0, time:** finish by **02:00 UTC Monday 11-16**. The nightlies are wa 02:30, usa 03:00 and pa 04:00; there is no weekly batch on Monday. If you can't finish by then, **hold** and post a note.
- **G1, code:** `/opt/ddp-sync` at **`a04b40b`** (your 11-13 OPEN-515 checkpoint `2936fea`), `git status` clean. If it is at anything else, stop and report.
- **G2, quiet:** no scrape, import, archive or LegBot line in the last 10 minutes, and 0 running `ddp-scrapers` tasks.
- **G3, rollback point:** tag the running ddp-sync image `ddp-sync:pre-open477` and record its id.

### 1. Pull, build, restart (as on 11-13)
1. `git -C /opt/ddp-sync merge --ff-only origin/main`, expected **`ad68c4f`**.
2. `docker compose -p ddp-sync -f infrastructure/docker-compose.prod.yml build` **on its own first**, with the live container untouched. If it fails, stop: leave the running container alone and report.
3. `sudo systemctl restart ddp-sync`. Nothing else is recreated: not api-v3, Redis, the broker or the Postgres containers.

**What the release carries** (`a04b40b..ad68c4f`, first-parent):
- **#277 OPEN-477:** the stall check. Files: `cloud_scrape_trigger.py`, `openstates_backfill.py` (shared stream-name helper), `openstates_scrape.py`, and `config/sync_schedule.yaml` (`stall_seconds: 2700`; `max_wait_seconds` becomes the alert time for scrapes).
- **#276 SYNC-160:** Redis is retried every 30 s if it wasn't up at start-up, with one alert after 5 min; pipeline locks are cleared when it connects (`redis_store.py`, `app.py`).
- **#274 SYNC-150:** the person-resolver step. **Off by default** (`LEGBOT_PERSON_RESOLUTION_MODE` unset means `off`; do not set it).
- **#275 BROKER-497:** test fixtures and the person-gate script only.

**No `.env`, manifest, IAM or task-definition change.** Reading the task's log stream uses the same `logs:GetLogEvents` this host already uses in `_fetch_task_output` (ops-handoff `step3-validation-failed-rds-security-group-gap-20260909`).

### 2. Checks right after (report each)
- The container is healthy, and the image id is new.
- The log since the restart has `Redis connected` (SYNC-160) and no `Redis unavailable`.
- The scheduler has re-registered the per-state scrape jobs (13, as after SYNC-157), with the next run times for wa, usa and pa.
- No `Traceback` since the restart.

### 3. Checks after Monday's nightlies (one note, by about 16:30 UTC Monday or when pa ends)
- For **wa 02:30, usa 03:00 and pa 04:00:** start, end, status, and the scrape's `found`. For every scrape, give any `cloud_scrape: stall check could not read the task log` warning (count, first line). That line means the stall check is blind for that run.
- **Pennsylvania specifically:**
  - Did it finish? If it passed 12 h, quote the `:hourglass: OpenStates scrape still running` alert (time) and confirm it was **not** stopped.
  - If it **was** stopped, quote the `stalled: no new log line for Ns` detail and the last 5 log lines before the stop.
  - Then the load, the archive and the LegBot trigger answer, as in `abcdf7a`.
- Any alert from `openstates_scrape.stall_check` or `openstates_scrape.long_run`: give the time and the text.

**Rollback:** `git -C /opt/ddp-sync checkout --detach a04b40b`, re-tag `ddp-sync:pre-open477` as `ddp-sync:prod`, then `sudo systemctl restart ddp-sync`. Do it if the build fails, the container isn't healthy, or the scheduler doesn't come back. A stall stop or a long-run alert on Monday is the new behaviour, not a reason to roll back: report it.

## Not authorized
- Retrying or starting any scrape (including Pennsylvania's failed run), archive, people load or LegBot run.
- Any `.env`, `sync_schedule.yaml` hand edit, manifest or task-definition change. Setting `LEGBOT_PERSON_RESOLUTION_MODE`.
- The broker, api-v3, Redis or Postgres containers; Secrets Manager.
- The Mac. Its ddp-sync is mine to update; it runs no cloud scrapes.

## What I check on the Mac side afterwards
On the Mac replica: Pennsylvania's bills updated after the nightly loads, against the 5,286 it holds now. Then OPEN-477 moves to Done once your Monday note shows a scrape ran through the new wait (finished, alerted or stopped as designed).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
