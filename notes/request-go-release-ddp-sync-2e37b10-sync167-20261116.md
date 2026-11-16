# Request (GO): release ddp-sync `2e37b10` (main) on EC2. It carries SYNC-167: a scrape or archive that ddp-sync stops now releases its own S3 lock. 2026-11-16 (Mac side)

**Ramon, 2026-11-16 (pop-up): "Yes, release it".** That is the go for this release on EC2.

**Why:** on 11-15 ddp-sync stopped Pennsylvania's nightly at its limit. The run's 24 h lock (`prod/pa/_lock`) stayed, so the 11-16 04:00 run was refused (your `921cfc1`). With the stall check live, any stopped run can block that state's next night. #286 releases the lock when ddp-sync stops the task, but only if all three of these hold:
* the stop request succeeded;
* ECS reports the task `STOPPED` (polled every 5 s for up to 150 s);
* the lock's holder is that run. The write is conditional, so a newer run's lock is never touched.

Otherwise the lock is left to its 24 h expiry, as today. The failure alert now says what happened to the lock.

## What I need from you

### 0. Gates (stop and report if any fails)
* **G0, window:** not while a scrape, load, archive or LegBot step launched by this ddp-sync is running. That includes the v61 data steps in the coming `State Specific Handling` GO note: Alabama's manual run and section B. Not within 15 minutes of a nightly start (wa 02:30, usa 03:00, pa 04:00 UTC). Your call which gap; say which.
* **G1, code:** `/opt/ddp-sync` at **`df605d7`** (your `e3de068`), `git status` clean.
* **G2, quiet:** no pipeline, scrape, archive or LegBot line in the last 10 minutes, and 0 running `ddp-scrapers` tasks that ddp-sync is waiting on.
* **G3, rollback point:** tag the running image `ddp-sync:pre-2e37b10` and record its id.

### 1. Pull, build, restart (as for `df605d7`)
1. Run `git -C /opt/ddp-sync merge --ff-only origin/main`. Expected **`2e37b10`**.
2. Build on its own first (`docker compose -p ddp-sync -f infrastructure/docker-compose.prod.yml build`), with the live container untouched. If the build fails, stop.
3. Run `sudo systemctl restart ddp-sync`. Nothing else is recreated.

**What it carries** (`df605d7..2e37b10`, first-parent):
* **#283 SYNC-163:** SVG organization logos. The logo step stays **off** (`LEGBOT_ORGANIZATION_LOGO_ENABLED` unset; don't set it).
* **#284 OPEN-537:** a docs pointer only.
* **#285 SYNC-161:** a stricter logo source-URL check (logo step, off).
* **#286 SYNC-167:** the lock release above. **No migration, no new setting, no `.env`, manifest, IAM or schedule change.**

### 2. Checks right after (report each)
* healthy, with a new image id;
* `Redis connected`;
* the 13 per-state scrape jobs registered;
* 0 `Traceback` since the restart;
* `config/sync_schedule.yaml` still shows `stall_seconds: 2700`.

### 3. Read-only, in your next nightly report
* **Not verified on the Mac:** whether the EC2 role may write `ddp-openstates-scraper-memory/prod/<state>/_lock` itself. The same host already writes the loader's lock in that bucket. If not, the alert will read `lock not released (AccessDenied)`, which is no worse than today. Please say whether the role allows `s3:PutObject` on that prefix. Read the policy only; change nothing.
* The first time ddp-sync stops a task after this release, report the alert's lock line, and that the state's next run started normally (SYNC-167 acceptance 3).

**Rollback:** `git -C /opt/ddp-sync checkout --detach df605d7`, re-tag `ddp-sync:pre-2e37b10` as `ddp-sync:prod`, then `sudo systemctl restart ddp-sync`.

## Not authorized
* Setting `LEGBOT_ORGANIZATION_LOGO_ENABLED` or any `.env` change.
* Any schedule, manifest or IAM change.
* Triggering any scrape.
* The broker, api-v3 or the Mac.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
