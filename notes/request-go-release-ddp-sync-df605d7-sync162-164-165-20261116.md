# Request (GO): release ddp-sync `df605d7` (main) on EC2. It completes SYNC-162, whose Mac half is live, and carries SYNC-161/164/165/166. 2026-11-16 02:35 UTC (Mac side)

**Ramon, 2026-11-16 ~02:35 UTC: "yes, post the ddp-sync release request".** That is the go for this release on EC2.

**Why:**
* **SYNC-162** (ddp-sync #282) ends the one-hour wait. The Mac now answers a LegBot trigger at once with a run id (202), and **EC2 checks back** every minute, for up to 6 h, until the run ends. It alerts once on a real failure.
* **The Mac half is live:** ddp-sync on the Mac has run `df605d7` since 01:16 UTC (Ramon's kickstart). **The EC2 half is this release.** Until then EC2 runs `ad68c4f` (your `8a65f1e`) against a Mac that answers the new way. The builder released the Mac first on purpose; please report in the nightly note how mi, usa and pa's LegBot triggers behaved tonight.
* SYNC-164 and SYNC-165 change LegBot's work on the Mac. They ride along here so EC2 runs the same code.

## What I need from you

### 0. Gates (stop and report if any fails)
* **G0, window.** Not while a scrape, load, archive or LegBot step that this ddp-sync launched is in progress. Tonight that means:
  * **either before 02:55 UTC** (usa starts 03:00, pa 04:00);
  * **or after Pennsylvania's 04:00 run has finished its LegBot step.** Pennsylvania can run 12 h or more; it is the OPEN-477 stall-check test, so don't interrupt it.

  Also not while one of your `9963ec8` steps 4–8 has a job running that ddp-sync launched (step 7's `refresh-extraction`, if you run it as a Fargate task through ddp-sync). Your call which window; say which.
* **G1, code.** `/opt/ddp-sync` at **`ad68c4f`**, `git status` clean.
* **G2, quiet.** No pipeline, scrape, archive or LegBot line in the last 10 minutes, and 0 running `ddp-scrapers` tasks that ddp-sync is waiting on.
* **G3, rollback point.** Tag the running image `ddp-sync:pre-df605d7` and record its id.

### 1. Pull, build, restart (as for `ad68c4f`)
1. `git -C /opt/ddp-sync merge --ff-only origin/main`. Expected **`df605d7`**.
2. Build on its own first (`docker compose -p ddp-sync -f infrastructure/docker-compose.prod.yml build`) with the live container untouched. If the build fails, stop.
3. `sudo systemctl restart ddp-sync`. Nothing else is recreated.

**What it carries** (`ad68c4f..df605d7`, first-parent):
* **#278 SYNC-161:** the organization-logo step. **Off by default** (`LEGBOT_ORGANIZATION_LOGO_ENABLED` unset; do not set it).
* **#279 SYNC-164:** LegBot keeps Massachusetts' bill → law pair (core's own comparison).
* **#280 SYNC-165:** LegBot lists only real bill types (`bill_types_only`).
* **#281 SYNC-166:** the logo judge pauses when LegBot is busy (logo step only, off).
* **#282 SYNC-162:** the trigger gets a run id and checks back.

**No `.env`, manifest, IAM or schedule change.**

### 2. Checks right after (report each)
* healthy, with a new image id;
* `Redis connected`;
* the 13 per-state scrape jobs registered;
* 0 `Traceback` since the restart;
* `config/sync_schedule.yaml` still shows `stall_seconds: 2700`.

### 3. The first LegBot trigger after the release (in your next report)
For the first state whose archive triggers LegBot after the restart, report:
* the trigger's answer (202 with a run id);
* EC2's check-back lines;
* the run's end, and that the steps after it ran (search refresh and embedding, for states enrolled in them).
* **There must be no `trigger_failed` at 3,600 s.**

**Rollback:** `git -C /opt/ddp-sync checkout --detach ad68c4f`, re-tag `ddp-sync:pre-df605d7` as `ddp-sync:prod`, then `sudo systemctl restart ddp-sync`.

## Not authorized
* Setting `LEGBOT_ORGANIZATION_LOGO_ENABLED` or any `.env`.
* Any schedule or manifest change.
* Triggering any scrape, especially LA or NE (`9963ec8`'s hazard stands).
* The broker, api-v3 (that is step 8 of `9963ec8`), the Mac.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
