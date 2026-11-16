# Report (read-only): Pennsylvania's 04:00 run failed after 61 s: the S3 lock from yesterday's killed run was 2 seconds from expiring. 2026-11-16 04:09 UTC (prod agent)

For the dev agent's awareness; the user was alerted by the CAMS message ("Essential container in task exited"). Nothing was retried, restarted or changed (a retry is not authorized by `9874e21`).

## What happened (observed)
- **Run `pa-5e8e74d5aa86`**, the first PA run on **ddp-sync `df605d7` and image revision 62**, launched 04:00:00 UTC. ddp-sync 04:01:01: `cloud_scrape: collection failed detail='Essential container in task exited' exit_code=90`, `openstates_pipeline: done ... duration_seconds=61.7 failed=1 ... status=failed`, `PipelineFailed: pa: 1 of 1 scrape session(s) failed`. Nothing was scraped, loaded, archived or sent to LegBot.
- **The task's own log (`/aws/ecs/ddp-scrapers`, stream ending `be1754aac39d`) has two lines:** `04:00:37 ERROR: another run of pa already holds the lock -- refusing to start a second one (OPEN-187)` and `{"source": "pa", "run_id": "pa-5e8e74d5aa86", "mode": "full", "status": "failed"}`. Exit 90 is `EXIT_DO_NOT_RETRY`, so nothing retries it. (`mode: full` is what the failure path always writes; it says nothing about the plan.)

## Why (read from S3, then confirmed in the code)
- The lock is `s3://ddp-openstates-scraper-memory/prod/pa/_lock`. **Contents: holder `pa-bfb6773bf3ba` (yesterday's run, the one ddp-sync stopped at 16:01 on the 12 h limit), acquired 2026-11-15 04:00:39 UTC, expires 2026-11-16 04:00:39 UTC.** Its S3 last-modified is 11-15 04:00:40, so it was **never released** (a release rewrites the object as expired). Today's run asked for it at **04:00:37, two seconds before that expiry.**
- `SourceLock` (`cloud_collector.py`): age-based expiry only, `LOCK_TTL_SECONDS = 24 h`; release happens in the collector's `finally`; a task that is **stopped from outside** never gets there, so its lock lives out the full 24 h.
- **The lock has now expired** (read at 04:09:14 UTC: `expires_at` passed), so a PA run started now would reclaim it as normal.

## Something you may want to fix, not just this once
- **The TTL equals the nightly cadence.** Any nightly that ddp-sync stops (the old 12 h limit, or the new OPEN-477 stall stop) leaves a lock that expires about 24 h after the run *started*; the next night's run starts at the same clock time, a few seconds before that expiry, and is refused. So **a stopped nightly blocks the next nightly the same way**, unless the next one happens to start a few seconds later. This is my reading of the code and of this one event; I have not reproduced it.
- Nothing I did caused it: I did not touch PA, the lock or the schedule.

## State
- **PA has loaded nothing since the last completed run**; two runs in a row now have loaded nothing (11-15 killed at 12 h, 11-16 refused at 61 s). The next scheduled PA run is tomorrow 04:00 UTC.
- **Not run:** a retry or manual trigger of PA (not authorized). I can start one if the user says so; the lock is free now.
- Not related: the broker. This is Fargate and ddp-sync only.
