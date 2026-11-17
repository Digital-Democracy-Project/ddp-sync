# UPDATE to `request-release-ddp-sync-629f992-sync158-sync172-20261117.md`: the target is now ddp-sync main **`cb5eade`** (adds SYNC-156). Still waits on Ramon's go. 2026-11-17 (Mac side, Onboard New States session)

This changes only the target commit and adds one line to each half. Everything else in the request stands as written, **including Half 2's Saturday 11-21 12:05 UTC to Sunday 11-22 01:55 UTC window (gate G0)**.

## What's new on main since `629f992`
| Merge | Ticket | What changes | Where it acts |
|---|---|---|---|
| #293 `cb5eade` (head `b02e3be`, reviewed and merged by the PM session) | SYNC-156 | ddp-sync's daily cadence review (00:30 UTC, plus once about 5 minutes after start) now compares the manifest's scraped states with the `openstates_scrape_<state>` jobs. It re-runs the existing registration when they differ, so a resumed state gets its job and a paused one loses it **without a restart**. Existing jobs keep their ids and triggers. | **EC2's scheduler**. Inert on the Mac, whose scrape jobs stay off. |

Files changed `629f992..cb5eade`: `src/ddp_sync/scheduler.py`, `tests/test_scheduler_cadence_actuation.py` and `primitives.md`. Still no `.env`, IAM, manifest, Dockerfile, compose, requirements or migration change. `config/sync_schedule.yaml` is not touched by #293.

## Changes to the request
* **Half 1 (Mac), step 1:** expected `cb5eade` (not `629f992`). Rollback is unchanged (`5d2592d`).
* **Half 2 (EC2), step 1:** `merge --ff-only origin/main`, expected **`cb5eade`**. The rollback tag becomes `ddp-sync:pre-cb5eade`, with the same steps.
* **Half 2, step 2 checks, one more:** about 5 minutes after the restart, the first cadence review logs its result. Report whether it shows `jobs_synced` and what it lists. Expected: nothing added or removed, because the jobs registered at startup already match the manifest's 16 jurisdictions. The `GET /schedule` table in step 2 is unchanged.
* **Gate G1 is unchanged:** `/opt/ddp-sync` at `ce459b4`, clean.

## Dependency to know
SYNC-156 runs inside the cadence review, which exists only while `dynamic_cadence.enabled: true` in `config/sync_schedule.yaml` (true today). If that is ever switched off as a rollback, the restart gap comes back.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
