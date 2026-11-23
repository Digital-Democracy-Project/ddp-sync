# REQUEST (waits on Ramon's go): release ddp-sync `85c8888` on EC2 (SYNC-187: cloud scrapes get their own 32-slot pool, so a burst no longer queues silently behind 8 threads). Best done when nothing is in flight. 2026-11-23 (Mac side, Onboard New States)

**Not a GO.** The EC2 release is yours after a GO note or Ramon's go in your session. The Mac's ddp-sync doesn't run cloud scrapes; its update is Ramon's own step, any time.

## Why
Your finding `c35a38c`:
* Cloud scrapes wait in `asyncio.to_thread`, in Python's default pool: 4 CPUs, so 8 threads.
* A ninth run waits with its lock held and no task or log line. Alerts share that pool.

SYNC-187 (ddp-sync #305, head `d4433be2`, reviewed and merged by the PM session as **`85c8888`**):
* Cloud scrapes, scheduled, manual and retried, and runs resumed after a restart, now wait on their own pool of **32** (`CLOUD_SCRAPE_SLOTS`; the PM is confirming the number with Ramon).
* A waiting run logs `cloud_scrape_waiting_for_slot` (state, place in queue, the states holding slots), then `cloud_scrape_slot_acquired` with its wait.
* Alerts keep the default pool.

## What changes
* **Code:** `b569cf2..85c8888` is exactly #305: `pipelines/cloud_scrape_trigger.py`, `pipelines/openstates_scrape.py`, `primitives.md` and a new test file.
* **No** `.env`, yaml, IAM, Dockerfile, compose, requirements or manifest change.

## When (the one real choice)
**Best: when no cloud scrape, load, archive or LegBot step is in flight.** Then the restart costs nothing.
* The current wave (mt, ia, md, hi, me, ms, mo, nm, id, ri, ok, az, fl, ut, la and wy, as running or queued) will drain over the next hours.
* Florida's 2025/2026 re-import can hold a slot for about a day. If waiting for Florida too means waiting past tonight's nightlies, release while only Florida is in flight, and accept its cost (below).

**Never** from 02:15 to 04:45 UTC: the WA 02:30, USA 03:00 and PA 04:00 nightlies start then.

## Gates (stop and report if one fails)
* **G1, code:** `/opt/ddp-sync` at `b569cf2` (your `38e02fc`), `git status` clean. `git log --first-parent --oneline HEAD..origin/main` lists only #305.
* **G2, what's in flight:**
    * list every state with a running Fargate task, a held `ddp:pipeline_lock:<st>`, or `openstates_pipeline: starting` and no task (queued);
    * list each one's step (collecting, loading, archive, LegBot);
    * **post that list as a checkpoint before restarting.**
* **G3, rollback point:** tag the running image `ddp-sync:pre-85c8888` and record its id.

## Release (as for `b569cf2`)
1. `git -C /opt/ddp-sync merge --ff-only origin/main`. Expect **`85c8888`**.
2. Build on its own first, with the same `DDP_CORE_SHA=939e4a2c…` as now (the core doesn't change). If the build fails, stop.
3. `up -d --no-deps` (as on 11-23), nothing else recreated.

## Checks right after (report each)
* Healthy, with a new image id; `Redis connected`; 0 `Traceback`; 0 `legbot_replica_list_unreadable`.
* `GET /schedule` shows the same jobs as before.
* `check-core-pin-drift.py --image-core 939e4a2c… --sync-container ddp-sync-ddp-sync-1` exits 0.
* **The new behaviour:** the next time more than 8 cloud scrapes are wanted at once, there is no `cloud_scrape_waiting_for_slot` line until 32 are in use. Report the highest number running at once over the next day.

## If anything was in flight: what to re-trigger, and what a restart cuts
This follows your own reading in `3cef331`/`38e02fc`: collection and load resume from Redis; the archive, hand-offs and later sessions of a multi-session run don't. **All of these are pre-approved repairs only if Ramon says so with the release go.**
* **A run collecting or loading:** it resumes. When its load ends, trigger its **archive** by hand (as you did for pa and mi on 11-23).
* **An archive in progress:** it is cut. Re-trigger that state's archive.
* **A multi-session run past its first session** (ok `2025,2026`; ia `2025,2026,2026s1`; fl `2024,2025,2026`): the sessions not yet started aren't launched. Re-trigger them with `?sessions=` (use `full=true` only where the original was a full re-import: ok, fl).
* **A run that was queued with no task (today: hi, me, ms, mo, nm, id, ri, ok, if still waiting):** it is dropped. Re-trigger it exactly as in its own request:
    * a plain first scrape for hi, me, ms, mo;
    * `full=true&sessions=…` for nm, id, ri and ok.

    **If its `ddp:pipeline_lock:<st>` is still held** with no task, report the key and its TTL. **Don't delete it**; we'll decide.
* **LegBot hand-offs** of runs cut mid-way: none needed for the states with LegBot off (mt, ia, md, hi, me, ms, mo, id, ri, ok, wy). For a LegBot-on state (pa, nm, la, ne, ut, az, fl…), report which hand-off was cut.

## Rollback
`git -C /opt/ddp-sync checkout --detach b569cf2`, re-tag `ddp-sync:pre-85c8888` as the running image, then `up -d --no-deps` again.

## Not authorized
* Any yaml, `.env`, schedule or manifest change.
* Deleting any Redis key.
* Any scrape or re-trigger beyond the repairs above (and those only with Ramon's go).
* The Mac's ddp-sync (Ramon's step).

Tickets: SYNC-187 (onboarding wave, OPEN-146).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
