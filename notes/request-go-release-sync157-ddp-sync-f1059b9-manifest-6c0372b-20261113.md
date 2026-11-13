# Request (GO): release SYNC-157 on the EC2 host: manifest to ddp-open-states `6c0372b`, then ddp-sync `f1059b9` (SYNC-157, OPEN-478/479/480, OPEN-523, OPEN-68), 2026-11-13 about 03:30 UTC (Mac side)

**Ramon's go, 2026-11-13 (/onboard-state pop-up, his choice): "Release now".** It covers Parts A to C below on the EC2 host. Part E (removing the old settings) is **not** covered and holds for its own go.

## What I need from you
1. **Part A:** the manifest rollout to ddp-open-states `main` `6c0372b` (dry run first).
2. **Part B:** ddp-sync to `main` `f1059b9`, recreate only the ddp-sync container.
3. **Part C:** the read-only checks, including the in-container drift findings you ran on 11-12.
4. **Part D (read-only):** one question SYNC-157 left open (below).
5. Reply: `reply-release-sync157-20261113.md`. **Part E holds.**

## Why
Your 11-12 finding (`2c175b9`): the scheduled drift check flags ddp-sync's **own** `DDP_OPENSTATES_JURISDICTIONS` (it has `ma`, lacks `la ne pa`). SYNC-157 removes ddp-sync's own state lists and its public-OpenStates route: ddp-sync then reads only the manifest and only our api-v3. It is also what Alabama's and Massachusetts's switch-on wait on.

## The code
* **ddp-open-states `main` `6c0372b`**: brings #361 (`e63ae81`, manifest gains `actions.newest_first` and `voatz.test_precinct`), #362 (`6041fc4`, ddp-sync's per-state behaviour lists become manifest fields), #363 (`6c0372b`, the validator rejects `voatz.creator_id`; no creator id in the manifest). Report the host checkout's revision before.
* **ddp-sync `main` `f1059b9`** (#272): from `cb6f947` (your 11-10 deploy). No migration. **New behaviour to expect:** with the old env names still set, ddp-sync logs **one** start-up warning ("SYNC-157: these settings no longer exist ..."). That is deliberate, not a failure. The scheduler **refuses to start** if the host's `config/sync_schedule.yaml` still carries a deleted list: G1 below guards that.

## Gates (report each value)
* **G0, timing:** start only **after Pennsylvania's 04:00 UTC nightly of 11-13 has finished** (it ran about 7 h 30 min on 11-12), at least an hour clear of 02:00, 02:30, 03:00 and 04:00 UTC, and **finish before Saturday 11-14 23:00 UTC** (Sunday's batch is 02:00 UTC). If you can't fit it in that window, stop and tell me.
* **G1, ddp-sync:** `git -C /opt/ddp-sync status` and `git diff config/sync_schedule.yaml` show **no local edits**; branch and HEAD (`cb6f947` expected).
* **G2, quiet:** no scrape, archive, people load or LegBot relay in flight; no ddp-sync job in the last 15 minutes. The recreate clears every pipeline lock, by design.
* **G3, rollback point:** both checkouts' revisions and the ddp-sync container/image you replace.

## Part A: the manifest (dry run first)
1. `rollout-manifest.sh` **dry run** to `6c0372b`. Paste its output. **Stop** if the validator rejects the manifest, or if the `jurisdictions.yaml` diff touches any `status`, `replica_routing`, `legbot_replica`, `cloud_path` or `archive.enabled` value (I expect only new fields: `actions`, `voatz.test_precinct`, `scrape.*` / `archive.timeout_s` and the behaviour fields of #362).
2. Run it for real. Include `deploy/sync-manifest.sh` and the api-v3 `api` service recreate **by name** if the script does them as on 11-10. **Never an unscoped recreate.**
3. Check `/ddp/enrollment` answers 200 and still lists `replica_routing` AZ FL LA MI NC NE PA US UT VA WA (no MA, no AL).

## Part B: ddp-sync
1. `git pull --ff-only origin main` (expect `f1059b9`).
2. Recreate **only the ddp-sync container**, by the host's usual procedure.

## Part C: checks (read-only; start no run)
1. `/health` healthy, Redis connected, scheduler running; startup log: only the one expected SYNC-157 warning, no error, no "refuse".
2. `GET /schedule`: exactly one `openstates_scrape_<state>` for each of **al az fl la ma mi nc ne pa ut va wa usa** (13), with the same next-run times as before (WA 02:30, USA 03:00, PA 04:00 nightly; the rest Sunday 02:00).
3. Run `collect_findings(sync_config, settings, root)` in the container as you did on 11-12 and paste it. **I expect `"drift": []`** (the env-list comparison is gone).
4. `POST /ddp-sync/v1/trigger/openstates-scrape/zz` answers **404** and its list has the 13 states. Start nothing real.
5. After the next nightly (WA, USA or PA, whichever comes first), read-only: the log shows the state reading our RDS api-v3 base, not the public host.

## Part D (read-only)
For each of `bill_sync`, `legislator_sync` and `legislator_bio_sync` on this host: did it run for `pa`, `ne` or `la` between 11-08 and the deploy; which OpenStates base did it call (log lines); did it write anything (counts only)? The compose file sets them disabled, so I expect "did not run".

## Part E: HOLDS (not authorized now)
Removing `OPENSTATES_API_KEY`, `OPENSTATES_API_BASE`, `DDP_OPENSTATES_JURISDICTIONS` and `LEGBOT_RDS_REPLICA_JURISDICTION_ALLOWLIST` from the host's env and secret. **Removing them before Part B would send every state to the public API.** A separate go note covers it.

## Not authorized
Part E; any scrape, archive, LegBot or people run; any broker, task-definition or image change; any env or yaml edit; any container other than ddp-sync and (Part A) api-v3 `api`; the Mac (Ramon does the Mac side himself); `status` or routing changes.

## What I check on the Mac side
The manifest at `6c0372b` validates and the drift script agrees on `main`; after your reply, that Sunday's batch runs on the new ddp-sync (your cycle report, `3b83a4f`).
