# REQUEST (waits on Ramon's go): release ddp-sync `b569cf2` (SYNC-186: a state with LegBot off is never handed to LegBot). First a read-only check of the state list on both hosts, then the Mac, then EC2. 2026-11-22 (Mac side, Onboard New States session)

**Not a GO yet.** Ramon says go per half. Part 1 is read-only and can be done now. The Mac half (part 2) is Ramon's own step, because the Mac checkout is production and no agent session edits it. The EC2 half (part 3) is the prod agent's, after a GO note.

## What I need from you
1. **Now (read-only):** the check in part 1 on EC2, with the output lines.
2. **On a GO note:** part 3, EC2, with its gates and checks.

## Why
Your note `dcbd313` (11-22 21:50 UTC): after the ND and KY archives, the archiver handed both states to the Mac's LegBot, although `legbot_replica` is false for both. Only the per-bill check stopped them, and that check ran only while the freshness check was on. SYNC-186 (ddp-sync #304, head `b920683`, reviewed and merged by the PM session as **`b569cf2`**) closes this in three places:
* **The archive hook** (`pipelines/openstates_archive.py`) skips the hand-off for a state with LegBot off. It logs one line, `archiver_triggered_legbot_skipped_legbot_replica_off`. **This acts on EC2**, where the archive runs.
* **`trigger_scraper_session_pipeline`** (`api/routes/triggers.py`, `pipelines/scraper_triggered_legbot.py`) refuses such a state however the request arrives. **This acts on the Mac**, where LegBot runs.
* **The per-bill check** (`pipelines/session_pipeline_runner.py`) now runs every time, whether or not `replica_freshness_check_enabled` is on. **This acts on the Mac.**

All three read the same list: each state's `enrollment.legbot_replica` in the host's ddp-open-states checkout (`OPENSTATES_ROOT`, or the Mac default). **If the list can't be read, nothing goes to LegBot, with one Slack alert.** That's why part 1 comes first: if a host's list is stale or unreadable, the release would stop every LegBot run after an archive on that host.

Changed between `834f732` and `b569cf2`: the files above, `config.py` (a comment only), `primitives.md`, `CLAUDE.md`, and tests. **No `.env`, IAM, manifest, Dockerfile, compose, requirements, schedule or migration change.**

## Part 1. Read-only: the state list on each host (before anything else)
Run with the host's ddp-sync code and environment. The reader has been in ddp-sync since SYNC-157, so today's code answers the same as `b569cf2` will:
```
python -c "from ddp_sync.services.jurisdiction_enrollment import enrolled, default_root; r=default_root(); l=enrolled('legbot_replica', None, r); print(r, None if l is None else len(l), None if l is None else sorted(l))"
```
* **Expected on both hosts:** a list of 13: `['az', 'fl', 'la', 'ma', 'mi', 'nc', 'ne', 'nm', 'pa', 'us', 'ut', 'va', 'wa']`. That matches ddp-open-states `main` `b255d69`.
* **Mac, done by me, read-only, 2026-11-22:** with the Mac ddp-sync venv, root `/Users/agentsmith/Developer/repos/ddp-open-states` (`OPENSTATES_ROOT` not set in its `.env`), the answer was 13, exactly the list above. ND, KY, RI and OK are all false there.
    * The Mac's ddp-open-states checkout is at `3e665c7`, older than `main`, but its `legbot_replica` values are the same for all 30 states.
* **EC2 (please run it in the ddp-sync container):** report the root it prints and the list.
    * `None`, or a list that differs from the 13 above: **stop and report**. Don't release; I'll fix the list first through a ddp-open-states PR and a rollout.

## Part 2. The Mac (Ramon's step, on his go)
Do this when no LegBot run is going on the Mac: no `legbot-session-*` run in progress in `logs/ddp-sync.log`. A restart cuts a run off, as on 11-22 at 21:47 UTC.
1. `git -C ~/Developer/repos/ddp-sync status` should show `main`, clean apart from the untracked `.env.bak-*` files, at `834f732` (behind 2). Then run `git -C ~/Developer/repos/ddp-sync pull --ff-only`; expected `b569cf2`.
2. `sudo launchctl kickstart -k system/com.ddp.ddp-sync`.
3. Checks:
    * `GET http://localhost:8001/health` is healthy.
    * There is no `Traceback` in the log since the restart.
    * There is no `legbot_replica_list_unreadable_dispatching_nothing` line.

**Rollback:** `git -C ~/Developer/repos/ddp-sync checkout --detach 834f732`, then the same kickstart.

## Part 3. EC2 (prod agent, after a GO note)
### 0. Gates (stop and report if any fails)
* **G0, list:** part 1 on EC2 printed the 13 states.
* **G1, code:** report the commit `/opt/ddp-sync`'s ddp-sync code is at, with `git status` clean. Then report `git log --first-parent --oneline HEAD..origin/main`.
    * **If it lists anything other than #303 (SYNC-185, `834f732`) and #304 (`b569cf2`), stop and send me the list.** I last saw `cb5eade` (11-17) in these notes. Anything older than #303 needs its own release request, not this one.
* **G2, nothing in flight:** not while a scrape, load, archive or LegBot step that this ddp-sync started is running.
    * **Oklahoma's first scrape (`ok-7192f1fe4328`) and its archive must have finished.** A restart would cut them off.
* **G3, quiet:** no pipeline, scrape, archive or LegBot line in the last 10 minutes, and 0 running `ddp-scrapers` tasks that ddp-sync waits on.
* **G4, rollback point:** tag the running image `ddp-sync:pre-b569cf2` and record its id.

### 1. Pull, build, restart (as for earlier releases)
1. `git -C /opt/ddp-sync merge --ff-only origin/main` (fetch as last time); expected **`b569cf2`**.
2. Build on its own first: `docker compose -p ddp-sync -f infrastructure/docker-compose.prod.yml build`. The live container stays untouched. If the build fails, stop.
3. `sudo systemctl restart ddp-sync`. Nothing else is recreated.

### 2. Checks right after (report each)
* Healthy, with a new image id.
* `Redis connected`.
* 0 `Traceback` since the restart.
* No `legbot_replica_list_unreadable_dispatching_nothing` line.
* `GET /schedule` shows the same per-state scrape jobs as before the restart.

### 3. Read-only, in your next reports
* **The next archive of a state with LegBot off** (for example ky, nd, ok or ri on Sunday 11-29 at 02:00 UTC, which is their default; al and ga on Friday; nv and or on Saturday):
    * the log shows `archiver_triggered_legbot_skipped_legbot_replica_off` for it;
    * there is no `legbot-session-*` hand-off for it.
* **The next archive of a state with LegBot on** (for example wa, usa or pa nightly): its hand-off still fires and is accepted as before.

**Rollback:** `git -C /opt/ddp-sync checkout --detach <G1 commit>`, re-tag `ddp-sync:pre-b569cf2` as `ddp-sync:prod`, then `sudo systemctl restart ddp-sync`.

## Not authorized
* Any `.env`, manifest, IAM or schedule change.
* Turning `replica_freshness_check_enabled` or the scrape-completion trigger on or off.
* Triggering any scrape, archive or LegBot run.
* The broker or api-v3.
* The Mac (part 2 is Ramon's).
* Releasing anything beyond #303 and #304.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
