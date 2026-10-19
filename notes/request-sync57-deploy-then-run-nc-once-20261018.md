# REQUEST: (1) bring the EC2-broker host's ddp-sync up to `main` 3e8a5527 (SYNC-57: the manual scrape trigger accepts `nc`), then (2) run North Carolina once through it and verify (OPEN-397). (Mac side, 2026-10-18 EDT)

**STATUS: GO, conditional.** Ramon asked (Mac session, 2026-10-18) for this note. Part 1 is a deploy; Part 2 is the NC run he handed to you in `request-open397-nc-rerun-on-v36-20261018.md` (ddp-open-states `notes/ops-handoff`), now possible over the normal HTTP route. Do the parts in order and only if every gate passes; a failed gate means stop and report, not improvise. A HOLD note from Ramon overrides this.

**This host only**: the EC2-broker host's ddp-sync (the one that ran NC's scheduled run, `nc-4ad2eace2886`). **Do NOT touch the votebot/ddp-api instance** (its own diverged branch; Ramon cancelled the SYNC-113 deploy there on 2026-10-17).

## 1. Why, and what the pull changes
NC's scheduled run failed on 2026-10-18 02:00 UTC (a vote-key collision, fixed in `ddp-scrapers:v36` / revision 39 and deployed today; your replies are `reply-open397-nc-rerun-gates-done-start-blocked-20261018.md`). You could not start NC alone: the manual trigger accepted only `va, mi, ma, ut, az`. **ddp-sync PR #231 (SYNC-57, merged as `3e8a5527`)** makes the single-state targets come from `openstates_scrape.secondary.jurisdictions` in `config/sync_schedule.yaml` (`va, mi, ma, ut, az, nc` today), so `POST /ddp-sync/v1/trigger/openstates-scrape/nc` works and an unknown target returns a 404 listing the current ones. The route calls `run_single_scrape_job`, the function you already identified, i.e. the same path the scheduler uses per state.

The host should be at `c822058` (your 2026-10-14 deploy), so this is **13 commits**, 15 files, no dependency or Docker changes (checked: no pyproject/requirements/Dockerfile in `c822058..3e8a5527`). What is in it:
- **SYNC-57** (`a7e47f5`, `8cce8e2`): the trigger change above, plus README/primitives.md.
- **SYNC-113** (`790691c`): **retires the Webflow organization merge**: deletes `org_merge.py` and **removes the `webflow_merge_duplicate_orgs` scheduled job** (and its trigger entry). If that job is registered on this host today, the host's job count drops by one after the pull (you reported 18 on 2026-10-14); that is expected, but see gate G5.
- **SYNC-121** (`c8d1f44`): the Voatz-to-Brevo import no longer sends `emailBlacklisted`/`smsBlacklisted`. Inert unless that job is enabled on this host.
- Docs/comments only: SYNC-118 (two docs commits), SYNC-123, CLAUDE.md.
No default is turned on and nothing here starts a run.

## 2. Part 1: the deploy. Gates (read-only until step 3)
- **G1. Starting state.** Branch, `git rev-parse --short HEAD` (expect `c822058`), `git status --short` in the host's ddp-sync checkout. Uncommitted changes, another branch or another commit: **stop and report**; do not stash, discard or reset.
- **G2. Quiet window.** No scrape, archive, load, LegBot relay or backfill in flight; at least an hour clear of **Sunday 02:00 UTC** (the weekly secondary job), **Monday 05:00 UTC** (FL and UT weekly archives) and **Thursday 05:00 UTC** (MI and NC archives). The container recreate briefly stops the scheduler. Pick the first window that satisfies this and say which you used.
- **G3. Rollback point.** The exact commit and the running image/container identity you are replacing (back to `c822058` and the previous image).
- **G4. Environment as expected.** Read, do not print values: `FORK_UPSTREAM_CHECK_ENABLED`, `LEGBOT_ORGANIZATION_CONTENT_ENABLED`, `LEGBOT_ORGANIZATION_RESOLUTION_MODE`, `GRANTBOT_SIMILARITY_REFRESH_ENABLED` unset/off; `KNOWLEDGE_BASE_INDEX_NAME` set here only. Report anything else.
- **G5. The retired Webflow job.** Is `webflow_merge_duplicate_orgs` among the jobs this host registers and is it enabled? If it is **registered and enabled**, stop and report rather than remove it silently (Ramon decided to retire it in SYNC-113, but this host was not named in that decision). If it is not registered here, say so and continue.
- **G6. LegBot side effects of a successful NC load (read-only, decides Part 2).** Read this host's `LEGBOT_SCRAPE_COMPLETION_TRIGGER_ENABLED` (default false) and, if on, its artifact-types and limit settings. If it is **on**, a successful NC load can dispatch bill-artifact generation for NC bills that have no summary yet (OPEN-390: about 3,550 versions with no changelog, a batch Ramon wants ordered deliberately, one LegBot batch at a time). Report the value now. **If it is on, do Part 1 but stop before Part 2 and wait for Ramon.**

## 3. Part 1: the change
1. `git pull --ff-only origin main` in that checkout (expect `3e8a5527`). Not a fast-forward: **stop and report**.
2. Recreate **only the ddp-sync container**, by the host's usual procedure. Do not touch Redis, api-v3, the broker or any other container; change no environment setting.
3. Verify: `/health` healthy, Redis connected, scheduler running; report the job count before and after (expect one fewer if G5 found the Webflow job registered); the code contains the change (`ddp_sync.api.routes.triggers._single_jurisdiction_targets` exists); no startup errors; `git rev-parse --short HEAD` equals `3e8a5527`.
4. Check the route is live without starting anything: `POST /ddp-sync/v1/trigger/openstates-scrape/zz` with this host's own key must return **404** whose `Available:` list contains `nc` (and `va`, `mi`, `ma`, `ut`, `az`, `fl`, `wa`, `usa`). A 404 starts nothing. Report the list.

## 4. Part 2: run NC once (only if Part 1 verified and G6 said off)
1. Re-run the **G2 quiet check** for the NC run itself: it collects on Fargate for roughly an hour (the failed run took 55 minutes to collect), then loads into production RDS. Keep it clear of the Monday 05:00 UTC and Thursday 05:00 UTC archive starts.
2. **Once:** `POST /ddp-sync/v1/trigger/openstates-scrape/nc` (this host's key; it returns 202 `{"status": "started", "target": "nc"}`). No retry if it fails; no second call while the first runs (a 409 means one is already running).
3. Then follow section 3 of the OPEN-397 request, unchanged: the run id, the task's image (must be v36 / revision 39, from ddp-sync's log line), the loader's result (**no `DuplicateItemError`**, `status: ok`), the baseline query again (expect `2025`: **774 votes, 774 keys, id_hash `44906cf9927a198f81937675078aeed1`**, unchanged; `2025E1`: more than 0 votes, and **0** with a key lacking the `2025E1-` prefix), and anything the scheduler starts as a side effect (LegBot dispatch, artifact generation, archive, embedding). Start none yourself and stop none; report what started.
4. If the run or the load fails, or a number differs: stop and report with the log tail. No retry, no rollback of the image without authorization.

## 5. Not authorized
Any other host (above all the votebot/ddp-api instance); any environment or yaml change; enabling any flag; any state but NC; a second NC run; editing or deleting any S3 marker, memory or working-tier object (including `nc-4ad2eace2886`'s files); re-running the loader on an old run; `--allow_duplicates`; `POST .../openstates-scrape/secondary` or any other batch target; any archive, embedding, LegBot or bill-artifact run you start yourself; changes to the Fargate task definition; touching the broker's data.

## 6. Reply on this branch
One note covering: G1 to G6 results and the window used; the commit and container before and after, the recreate time and the job counts; the 404 check's target list; then, if Part 2 ran, everything in 4.3. If a gate stopped you, say which and what you saw.
