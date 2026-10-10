# Nightly people-pull: PR #206 is built and tested in dev (SYNC-102); deploy it after it merges (Claude, 2026-10-10)

Answers your `reply-nightly-people-pull-please-build-and-test-in-dev-then-pr-and-i-deploy-20261010.md`. Ramon approved all my recommendations. **Do nothing until Ramon (or I) tell you #206 is merged.** Then deploy as below.

**PR:** https://github.com/Digital-Democracy-Project/ddp-sync/pull/206 · **Ticket:** SYNC-102. 15 new tests pass, including a real-git fast-forward and a diverged checkout that is refused and left untouched. Full suite: 1660 pass, 1 failure (`test_trigger_legbot_analyze_bill_full::test_retry_failed_is_passed_through`) that also fails on clean `main`.

## What it does
- Daily 03:00 UTC: `git -C $OPENSTATES_ROOT/people pull --ff-only`, argv list, `GIT_TERMINAL_PROMPT=0`, 300 s timeout. On your host that is `/opt/ddp-open-states/people`. Never rebase, force, reset, checkout or merge.
- **Flag: `OPENSTATES_PEOPLE_PULL_ENABLED`, default off.** Set `true` in the EC2-broker instance's environment (this is the only value to set). The YAML block `openstates_scrape.people_pull` (enabled, `sync_time_utc: "03:00"`) is shared. It sits under `openstates_scrape`, so `OPENSTATES_SCRAPE_ENABLED` must also be true on that instance (the same gate `patch_refresh` uses).
- I dropped the "skip if the weekly import is running" guard: there is no running state to read, 03:00 daily cannot overlap Sunday 10:00, and a concurrent pull fails on the git lock anyway.
- **Failure visibility:** any failure (non-zero exit, timeout, any exception) writes flow status `failed` and alerts to Slack via the existing `_alert_scrape_failure`. `/health` now lists the flow `openstates_people_pull`. Flow status keeps a before/after head sha and commit date, the branch, git's summary line and the path.
- **Manual run:** `POST /trigger/openstates-scrape/people-pull` (does not check the flag, on purpose, like the other targets).

## Deploy steps (yours, on-host; the usual image rebuild)
1. Settle your host-local commits first (Part 1 of my earlier note). A dirty compose/service/render script will otherwise fight the pull of `main`.
2. Pull `main`, then `render-env.sh` and `docker compose -p ddp-sync -f /opt/ddp-sync/infrastructure/docker-compose.prod.yml up -d --build`, with `OPENSTATES_PEOPLE_PULL_ENABLED=true` in the instance environment.
3. Verify: log line `openstates_people_pull: registered sync_time=03:00`; the job id `openstates_people_pull` in the scheduler; then fire the trigger once and read `ddp:flow:openstates_people_pull` (status `completed`, `head_sha_before` and `head_sha_after`, `head_commit_date_after` newer than 2026-09-24 if the fork has moved). It pulls the **DDP fork** (`ddp` remote) only if that clone tracks it; tell me which remote and branch it pulled from (`branch` in the status, plus `git -C ... rev-parse --abbrev-ref @{u}`).
4. Rollback: unset the flag and recreate the container.
The Mac instance needs its own flag set later; nothing to do there now.

## Two things about this host that I want you to re-check (names only, no values)
1. You said the container has no `SLACK_BOT_TOKEN`. Ramon pasted real Slack alerts today that came from this instance's jobs (patch-refresh failures at 9:00 PM, ut/nc/mi collection exits, an ma archive "gave up after 43200s"). So something posts. Please list which of `SLACK_BOT_TOKEN`, `SLACK_WEBHOOK_URL`, `CAMS_API_TOKEN` the **ddp-sync container** has (names only). The new alert relies on the same path those use.
2. The 9:00 PM patch-refresh alert says `/opt/ddp-open-states/apply-local-patches.sh` line 52 does `cd /Users/agentsmith/Developer/repos/ddp-open-states/openstates-core` (the Mac's path). Please confirm that line on the host (read-only). If so, patch refresh can never succeed there; the host's `OPENSTATES_PATCH_REFRESH_ENABLED` is the stop-gap. Do not edit the file; Ramon decides (that repo is changed through its dev checkout and a PR).

## Precautions for the 13 commits that ride the same rebuild (host `faa9630` to `e3e36f4`)
I read them. Six files change: `scheduler.py`, `sync_schedule.yaml`, `broker_client.py`, `triggers.py`, `bill_organization_position_research.py`, a new `verify_org_citations.py`.
- **SYNC-100 (org positions) needs a newer broker.** `write_bill_organization_position` now expects the broker to upsert and answer `{id, result, verification_verdict}`. That broker change is ddp-broker-py PR #429 (`3002b608`), merged after your 10-09 deploy of `28fdb2f5`. **Do not run the org-position research or the new `POST /trigger/verify-org-citations` until the broker has #429**, or they will create duplicate rows (the old broker always inserts). The people-pull does not depend on this; deploying the image without calling those triggers is safe.
- **SYNC-95:** `alert_backlog_over` goes from 0 to 100, so a knowledge-base archive run with more than 100 bills out of step now posts a Slack alert. After the rebuild, expect none unless something regressed; an alert in the first nights is information, not an outage.
- **`mi_cookie_publish`** changes from every 6 hours to the 1st of each month at 08:00 UTC. After the restart it will **not** publish until 2026-11-01; the cookie already in S3 (about a year to expiry per the YAML comment) is what the reader uses. If you want one now, trigger it by hand.
- No new dependencies and no changes to compose, Dockerfile or `pyproject.toml` in those 13 commits.
