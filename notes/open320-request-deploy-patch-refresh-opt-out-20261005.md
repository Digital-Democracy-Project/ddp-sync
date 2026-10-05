# Please deploy OPEN-320 (patch-refresh opt-out) to the EC2-broker ddp-sync

**To:** the prod agent on the EC2-broker host (`/opt/ddp-open-states`, co-located with `ddp-broker-py`).
**Not for** the votebot/ddp-api instance -- it runs its own diverged branch on purpose (see `CLAUDE.md`),
and this change should not be pulled there.

**Ticket:** OPEN-320 (https://digitaldemocracyproject.atlassian.net/browse/OPEN-320), now In Review.
**PR:** https://github.com/Digital-Democracy-Project/ddp-sync/pull/192, **merged to `main` as `7f2f9f0`**.

## Why

Your host's `ddp-sync` fails and Slack-alerts every night at 9:00 PM ET (01:00 UTC):

```
OpenStates scrape failed: patch refresh — exited 1: /opt/ddp-open-states/apply-local-patches.sh:
line 52: cd: /Users/agentsmith/Developer/repos/ddp-open-states/openstates-core: No such file or directory
```

`apply-local-patches.sh` keeps the Mac Studio's *live editable* openstates checkouts on their forks'
`main`, and hardcodes the Mac's paths. This host scrapes through baked-in Fargate images, so it has no
such checkouts and nothing to refresh -- the job should simply not run here. The shared
`sync_schedule.yaml` can't turn it off for one host, so the PR adds a per-host flag.

## What changed

- New env flag **`OPENSTATES_PATCH_REFRESH_ENABLED`** (default `true`, so every other host is unchanged),
  ANDed with `patch_refresh.enabled` in the scheduler, same pattern as `OPENSTATES_SCRAPE_ENABLED` /
  `OPENSTATES_ARCHIVE_ENABLED` / `MI_COOKIE_PUBLISH_ENABLED`.
- `infrastructure/docker-compose.prod.yml` sets it to `false` for this host.
- `apply-local-patches.sh` itself is **not** changed (still correct and needed on the Mac).

## What to do

This changes Python code under `src/` (baked into the image) **and** an env var in the compose file, so
it needs a rebuild and a recreate, not just a restart or a `git pull` (only `config/` is bind-mounted).

1. `git pull` `ddp-sync` on this host to `main` (`7f2f9f0` or later).
2. Rebuild and recreate **the `ddp-sync` service only** -- name it explicitly, do not run an unscoped
   `--force-recreate` (that has hit shared services before). Use your usual build wiring; the build needs
   `GITHUB_PERSONAL_ACCESS_TOKEN` exported, per the compose file's `secrets:` block.
3. Confirm the container actually has the flag: `docker compose -f infrastructure/docker-compose.prod.yml
   exec ddp-sync env | grep OPENSTATES_PATCH_REFRESH_ENABLED` should print `=false`.
4. Confirm the job is gone from the live schedule (the schedule endpoint on this instance, or the
   scheduler's startup log): there should be **no** `openstates_patch_refresh` entry, and **no**
   `openstates_patch_refresh: registered` log line. Your other OpenStates jobs must still be registered --
   this flag only affects the patch job.
5. After the next 01:00 UTC, confirm the "patch refresh -- exited 1" Slack alert did not fire.

## Please also report back (this is the one thing I couldn't verify from here)

The committed `docker-compose.prod.yml` says `OPENSTATES_SCRAPE_ENABLED=false` for this host (with a
comment that this stops the six recurring OpenStates jobs, patch refresh included) -- yet this host is
clearly running scheduled scrapes (UT/NC/MI ECS tasks failed at ~10 PM ET). So the live environment
evidently differs from the committed file. Please paste the output of:

```
docker compose -f infrastructure/docker-compose.prod.yml config | grep OPENSTATES
```

and tell me where the live value comes from (a host `.env`, an edited compose file, an override file).
The new flag is set explicitly, so the fix works either way -- but if the live file is not the committed
one, a `git pull` could overwrite a local edit, so **check `git status` / `git diff` on the compose file
before pulling** and keep whatever local settings are real.

## Separate, not part of this deploy

The same alert batch also showed UT, NC and MI ECS scrapes exiting 1 (after 92s, 61s and 273s) and an MA
archive giving up after 43,200s. Those have no root cause yet and are **not** fixed by this change. If you
can, a stopped-task stderr tail (CloudWatch group `/aws/ecs/<ecr repo>`) for one failed UT or NC task would
let us start diagnosing them.
