# Report: api-v3 OPEN-316 search fixes deployed (prod agent, 2026-10-06)

Completes item 2 of `request-deploy-sync68-and-api-v3-search-fixes-20261006.md`; follows my earlier reply on this branch (`c6d90e5`). Deployed on the operator's go-ahead.

## What was deployed
- api-v3 `1b7fae0` -> `ae33226` (PRs #18, #19, #20; PR #21 is not included). Code-only: the diff was two files (`api/ddp_search.py` and its tests); no migration, dependency, settings or Dockerfile change.
- Checkout `/opt/ddp-open-states/api-v3` (the build context of the `ddp-openstates-rds` compose project), clean before, `git pull --ff-only`. Image rebuilt in about 17 s; only the `api` service was recreated (`up -d --no-deps api`, 21:22:48 UTC), not Redis or the database. New image `46710c72f38a`; healthy about 20 s later; restart count 0.
- Rollback: the old image is tagged `ddp-openstates-api:pre-open316` (`dc862ab73a74`). Retag it as `ddp-openstates-api:local` and run the same `up -d --no-deps api`. I have not run that rollback.
- Quiet window: no backfill locks, ddp-sync idle for 45 min, next scheduled jobs 02:00 UTC. ECS tasks could not be checked (this host's role is denied `ecs:ListClusters`).

## Your checks (X-API-Key header, from inside the ddp-sync container; `jurisdiction` is required or the endpoint returns 400)
- `HB 1C`, FL: `/ddp/search` exact tier has 2 hits, both labelled `HB 1C`; `/ddp/search/suggest` lists the same two. UT and VA: none (nothing to find).
- `HB 1A`, FL: 1 exact hit on both endpoints.
- `HB 99999999`, FL, UT and VA: `/ddp/search` returns 0 in every tier (no look-alike titles); suggest returns 0.
- `HB 1` (unchanged behaviour): FL exact 2 then text matches, suggest lists `HB 1`, `HB 1`, `HB 1D`, `HB 11`; UT exact 1, suggest `HB 1`, `HB 16`, `HB 15`, `HB 17`; VA exact 1, suggest `HB 1`, `HB 11`, `HB 13`, `HB 14`.

## Not tested
- Dotted forms (`H.R. 1`, `H. R. 1`, `H.J. Res. 1`): my script only used `H.R. 1` for the US jurisdiction, and never ran it. Letter-only forms (`HJR A`) were not tested either.
- I did not check what the two `HB 1C` hits in FL are (probably two sessions); I only saw their labels.

## After the deploy
Broker `/api/status/`, ddp-sync health and api-v3 `/healthz` all returned 200. api-v3's log had no errors and no `apikey=` lines.
