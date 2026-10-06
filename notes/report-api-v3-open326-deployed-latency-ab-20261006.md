# Report: api-v3 OPEN-326 deployed (d3b5b69); suggest latency measured, with a before/after comparison (prod agent, 2026-10-06)

Replies to `request-deploy-api-v3-open326-name-typos-20261006.md`. Deployed on the operator's go-ahead. Times UTC.

## Deployed
- api-v3 `ae33226` -> `d3b5b6985de4f1360b9862d9ac6638747df822f4` (PR #21). Quiet window first: no backfill locks, ddp-sync idle for 45 min, checkout clean, diff two files only (+204/-2), no migration, dependency, settings or Dockerfile change. ECS tasks could not be checked (this host's role is denied `ecs:ListClusters`).
- Rollback tag `ddp-openstates-api:pre-open326` (`46710c72f38a`). `git pull --ff-only` in `/opt/ddp-open-states/api-v3`, rebuilt (13 s), `up -d --no-deps api` at 22:14:14. New image `e5a6cec321c8`; healthy after about 40 s; restart count 0. Broker `/api/status/`, ddp-sync health and api-v3 `/healthz` all 200 afterward; api-v3's log has no errors and no `apikey=` lines.

## Your checks (suggest, the 8 enrolled jurisdictions, X-API-Key header from the ddp-sync container; every result counted by `entity_type`)
- `Smtih`: 8 results, all `person` (Adam, Adrian, Austin, Carlos Smith ...). `Pelsoi`: 1 `person`, Nancy Pelosi. `Garica`: 8 `person` (Alina Garcia, Brian Garcia, Daniela Garcia ...). `Jhonson`: 8 `person` (Bert, Bill, Chad, Cynthia Johnson ...). `Martniez`: 3 `person` (Marty, Ray D., Teresa Martinez).
- No new noise: `budget`, `housing`, `medicaid` return only `bill` results (8 each).
- The shape checks I skipped last time: `H.R. 1` (8 bills, `HR 1` first), `H.J. Res. 1` (`HJRES 1` first), `HJR A` (`HJR A` first, then `HJR AA`). All bills, none missing.
- NC: not tested for a name (0 current NC people, as you said).

## Latency
**First, my plain loop** (two runs of 80 calls, direct to api-v3, 8 jurisdictions, 20 query words including `smtih`, `garica`, `jhonson`): p50 117 and 114 ms, **p95 427 and 419 ms**, max 480 and 453 ms. That is above your 210 ms line. But the 184 and 189 ms figures from 10-05 came from a different loop that I no longer have, so this is not comparable to them, and I did not roll back (your rule).

**So I measured before and after with the identical loop.** The old image (`pre-open326`, `ae33226`) ran in a throwaway container on the same Docker network with the same environment (removed afterward, nothing published); each container called its own loopback; new and old alternated for three rounds. Same query list, 80 calls per run:

| round | NEW `d3b5b69` p50 / p95 / max (ms) | OLD `ae33226` p50 / p95 / max (ms) |
|---|---|---|
| 1 | 117 / 421 / 431 | 100 / 404 / 447 |
| 2 | 114 / 425 / 480 | 100 / 390 / 411 |
| 3 | 118 / 422 / 440 | 102 / 395 / 452 |

- OPEN-326 adds about **+15 ms at the median and about +25 to 30 ms at p95**; the max is noisy. By word: `education` 422-432 ms new vs 395-396 old; `water` 261-267 vs 237-252; `housing` 254-261 vs 222-232; `smith` 198-209 vs 184-191; `tax` about equal (151-157 vs 153-160). The typo words are fast (`smtih` about 74 ms new).
- **The tail was already about 400 ms before this change.** It comes from common topic words over 8 jurisdictions (`education` alone takes about 400 ms on the old image, about 106 ms on one jurisdiction). So the p95 against the 200 ms bar depends on the query mix: on this mix the old image was already about 390 to 404 ms; on the earlier lighter mix it was 184 to 189 ms.
- Per your rule I am not rolling back, and you asked to be told when p95 is above about 210 ms: it is, on this mix (about 420 ms), with the comparison above showing OPEN-326 contributes roughly 25 to 30 ms of it. Whether real type-ahead traffic looks like my mix or the earlier one I cannot tell from here; a mix taken from real suggest traffic is what the precomputed-surname-column decision needs.

## Not done
No change to the Mac's api-v3; this is the production host only.
