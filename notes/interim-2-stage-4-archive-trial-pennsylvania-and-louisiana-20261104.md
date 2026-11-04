# Interim 2: Stage 4 archive trial, Pennsylvania and Louisiana (OPEN-478, OPEN-480). Both still running, still no stop condition. Written 2026-11-04 05:25 UTC.

Follows `interim-stage-4-archive-trial-pennsylvania-and-louisiana-20261104` (04:30 UTC). Tasks unchanged: pa `4f910c51ee334bf89332748038f4d068` (started 04:07:19 UTC), la `e49db073760f41a6b240205a3f806d75` (started 04:08:04 UTC). Each launched once, no retry, no hooks.

## Pennsylvania early reads (read-only on the database)
| when | saved | `is_error` | empty or under 50 characters | shortest five | media type |
|---|---|---|---|---|---|
| about 18 min (04:25 UTC) | 994 | 0 | 0 (0.0%) | 381, 392, 452, 489, 514 | application/pdf 994 |
| about 30 min (04:37 UTC) | 1,714 | 0 | 0 (0.0%) | 381, 392, 452, 489, 514 | application/pdf (1,715 counted a moment later) |
| about 77 min (05:24 UTC) | **4,513** | 0 | **0 (0.0%)** | **362, 369, 374, 381, 392** | application/pdf 4,513 |

The numbered-line reader keeps returning real text for Pennsylvania: no empty or short document in 4,513, the shortest is 362 characters. The 20% stop rule has never come close.
Pace: about 58 documents a minute, so about 73% of Pennsylvania's 6,194 versions in 77 minutes; I expect it to finish around 06:00 UTC (not seen yet, only the pace).

## Both tasks (log scan every 4 minutes, last at 05:21 UTC)
- pa: RUNNING, 4,448 log lines; la: RUNNING, 4,452 log lines.
- **0** lines with 429, 403, "Too Many", Barracuda, block page or Traceback; **0** `no function for`; **0** `failed to persist` lines so far (so no repeat of Nebraska's too-long file name yet on either state).
- I did not read Louisiana's saved documents (the request asks for the early read on Pennsylvania only); Louisiana's first read will be in its report.

## Still to come
Each state's full report when its task finishes (summary line verbatim, every `failed to persist` line, pattern counts, first 10 log lines, after-counts by media type and `is_error`, other states unchanged, S3 count and size, no LegBot lines). Louisiana's progress at about 3 hours (07:08 UTC) if it is still running; either task at 4 hours (08:07 UTC) if still going.

Nothing else changed: no scrape, no other state, no people load, no config, no restart.
