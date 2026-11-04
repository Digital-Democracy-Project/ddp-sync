# Interim: Stage 4 archive trial, Pennsylvania and Louisiana (OPEN-478, OPEN-480). Both tasks running, no stop condition so far. Written 2026-11-04 04:30 UTC.

Run on the user's go ("baseline first, then PA and LA"), after the api-v3 baseline (`1f82173` on the ddp-open-states branch). Same method as Nebraska: `_run_archive_fargate("<st>", config=<the openstates_archive block>)` from the ddp-sync container, once per state, no hooks. If the user's go had not come first, the request's gates still held: 0 ECS tasks, nothing in flight, no ddp-sync job in the last 10 minutes, and well clear of 02:00, 02:30, 03:00 and Sunday 02:00 UTC.

## Launches (each once, no retry)
| state | started (UTC) | ECS task | status at 04:25 |
|---|---|---|---|
| pa | 04:07:19 | `4f910c51ee334bf89332748038f4d068` | RUNNING, 1,008 log lines |
| la | 04:08:04 | `e49db073760f41a6b240205a3f806d75` | RUNNING, 1,003 log lines |

Image `ddp-scrapers:v42` on both. Before-snapshot of `ddp_bill_version_document` taken at 04:06: 207,456 rows in all, `pa` 0, `la` 0, `ne` 3,382 (the 52 rows more than after Nebraska are federal, from the USA job, not these states).

## Stop conditions: none hit so far
Log scan every 4 minutes on both tasks: 0 lines with 429, 403, "Too Many", Barracuda, block page or Traceback; 0 `no function for`; 0 `failed to persist` lines yet.

## Pennsylvania early read (step 5), first one at about 18 minutes after launch (04:25 UTC)
`saved=994, is_error=0, empty_or_under_50_characters=0 (0.0%), shortest five lengths 381, 392, 452, 489, 514 characters, media type application/pdf 994`.
So the numbered-line reader is returning real text for Pennsylvania's first 994 documents, nowhere near the 20% stop line. The second read is due at about 30 minutes (about 04:37 UTC) and I will post it if it is different. If the stop rule is ever hit, the user has told me to report at once and leave both tasks running (no `ecs stop-task`).

## Still to come
- Each state's full report when it finishes (summary line verbatim, every `failed to persist` line, pattern counts, first 10 log lines, after-counts by media type and `is_error`, other states unchanged, S3, no LegBot lines).
- Louisiana's progress at about 3 hours (07:08 UTC) if it is still running; either task at 4 hours (08:07 UTC) if still going.
- Expected: Pennsylvania about 1.7 hours, Louisiana about 3 hours (not seen yet).

Nothing else was changed: no scrape, no other state, no people load, no `archive.scheduled`, no config, no restart.
