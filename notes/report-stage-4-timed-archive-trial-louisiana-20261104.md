# Report: Louisiana archive trial finished clean (exit 0, 10,468 of 10,468 saved, nothing blocked). **Posted about 8 hours late.** Written 2026-11-04 15:35 UTC.

**Late-report disclosure.** The task stopped at 07:04 UTC. My watcher noticed at 07:06:50 UTC and exited, but I did not read it and post this until 15:30 UTC, about **8 hours 25 minutes** after the task ended. The request asked for Louisiana's progress at about 3 hours (07:08 UTC) and a report when it finished; neither was posted on time. Nothing was wrong on the host in the meantime (the task had already stopped); the delay is mine. The numbers below are read now from the logs and the database, not from memory.

## Run
- Start 04:08:04 UTC, end 07:04:04 UTC. **Duration 10,558 s (2 h 55 min 58 s)** by the wrapper (the archive's own clock: 10,487 s). Exit code **0**. Wrapper result `{"success": true, "jurisdiction": "la", "duration_seconds": 10558.3}`. Task `e49db073760f41a6b240205a3f806d75`, image `ddp-scrapers:v42`, launched once, no hooks. Run id `la-archive-8ea59f72d4ca`. Inside the 4-hour limit (14,400 s) with room to spare.
- Gates before launch: as in the Pennsylvania report (0 ECS tasks, nothing in flight, no job in 10 minutes, clear of 02:00, 02:30, 03:00 UTC).

## The archive's summary line, verbatim
`la: 2616 bills checked | fetched=10468 skipped=0 archived=10468 fetch_errors=0 blocked=0 extract_errors=0 conflicts=0 concurrent_writes=0 s3_verified=10468 s3_unverified=0 persist_errors=0`

## Log (10,637 lines, saved redacted in `notes/la-archive-trial-20261104-task-log-redacted.txt`)
First 10 lines: `04:08:42 DEBUG openstates.stats: Stat emission is not enabled.` then nine `INFO scrapelib: GET - 'https://www.legis.la.gov/Legis/ViewDocument.aspx?d=...'` lines, about one a second. Counts: `no function for` 0, 429 0, 403 0, "Too Many" 0, Barracuda 0, block page 0, **"blocked response" 0**, Traceback 0, **`failed to persist` 0** (so no OPEN-487 file-name case in Louisiana). Levels: 1 DEBUG, 10,468 INFO, 166 progress ("heartbeat") lines, 2 others (the summary lines), 0 WARNING, 0 ERROR.

## After the run (read-only on the database, read at 15:30 UTC)
- `ddp_bill_version_document`: Louisiana **10,468 rows**, all `application/pdf`, `is_error` false; **0 empty or whitespace-only `raw_text`; 0 under 50 characters**; 10,468 with `archived_at` and `archive_location`; OCR applied 0. **2,599 of the 2,616 Louisiana bills have at least one document** (17 have none; not looked into).
- Table total **224,112** rows.
- **Every other state is exactly unchanged** against the 04:06 snapshot, and Pennsylvania still 6,188, Nebraska 3,382 (US 92,768, AL 2,923, AZ 8,935, FL 20,072, MA 12,113, MI 14,458, NC 6,207, UT 9,371, VA 25,591, WA 11,636). Louisiana grew only from the task (6,359 at the time the Pennsylvania task finished, then to 10,468).

## S3 and side effects
- `s3://ddp-bill-archive/bills/raw/la/`: **10,468 objects, 508 MB**; `s3_verified=10,468`, `s3_unverified=0`.
- **LegBot, embedding, search-refresh and relay lines for `la`: 0** in the task log and 0 in ddp-sync's log since 04:08. Disk on this host 142 GB free (51% used).
- No ECS task is running now; nothing else changed.

## Both states together
| | Pennsylvania | Louisiana |
|---|---|---|
| duration | 105 min 47 s | 175 min 58 s |
| saved / expected | 6,188 / 6,194 | 10,468 / 10,468 |
| blocked | **6** (HTML where a PDF was expected; reported in `10db4fe`) | 0 |
| `failed to persist` | 0 | 0 |
| empty or under 50 characters | 0 | 0 |
| S3 objects | 6,188 (462 MB) | 10,468 (508 MB) |

Open from Pennsylvania: whether the six blocked documents need a second look; I have not fetched them again and am waiting for your answer on that branch.

Nothing else changed: no scrape, no other state, no people load, no config, no restart.
