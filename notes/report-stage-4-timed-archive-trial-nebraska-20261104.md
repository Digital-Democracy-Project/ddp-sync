# Report: Stage 4 timed archive trial, Nebraska only (OPEN-479). Finished clean, one file not saved.

**Ran inside the window the request asked me to avoid.** The request said not between 01:50 and 04:00 UTC. The user told me in this session to run it now ("let's run it now, since it's independent of the other scrapers"), so it started at 01:41:27 UTC and ran until 02:39:38 UTC. The 02:30 WA scrape and the 03:00 USA scrape overlapped or followed it; I did not look for interference between them.

## Run
- Launched **once**, no retry: `_run_archive_fargate("ne", config=<openstates_archive block>)`, one Fargate task, **no hooks**. The wrapper and the ECS task started 01:41:27 UTC, ended 02:39:38 UTC. **Duration 3,490 s (58 min 10 s)** as the wrapper measured it; the archive's own clock says 3,389 s.
- ECS task `432405b6478b44feaa2f9aed07611e33`, task definition **`ddp-scrapers:45`, image `v42`** (the revision-45 confirmation asked in `f18db1a`), 1024 cpu / 2048 MB. **Exit code 0.** Wrapper result `{"success": true, "jurisdiction": "ne", "duration_seconds": 3490.0}`. Run id `ne-archive-c3ee5df229d8`.
- Gates before launch (01:41:24): no ECS tasks, nothing in flight, no ddp-sync job in 10 min. Final database snapshot before the run: `ddp_bill_version_document` 204,022 rows, Nebraska 0.

## The archive's summary line, verbatim
`ne: 1847 bills checked | fetched=3383 skipped=0 archived=3382 fetch_errors=0 blocked=0 extract_errors=0 conflicts=0 concurrent_writes=0 s3_verified=3382 s3_unverified=0 persist_errors=1`

**The one `persist_errors=1`** (a stop-worthy line in my reading; please judge): `failed to persist https://nebraskalegislature.gov/FloorDocs/109/PDF/AM/AM2748.pdf to <local file name>.pdf: [Errno 36] File name too long`. The task builds a local file name from the URL and one name was longer than the file system allows (the amendment AM2748). That one file was fetched but not saved; the other 3,382 were. Nothing else failed. I did not retry.

## Log (3,442 lines, saved redacted in `notes/ne-archive-trial-20261104-task-log-redacted.txt`)
First 10 lines (links shortened):
```
01:42:07 DEBUG openstates.stats: Stat emission is not enabled.
01:42:07 INFO scrapelib: GET - 'https://nebraskalegislature.<x>.pdf'   (x9 more, 01:42:08 to 01:42:15)
```
Counts: `no function for` 0, 429 0, 403 0, "Too Many" 0, Barracuda 0, Traceback 0. "block" appears twice, both in the summary line (`blocked=0`). Levels: 1 DEBUG, 3,383 INFO, 58 progress lines ("heartbeat"), 0 warning, 0 error lines apart from the one failed-to-persist message. One download roughly every 1 s (3,383 downloads in 58 min).

## After the run (read-only counts in the production database)
- `ddp_bill_version_document`: **207,404** rows (= 204,022 + 3,382).
- **Nebraska: 3,382 rows**, all `application/pdf`, `is_error` false; **0 empty or whitespace-only `raw_text`**; 3,382 with `archived_at` and `archive_location`; OCR applied 0. Documents cover **all 1,847 Nebraska bills** (1,847 of 1,847 have at least one).
- Every other state is **exactly unchanged** against `before.json` (US/federal 92,716, AL 2,923, AZ 8,935, FL 20,072, MA 12,113, MI 14,458, NC 6,207, UT 9,371, VA 25,591, WA 11,636).

## Mirror in S3
- 3,382 objects under `s3://ddp-bill-archive/bills/raw/ne/`, 247 MB in total, one per saved row; `s3_verified=3,382`, `s3_unverified=0`.
- One checked by hand (head only): `.../LB621--eb603a14-.../Introduced-35387b64dfe1a8bc.pdf`, 59,277 bytes, storage class `GLACIER_IR`.

## Side effects
- Disk on this host: 142 GB free (50% used), no real change (the files went to S3 from Fargate).
- **LegBot, embedding, search-refresh and relay lines for Nebraska: 0** in the task log and 0 in the ddp-sync container log since 01:41.
- Nothing launched for any other state, no config edit, no restart.

## Not looked at
- Whether `AM2748` has another saved version or only the one that failed (I did not query that).
- The text content of the saved Nebraska files beyond "not empty".
