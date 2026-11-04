# Report: Pennsylvania archive trial finished, exit 0, **but 6 documents were "blocked" (a stop-and-tell condition in your request)**. Louisiana is still running. Written 2026-11-04 05:58 UTC.

**Stop-and-tell, as the request says: `blocked` above 0.** Details below. I started nothing else; Louisiana was already running (launched 04:08 UTC) and is untouched. I did not retry Pennsylvania and did not fetch any of the six addresses again.

## Run
- Start 04:07:19 UTC, end 05:53:07 UTC. **Duration 6,347 s (105 min 47 s)** by the wrapper (the archive's own clock: 6,272 s). Exit code **0**. Wrapper result `{"success": true, "jurisdiction": "pa", "duration_seconds": 6347.3}`. Task `4f910c51ee334bf89332748038f4d068`, image `ddp-scrapers:v42`, launched once, no hooks. Run id `pa-archive-748190f224df`.
- Gates before the launch: 0 ECS tasks, nothing in flight, no ddp-sync job in 10 minutes, well clear of 02:00, 02:30, 03:00 UTC and Sunday 02:00. Before-snapshot: `pa` 0, `la` 0.

## The archive's summary line, verbatim
`pa: 5270 bills checked | fetched=6188 skipped=0 archived=6188 fetch_errors=0 blocked=6 extract_errors=0 conflicts=0 concurrent_writes=0 s3_verified=6188 s3_unverified=0 persist_errors=0`

## The six "blocked" documents
Each is the same message: `blocked response for <address>: expected application/pdf magic bytes, got b'\r\n\t\t<html>\r\n\t\t<h' instead`. The legislature's server answered a PDF address with an HTML page. The six, with their log times (UTC):
1. 04:13:09 `https://www.palegis.us/legislation/bills/text/PDF/2025/0/HB2649/PN3752`
2. 04:34:35 `https://www.palegis.us/legislation/bills/text/PDF/2025/0/HB2124/PN2744`
3. 05:23:51 `https://www.palegis.us/legislation/bills/text/PDF/2025/0/HB0200/PN0561`
4. 05:23:52 `https://www.palegis.us/legislation/bills/text/PDF/2025/0/HB0200/PN2916`
5. 05:39:14 `https://www.palegis.us/legislation/bills/text/PDF/2025/0/HB2359/PN3629`
6. 05:39:56 `https://www.palegis.us/legislation/bills/text/PDF/2025/0/HB2880/PN4096`

What I can and cannot say:
- **They are isolated, not a wall.** Six in 106 minutes among 6,194 downloads; two were back to back (05:23:51 and 05:23:52), the other four spread out. After each one the next download succeeded at once.
- **There is no 403, 429, "Too Many", Barracuda or "block page" line anywhere in the log** (0 of each), no Traceback, no non-zero exit.
- **I do not know whether this is a WAF block or an ordinary error or "not found" page.** I only have the first 16 bytes of each reply, and I did not fetch any of them again (the request says not to retry). Someone who can open those six links in a browser can tell in a minute. The six documents were not saved: 6,194 versions expected (your number) = 6,188 saved + 6 blocked.

## Log (6,306 lines, saved redacted in `notes/pa-archive-trial-20261104-task-log-redacted.txt`)
First 10 lines: `04:07:58 DEBUG openstates.stats: Stat emission is not enabled.` then nine `INFO scrapelib: GET - 'https://www.palegis...'` lines, one a second. Counts: `no function for` 0, 429 0, 403 0, "Too Many" 0, Barracuda 0, block page 0, "blocked response" 6, Traceback 0, **`failed to persist` 0** (so no OPEN-487 file-name case in Pennsylvania). Levels: 1 DEBUG, 6,194 INFO, 103 progress ("heartbeat") lines, 8 others (the summary lines and the six blocked messages), 0 WARNING, 0 ERROR.

## After the run (read-only on the database)
- `ddp_bill_version_document`: Pennsylvania **6,188 rows**, all `application/pdf`, `is_error` false; **0 empty or whitespace-only `raw_text`; 0 under 50 characters**; 6,188 with `archived_at` and `archive_location`; OCR applied 0. **5,267 of the 5,270 Pennsylvania bills have at least one document** (3 have none; likely those whose only documents are among the six blocked, not checked).
- Every other state **unchanged** since the 04:06 snapshot (US 92,768, AL 2,923, AZ 8,935, FL 20,072, MA 12,113, MI 14,458, NC 6,207, NE 3,382, UT 9,371, VA 25,591, WA 11,636). Louisiana is at 6,359 rows and still growing (its task is running).
- Pennsylvania early reads (already posted): 994 at ~18 min, 1,714 at ~30 min, 4,513 at ~77 min; never any empty or short text.

## S3 and side effects
- `s3://ddp-bill-archive/bills/raw/pa/`: **6,188 objects, 462 MB**; `s3_verified=6,188`, `s3_unverified=0`.
- **LegBot, embedding, search-refresh and relay lines for `pa`: 0** in the task log and 0 in ddp-sync's log since 04:07. Disk on this host 142 GB free.
- Nothing else changed.

## Louisiana
Still RUNNING at 05:54 UTC, no stop-pattern lines, no persist errors, 6,359 documents saved so far (of about 10,468 expected). Expected to finish around 07:10 UTC; I will post its full report when it stops, and a progress line at 07:08 UTC if it is still going; 4-hour cap 08:07 UTC.

## Question for you (Ramon)
Do the six "blocked" documents need anything from me? I will not retry them. If you want them fetched again, say so on this branch and say how.
