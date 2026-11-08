# Reply (read-only): Louisiana's first run on v48 / revision 50 passes the first-run checks (OPEN-500, OPEN-494). 2026-11-08, 06:05 UTC

Reply to `ecef62e` item 2 for Louisiana (Nebraska was in `58752e8`). Nothing was changed: no retry, relaunch, pause, pull, restart or registration.

**The run:** run `la-68910593ff46`, task `ef9e59d8fbd84dd7b84fd69346ab0256` (created 02:00:02 UTC, **task definition `ddp-scrapers:50`, image v48**), **container exit code 0**, stopped 05:51:42 UTC; ddp-sync logged `collection done, loading into RDS` at 05:52:08 and `cloud_scrape: done` at **05:59:05** (duration 14,343.5 s = 3 h 59 min including the 7-minute import).

## Checks (a) to (g)
- **(a)** no `unexpected keyword argument` in the task log (0 lines, 0 Tracebacks; I read all 19,440 events, paging the stream).
- **(b)** the new line is there: `02:00:44 INFO openstates: LA OPEN-500: ignoring start=2026-11-03T01:13:32 and doing a full walk` (the watermark minus one hour).
- **(c)** the scrape saved **2,616 bills and 3,172 vote events** (log: `bill: 2616`, `vote_event: 3172`), the same as the first scrape (2,616 / 3,172). **RDS now holds 2,616 bills, 3,172 vote events and 230,721 person votes** for Louisiana: equal to the first scrape (2,616 / 3,172 / 230,721). No collapse, no growth.
- **(d)** the import ran (the 7 minutes between `collection done` and `done`); the harness's result line is `{"source": "la", "run_id": "la-68910593ff46", "mode": "incremental", "status": "ok", "found": 5792, "duration_s": 13640}`; task exit code 0. I still cannot read the importer's `bills_new / bills_updated / bills_noop` summary. In RDS: **0 bills created or updated, 0 vote events created, 4 vote events updated** since 02:00 UTC (a full walk that found almost nothing new, since the data was already loaded on 11-03).
- **(e)** **no `no objects returned from` line and no "no changes since cutoff (no-op)" summary.** The watermark in S3 **advanced**: `la.ts` is now `2026-11-08T05:48:03`, count `5792:incremental` (it was `2026-11-03T02:13:32`, `5792:full`). The count `found: 5792` equals the first scrape's 5,792, so this was a real full walk, not an empty one.
- **(f)** the scrape took **3:47:18** (task 13,640 s) against about 4 h for the first scrape. **No 429, 403, captcha or WAF text.** (My text search for `429`/`403` matched 3 lines; all three are vote-event names such as `FINAL PASSAGE (#403)` and `(#429)`, not HTTP codes.)
- **(g)** RDS (`ddp-openstates`) CPU in 20-minute buckets from 04:00 to 06:10 UTC: average **6 to 10%**, maximum **19%**; connections maximum **14** over the window. Louisiana **overlapped Pennsylvania** (04:00 UTC onward, running now) **and Massachusetts** (still running), plus the USA scrape and archive earlier; nothing noticeable on the database.

## Still owed
Pennsylvania: the 3-hour report (about 07:00 UTC) and the end report (about 11:00 UTC; first scrape was about 7 h). Massachusetts is still running (it was not in your list of three). If anything fails or stalls I stop and report; I will not retry.
