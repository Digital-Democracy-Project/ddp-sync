# Reply (read-only): Pennsylvania's nightly run at about 3 hours (OPEN-500, OPEN-494). 2026-11-08, 07:05 UTC

Reply to `3a79efc` item 1 and `ecef62e` item 2. Nothing was changed: no retry, relaunch, pause, pull, restart or registration. The run is **still going**; this is the interim report.

**The run:** run `pa-1adcd7a750b2`, task `12d73f4c6b5e403eb554810ba6e7f87f`, created 04:00:01 UTC, **task definition `ddp-scrapers:50`, image v48**, status RUNNING at 07:02 UTC.

## Checks so far (I read all 23,004 log events of the stream, paging it)
- **(a)** no `unexpected keyword argument` (0 lines, 0 Tracebacks): the 11-07 crash is gone.
- **(b)** the new line is there: `04:00:41 INFO openstates: PA OPEN-500: ignoring start=2026-11-03T04:12:48 and doing a full walk`.
- **(c) progress against the first scrape (5,270 bills / 5,858 vote events / 425,560 person votes):** after 3 h 02 min the scraper has saved **2,544 bills and 2,466 vote events** (48% and 42%); the person votes only appear in the import at the end. The last 30 minutes saved 358 bills (about 716 an hour). **My estimate, from that rate and the first scrape's 7 h 9 min, is that it ends between about 10:30 and 11:30 UTC; it is an estimate, not a measurement.**
- **(e)** no `no objects returned from` and no "no changes since cutoff (no-op)" line so far.
- **(f)** **No real 429 or 403**: my text search matched 15 lines containing `403` or `429`; all are roll-call numbers in URLs such as `rollcallid=429`, `rcNum=403`, or bill numbers, not HTTP codes. The only warning in the log other than the insecure-request one is `no session provided, using active sessions: {'2025-2026'}`. No retry, timeout, Forbidden, Too Many Requests, captcha or Access Denied text.
- **(g)** RDS (`ddp-openstates`) CPU over the last 10 minutes before 07:02: **average 6.3%, maximum 7.5%; connections 12**. It overlaps **Massachusetts' scrape (still running, 02:00 start)**; Louisiana finished at 05:59, the USA scrape and archive earlier. No other job is starting before the end: the next ddp-sync jobs are the 11-09 02:00 to 04:00 UTC batch.

## Next
The end report when it finishes (exit code, bills / vote events / person votes against the first scrape, import summary as far as I can read it, the watermark, the final RDS CPU, and a read of Pennsylvania's `motion_classification` values). If it fails or stalls I stop and report; I will not retry it.
