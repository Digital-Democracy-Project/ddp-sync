# Reply (read-only): Sunday 11-08 launches ran on revision 50 / v48; Nebraska's first-run checks pass; Louisiana and Pennsylvania still running (OPEN-500, OPEN-494). 2026-11-08, 04:05 UTC

Reply to `ecef62e` (items 1 and 2) and `2f87ce4` item 4. Nothing was changed: no retry, relaunch, pause, pull, restart or registration.

## Launch revisions (read with `describe-tasks` on each launched task)
- **Every task launched since the Sunday 02:00 UTC batch ran `ddp-scrapers:50`, image `ddp-scrapers:v48`** (digest-tag `v48`), **not 49 / v47**. Revision 50 (v48) was registered after revision 49 (your `b57694b9` says about 05:00 UTC on 11-07), and the family resolves to the latest, so v47 / 49 was superseded before any batch used it. Last cut-over seen before: USA's second collection on 11-07 at 04:22 UTC was still 48 / v46.
- **Sunday 02:00 batch:** eight tasks created 02:00:02 to 02:00:03 UTC (florida, louisiana, utah, michigan, north carolina, massachusetts, virginia, arizona) and **Nebraska at 02:01:37 UTC** (run `ne-e0bd0f09a098`, task `b4cb1c4536d34c5e866bb8027dc4f47a`): all **revision 50 / v48**. The secondary job's list was `az la ma mi nc ne ut va` (Pennsylvania absent), as intended.
- **USA 03:00 UTC** (run `usa-d21ca51e812d`) and **Pennsylvania 04:00:00 UTC** (run `pa-1adcd7a750b2`, task `12d73f4c6b5e403eb554810ba6e7f87f`, created 04:00:01): **revision 50 / v48**.
- So the answer to `2f87ce4` item 4 is: **the next scheduled scrape runs v48 (revision 50)**, as does Pennsylvania's 04:00 run on 11-08.

## Nebraska, first-run checks (a) to (g): it finished at 03:09:46 UTC (task exit code 0, stopped 03:05)
- **(a)** no `unexpected keyword argument` in its task log (0 lines; also 0 Tracebacks).
- **(b)** the new line is there: `02:02:20 INFO openstates: NE OPEN-500: ignoring start=2026-11-02T22:26:28 and doing a full walk` (the start is the watermark minus one hour, as expected).
- **(c)** the scrape saved **1,847 bills and 1,772 vote events** (log: `bill: 1847`, `vote_event: 1772`), equal to the first scrape's numbers. RDS now holds **1,847 bills, 1,772 vote events and 86,723 person votes** for Nebraska: the same as the first scrape (1,847 / 1,772 / 86,723), no collapse and no growth.
- **(d)** the import ran: ddp-sync logged `collection done, loading into RDS` at 03:05:47 and `done` at 03:09:46 (about 4 min); the harness's own result line is `{"source": "ne", "run_id": "ne-e0bd0f09a098", "mode": "incremental", "status": "ok", "found": 3621, "duration_s": 3626}`. **I could not read the importer's `bills_new / bills_updated / bills_noop` summary** (it is not in the ddp-sync INFO log I can read). What I can see in RDS: no bill row created or updated since 02:00 UTC, **0 created and 1,397 of 1,772 vote events updated** since 02:00 UTC. So it re-saved vote events and found no new bills.
- **(e)** **no `no objects returned from` line and no "no changes since cutoff (no-op)" summary** in the log. The watermark in S3 **advanced** (`ne.ts` 2026-11-08T03:02:44, count `3621:incremental`; it was `2026-11-02T23:26:28`, `3621:full`), and it advanced after a real full walk, not an empty one.
- **(f)** the scrape took **1:00:24** (task 3,626 s; 4,088 s in ddp-sync including the import) against about 68 min for the first scrape. No 429, 403, captcha or WAF text in the log.
- **(g)** RDS CPU 02:00 to 04:00 UTC in 10-minute buckets: average **6 to 11%**, maximum **22%** (at 02:30); nine scrape tasks ran at the same time at 02:00 and nothing overlapped badly. (That is the Pennsylvania-style check for Nebraska's window.)

## Rest of the Sunday batch (from ddp-sync's own lines, not each task log)
`cloud_scrape: done` for florida (94 s), michigan (217 s), utah (2,140 s), north carolina (3,560 s), nebraska (4,088 s), arizona (5,115 s); virginia's scrape-triggered archive finished 03:24. **Louisiana and Massachusetts are still running** (two tasks created at 02:00:02 are still up at 04:03). I did not read those logs yet.

## Pennsylvania
Launched 04:00:01 on revision 50 / v48 (above). The 3-hour report is due about 07:00 UTC and the end report when it finishes (the first scrape took about 7 h). I will read the same checks (a) to (g), including RDS CPU and connections and whether it overlaps another job (it overlaps Louisiana and Massachusetts now). If it fails or stalls I stop and report; I will not retry.

## Louisiana
End report when it finishes (the first scrape took about 4 h, so about 06:00 UTC).
