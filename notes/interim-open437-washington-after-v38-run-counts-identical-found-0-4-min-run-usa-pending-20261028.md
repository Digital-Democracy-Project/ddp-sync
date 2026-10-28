# Interim (read-only): OPEN-437 Washington after tonight's v38 run: counts identical to the baseline, no vote-event drop; the run found 0 bills and took 4 minutes. USA still to come

Prod agent, Wed 2026-10-28 02:42 UTC. Ran on the operator's "Option A, run the Washington numbers now". Read-only; nothing launched or changed. Output in `/home/bitnami/ops/open437-wa-usa-20261028T0236Z/`. USA launches at 03:00 and I will report it after its `cloud_scrape: done`, together with the 03:15 session cleanup and the broker write check.

## 1. Washington counts (RDS, 02:37:47 UTC) against your baseline (81f191e, 22:10 UTC 10-27)
| | baseline | now |
|---|---|---|
| bills | 3,411 | **3,411** |
| vote events | 2,302 | **2,302** |
| person votes | 171,556 | **171,556** |
| duplicate groups | 1 | **1** |
| vote events with no voters | 0 | **0** |
No drop. No Washington bill was touched in the last 2 hours; the latest vote event was written 2026-06-21.

## 2. The run
* Run **`wa-00a613a60473`**, Fargate task **`2ff0bcb0c391482ca33dfbeb9b295dfd`**; launched 02:30:00, scrape and load done **02:35:35**; ddp-sync `duration_seconds=334.2`.
* Scraper line: `{"source": "wa", "run_id": "wa-00a613a60473", "mode": "incremental", "status": "ok", "found": 0, "duration_s": 246}`.
* **Duration vs the previous nights:** the four previous incremental runs took **3,437 to 3,445 s** (about 57 min; `found: 0` each time); tonight took **246 s** (about 14 times faster) with the same `found: 0`.
* CloudWatch stream: 2 WARNINGs, 1 Traceback, 0 timeouts, 0 blocked/captcha. **The warnings:** "no session provided, using active sessions: {'2025-2026'}" (normal) and "Could not load Digests document listing for House -- skipping just this doctype, not the rest of House's document listings". **The Traceback** is `openstates.exceptions.ScrapeError: no objects returned from WABillScraper scrape`, followed by the collector's own line `wa: no new objects since cutoff (no-op)`: this is the normal "nothing changed" path; the same no-op line appears on every night's Washington run I could find (10-24, 10-25, 10-26, 10-27 at about 03:27 to 03:28, and tonight at 02:34), and the status stays `ok`. I did not compare the Digests warning with earlier nights.

## 3. The chain after the load
Scrape-triggered archive: Fargate task done in **121.3 s**, `archiver_triggered_legbot_no_sessions_touched` (no session touched, so **no LegBot**); bill search refresh `refreshed=0 orphans_removed=0`; knowledge-base run `bills=0 chunks=0 documents=0 failed_bills=0` (3,411 listed, 0 to check); `scrape_triggered_archive_result success True`. **No broker write** (no non-local POST since 02:30). All clean.

## 4. Washington specifically
* **Bills found: 0** (no more than usual: the run found nothing, so the new "pick incremental bills by latest status date" behaviour had nothing to pick, and the chapter-citation code did not run on any bill).
* Chapter citations (from RDS, `opencivicdata_bill.citations` and `opencivicdata_billaction`): **690** Washington bills have an action whose description mentions "chapter"; **9** of them have **no citations at all** (baseline unknown, as you said). I did not count how many carry a citation that itself names a chapter (my second query failed).

## Stop rule
Nothing moved the wrong way for Washington.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
