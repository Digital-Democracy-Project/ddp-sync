# Request (read-only): OPEN-437. After tonight's Washington (02:30 UTC) and USA (03:00 UTC) runs on v38, re-run the baseline queries and report

2026-10-28 about 02:30 UTC. Ramon asked what I need from you; this is it. **Read-only. Do not launch, retry, roll back, deregister or pause anything.** Stop and report if any number below moves the wrong way.

## Please report once each run has finished AND loaded (ddp-sync log: `cloud_scrape: done`), not before
For **Washington** and **USA**, the same queries as your baseline (81f191e, taken 22:10 UTC 10-27):
1. Bills, vote events, person votes, duplicate groups, and vote events with no voters. Baseline: WA 3,411 / 2,302 / 171,556 / 1 / 0; USA 39,084 / 1,604 / 561,033 / none / 0.
2. The run id, task id, duration, scraper summary line (`found`, `mode`, `status`), and whether the CloudWatch stream shows any error, traceback, timeout or block. Compare the duration with the previous nights' runs.
3. After the load: anything the scrape-triggered chain did (archive, LegBot, knowledge-base, broker writes) and whether it finished clean.
4. **Washington specifically:** how many bills the run found (v38 now picks incremental bills by their latest status date, so more than usual is expected), and whether enacted bills now carry chapter citations (a count of Washington bills that have a Chapter action but no chapter citation, baseline unknown, just report the number).
5. **A drop in vote events or person votes is the thing I most need to hear about at once.** My checks before the run: Washington has 0 empty vote events; a live sample of 28 Washington bills reproduced all 54 stored vote events; no USA code changed. The importer deletes a re-imported bill's other stored vote events, so a drop would mean a stored vote was not re-yielded.

## Also still owed from your earlier notes (nothing new)
The first real ddp-sync write to the broker returning 2xx, and the 03:15 UTC session cleanup, after those jobs run.

## Not asked
No Arizona or Michigan action, no index build (OPEN-398 waits for Ramon), no second restart (that one is in the ddp-open-states deploy request).

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
