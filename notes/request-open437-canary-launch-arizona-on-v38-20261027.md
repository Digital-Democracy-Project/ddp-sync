# Request: OPEN-437 canary. Launch ONE Arizona scrape by hand on the new image (v38 / revision 41), then report. Plus a read-only look at the cadence review

2026-10-27 about 22:25 UTC. Ramon said "proceed" in this session for exactly this launch (Arizona only). **This is the only write: the one launch. Nothing else is authorised.**

## Context
`v38` was registered as `ddp-scrapers` revision 41 at 22:06 UTC (previous: revision 40 on `v37`). Your baseline (81f191e) is the comparison. Arizona is the canary because it exercises #71, #78 and #79 (the NV "not voting" label, the excused count, undated Engrossed version names) and is not Michigan.

## 1. Cadence review: read only, do this FIRST
Which states has the dynamic-cadence review currently escalated to nightly, and would the 00:30 UTC review (or the 02:30 WA / 03:00 USA daily jobs) launch anything tonight? Name the states and times. Do not change anything.

## 2. Launch the canary (one call)
`POST /ddp-sync/v1/trigger/openstates-scrape/az` (the manual route from your pause-switch reply). Same incremental mode a scheduled Arizona run uses; no `bill_no=` list, no full rescrape flag. Post the response and the time.
**Do not launch any other state. If a task for any state is already running or the call returns anything but an accepted launch, stop and report; do not retry.**

## 3. Report (read-only after the launch)
1. Fargate task id, and which task-definition revision/image it ran on if you can see it anywhere (ddp-sync log, the task's log stream header); if you cannot read it, say so (I will check it from the Mac).
2. The scrape log: start/end time, duration, bills scraped, errors/warnings (WAF, timeouts, tracebacks), the archive step if it runs.
3. After it finishes, re-run the baseline queries for Arizona only: bills, vote events, person votes, duplicate groups, and the new working-tier run id with object count.
4. Arizona person votes with option `not voting` after the run (new on v38; before: counted as `other`) versus `other`.

## Stop rule (report, never roll back)
Stop and report at once, and do not launch anything else, if: the launch is rejected or the task fails to start; the task exits non-zero; the log shows a traceback or a block; Arizona bills fall by more than 2% or vote events by more than 5% against the baseline; or any other scrape starts while the canary is running. **Do not deregister the revision and do not pause ddp-sync yourself: Ramon decides.**

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
