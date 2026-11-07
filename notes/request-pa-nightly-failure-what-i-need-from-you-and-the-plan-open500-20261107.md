# Request (read-only asks now; no production change): Pennsylvania's nightly failure: what I need from you, and the plan (OPEN-500, OPEN-494, OPEN-478). 2026-11-07 about 04:25 UTC (Mac side)

Thank you for the clear failure report (`394c4c2`) and for stopping without a retry. **I found the cause and a fix is being built.** This note asks for read-only things and tells you the plan; nothing in it changes anything.

## What I need from you
1. **Do not retry, relaunch or pause anything for Pennsylvania.** Leave the 04:00 UTC job as it is. It fails in about 93 seconds before saving anything, so tomorrow's 04:00 UTC run (11-08) will fail the same way if no fixed image is registered by then; that is harmless and I would rather not make a production config edit and a ddp-sync restart for it. **If the failure alerts are a nuisance to Ramon, tell me and I will ask him about pausing.**
2. **A read-only check that Sunday will fail for Nebraska and Louisiana too.** Please read, for **pa, ne and la**, the last-run watermark the scrape harness uses (the `last-run` marker, or the S3 scraper-memory marker if that is where it lives: the thing `run-scrape.sh` reads at lines 237-260 to decide full versus incremental) and report the timestamp for each. Expect each to be about 2026-11-03 (their first full scrapes). If either has none, tell me: that would mean it runs full on Sunday and does not crash.
3. **A read-only check of last Sunday's batch.** For the Sunday 11-01 02:00 UTC secondary batch (va, mi, ma, ut, az, nc, and whatever else was in it), report each jurisdiction's result (exit code, and any `unexpected keyword argument` line in the task log). I want to be sure no other state has the same hidden problem.
4. **Read the revision from the next launch as you did today** (describe-tasks), and tell me which image tag the next scheduled scrape runs.

## What happened (so you can check me)
`run-scrape.sh` turns any state with a last-run watermark into an incremental run and adds `start=<last run minus 1 h>`. The Pennsylvania, Nebraska and Louisiana bill scrapers take no `start` argument, so their second run crashes at once with the `TypeError` you read. Pennsylvania's first run (11-03) was full because no watermark existed. The manifest records the fact (`scrape.start_filter: false`) but nothing reads it. Massachusetts and North Carolina avoid it because their scrapers accept `start` and ignore it.

## The plan
- **OPEN-500** (openstates-scrapers): make the pa, ne and la bill scrapers accept and ignore `start`, like Massachusetts. A background build is working on it now. **OPEN-501** (ddp-open-states): make `run-scrape.sh` read `scrape.start_filter` so a new state cannot hit this again (later, not urgent).
- Ramon merges the fix and releases a new image; **target: registered before Sunday 11-08 02:00 UTC**, so Nebraska's and Louisiana's weekly runs and Pennsylvania's 04:00 run are fine. I will post the tag and revision here when it exists and ask you to confirm it from the launch.
- **The Pennsylvania switch-on request is on hold** until a Pennsylvania scrape has completed cleanly on the fixed image (the data on RDS is still the 11-03 scrape). I will post it after that run's end report.

## Not authorized
Any retry or relaunch, schedule or config change, pull, restart, task-definition registration, recompute, or LegBot run.

## Reply
`reply-pa-nightly-failure-readonly-checks-<date>.md` with items 2 and 3 (item 4 when the next launch happens).

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
