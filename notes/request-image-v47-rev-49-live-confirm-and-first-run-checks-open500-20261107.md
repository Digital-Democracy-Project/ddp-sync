# Request (reading and reporting only): image v47 / revision 49 is registered; confirm it from the next launches and check the first runs of Pennsylvania, Nebraska and Louisiana (OPEN-500, OPEN-494, OPEN-478/479/480). 2026-11-07 about 04:30 UTC (Mac side)

## What I need from you
1. **Confirm the revision from the next launches** (describe-tasks on a launched task, as you did for Washington and Pennsylvania). Ramon registered **`ddp-scrapers:v47`, task-definition revision 49** at about 04:24 UTC (his deploy script printed "tag v47, revision 49"; digest `sha256:704db9eccb1b7046619f9e4df4ef7eee20e6c25cf0115e81689b46c7165f9849`; previous v46 / rev 48). One scrape task was running at that moment (task `22920027f06a...`): tell me which job it was and which revision it runs, so we know the cut-over line. Report the revision of **Nebraska's and Louisiana's launches in Sunday's 02:00 UTC batch** and of **Pennsylvania's 04:00 UTC launch on 11-08**: all should be 49 / v47.
2. **First-run checks (the success criteria a reviewer asked for), for each of Nebraska, Louisiana and Pennsylvania, in your end report:** (a) the container did **not** die with `unexpected keyword argument 'start'`; (b) the log shows the new line `... OPEN-500: ignoring start=... and doing a full walk`; (c) **bills loaded are plausible against the first scrape** (Pennsylvania about 5,270 bills / 5,858 vote events / 425,560 person votes; Nebraska 1,847 bills / 1,772 vote events / 86,723 person votes; Louisiana 2,616 bills / 3,172 vote events / 230,721 person votes; small growth is normal, a collapse is not); (d) **the import ran** (bills_new / bills_updated / bills_noop in the summary) and exit code 0; (e) **no `no objects returned from` line, and no "no changes since cutoff (no-op)" summary**: these runs are labelled `mode=incremental` but are really full walks, so a no-op summary here means an empty scrape was mistaken for "nothing changed" and the watermark advanced; if you see one, **stop and tell me, do not accept it as success**; (f) duration against the first scrape (Pennsylvania about 7 h, Nebraska 68 min, Louisiana 4 h) and any 429, 403 or block; (g) RDS CPU and connections during the Pennsylvania run, and whether it overlaps another job.
3. **Still owed from my earlier request `2f87ce4` (read-only):** the last-run watermark timestamps for pa, ne, la (and whether the failed 04:00 run moved Pennsylvania's); last Sunday's per-jurisdiction results. No hurry beyond Sunday 02:00 UTC.

## What this means
Fix OPEN-500 (openstates-scrapers #81, merge `0cbf0dce5`) is in v47; the Nebraska and Louisiana weekly runs should not crash on Sunday, and Pennsylvania's nightly should run on 11-08 at 04:00 UTC. **The Pennsylvania switch-on request stays on hold** until that run completes cleanly, then I post it.

## Not authorized
Any retry, relaunch, schedule or config change, pull, restart, task-definition registration or deregistration, recompute, or LegBot run. If anything in item 2 looks wrong, stop and tell me; do not retry.

## Reply
`reply-image-v47-first-runs-<date>.md` (Sunday's batch and Pennsylvania's run can be separate replies).

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
