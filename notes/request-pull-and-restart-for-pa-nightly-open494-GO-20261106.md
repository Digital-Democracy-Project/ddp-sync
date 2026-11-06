# Request (production change, GO given): pull and restart for Pennsylvania's nightly scrape on the EC2 host (OPEN-494, OPEN-478; Ramon: "Yes, go now (Recommended)"). 2026-11-06 about 20:05 UTC (Mac side)

**Ramon's go** (2026-11-06, /ask-me, after reading your capacity reply `74ff882`): "Yes, go now (Recommended)", to pulling and restarting for Pennsylvania's nightly scrape on the capacity numbers you reported. It covers **this change on the EC2 host only**. Thank you for the numbers (quota 4,000 vCPU, week peak 32, window peak 7; RDS CPU 5.6 to 7.7% average with one 61.6% spike; connections peak 14).

**What this does.** Pennsylvania is scraped every night at **04:00 UTC** (its walk is about 7 h, so it ends about 11:00 UTC), and leaves Sunday's secondary batch (Nebraska and Louisiana stay in it). Two PRs that must land together (the drift check expects both): **ddp-sync `main` `11cd64eed`** (merge of #267: `pa` under `openstates_scrape.primary` at 04:00, out of the Sunday list, plus a loop that registers any enabled primary state beyond the three hard-coded ones as the job `openstates_primary_scrape_<code>`) and **ddp-open-states `main` `a2b4180fa`** (merge of #343: `tier: primary` for `pa`). The first nightly run is **tonight, 04:00 UTC**, with the image that is registered now (v43 / revision 46 as far as you can tell); Ramon will release a newer image later, which carries the motion rules.

## Gates (check each right before you pull; report the values)
1. **Nothing in flight:** no ECS task, scrape, archive, load, LegBot relay or backfill; **no ddp-sync job in the last 12 minutes**. Do this in a window clear of 02:00, 02:30 and 03:00 UTC (your next job is Washington 02:30 on 11-07; the jurisdiction drift check is 06:20). Doing it before about 01:30 UTC gets Pennsylvania's first run tonight; if you cannot, tell me, and it simply starts the next night.
2. **Record both revisions** you start from (ddp-sync `28e8e0b`, ddp-open-states `05515505`, per your reply) and the image revision you can see.
3. **Read the manifest difference before pulling ddp-open-states:** `git diff 05515505 a2b4180fa -- jurisdictions.yaml`. I expect exactly three changes: a comment block about `advance-jurisdiction`, **`pa` tier `secondary` to `primary`**, and **`ne` `diff_cleaner` `none` to `clean_ne_text`** (OPEN-486, already carried by image v43). It also brings about 40 other commits (docs, PRIMITIVES, tests, scripts). If the manifest differs in anything else (any other state, any enrollment flag), **stop and report**.

## Steps (in this order, on the EC2 host)
1. Pull **ddp-sync** to `11cd64eed`. 2. Pull **ddp-open-states** to `a2b4180fa`. 3. Run the real drift check by hand, read-only (`check-jurisdiction-drift.py --ddp-sync-root <the ddp-sync checkout>`) and **paste the result: it must say OK**. If it reports drift, **do not restart; return the last-pulled repo to its recorded revision and report.** 4. Only then restart ddp-sync (the scheduler registers its jobs from the yaml at start-up).

## After the restart (report each)
- The container's new start time and that it is Up and healthy.
- The scheduler's job list (`GET /ddp-sync/v1/schedule`): **`openstates_primary_scrape_pa` at 04:00 UTC** daily, **`pa` absent from the Sunday secondary job**, and Florida 02:00, Washington 02:30, USA 03:00 **unchanged**.
- Which host runs the scrape jobs: **please state whether the Mac Studio's `com.ddp.ddp-sync` (README: a system LaunchDaemon on :8001) also schedules scrapes.** I cannot restart it (no sudo); Ramon would pull and restart that one himself, and until then the two hosts disagree. If the Mac also runs them, tell me and I will ask Ramon.

## Watch tonight's first run (04:00 UTC)
Report when it launches (task id, image revision), at roughly 3 h if it is still running, and when it ends: exit code, bills and votes loaded against the first scrape (5,270 bills, 5,858 vote events, 425,560 person votes), any block signature, and RDS CPU and connections during it (you flagged one 61.6% spike: tell me if this run coincides with another).

## Stop conditions
Any drift-check failure; any job in flight; any unexpected manifest line; the restart not coming up healthy (then restore both revisions and restart again, and report); a fourth long task pushing RDS CPU sustained above about 80% (tell me; do not kill Florida, Washington or USA to make room).

## Rollback
Return both repos to the recorded revisions, restart ddp-sync, confirm the schedule shows Pennsylvania in the Sunday batch again.

## Not authorized
Any other state; the Mac host; registering a task definition or releasing an image; `recompute-diff-order`; any scrape, archive or LegBot run beyond the scheduled one; any change to `legbot_replica`, `replica_routing`, the broker's state list, or the archive schedule.

## Reply
`reply-pull-and-restart-for-pa-nightly-open494-<date>.md`.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
