# Reply: Idaho (OPEN-664) and Wyoming (OPEN-662) first scrapes COMPLETED, no repeated-key stop

From DDP Prod Agent, 2026-11-23 05:09 UTC. Rollout and triggers are in `41e6ca9`.

## Wyoming: run wy-d64df4852faf (ddp-scrapers:81)
- Triggered 04:26:57 UTC; collection done 04:35:04; load done 04:36:57 (598 s); archive task done 05:00:02 (1,385 s); `openstates_pipeline: done archived=True failed=0 status=completed duration 2,047.6 s` at 05:01:06 UTC.
- **Counts (RDS, read only):** session 2026 only: **335 bills** (you expected about 335), **1,254 vote events**. Sessions line, fetch errors and archived-document count: not read.

## Idaho: run id-2a1021ebee6f (ddp-scrapers:81)
- Triggered 04:26:54 UTC; collection done 04:45:10; load done 04:47:08 (1,211 s); archive done 05:03:42 (993 s); pipeline `done archived=True failed=0 status=completed duration 2,268.4 s` at 05:04:45 UTC.
- **Counts (RDS):** session 2026 only: **817 bills** (your offline fetch counted 763 bill pages), **853 vote events**. The bill count is above your number; I have not looked into why.

## LegBot hand-off after both archives
Both archives still handed the state to the Mac (EC2's SYNC-186 is not released yet): WY accepted 05:00:06 and ID accepted 05:03:45, and **both ended within a minute with `trigger_failed ... 'legbot_replica_off'`**. So the Mac's own gate (SYNC-186 on the Mac) refuses them quietly: no LegBot work. Neither pipeline failed.

## Not done
No second run of either state. Core used by the loader: ddp-sync's 7f531958 (the rebuild is waiting for Oklahoma's archive).

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
