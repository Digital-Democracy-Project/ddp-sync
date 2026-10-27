# Request (read-only): OPEN-437 baseline before the scrapers image goes live (Wednesday 2026-10-28, after 06:00 UTC)

2026-10-27. Ramon's go given in this session. **Read-only: look and report. Do not deploy, trigger a scrape, pause anything or change a config.** If any step would need a write, stop and report.

## Why
OPEN-401's catch-up (openstates-scrapers #73, merge `5a0f60075`) is on fork `main`. Ramon runs `deploy-scrapers-image.sh` from the Mac; registering the new `ddp-scrapers` revision is the cutover and moves every state at once. The canary and the rollback rule compare against what production looks like just before. I need that picture saved first.

## Please report (save the outputs under `/home/bitnami/ops/open437-baseline-<UTC time>/` and name the path)
1. **Live revision:** the latest ACTIVE `ddp-scrapers` revision, its image tag, and the last revision a task actually ran on (previous note: `ddp-scrapers:39`).
2. **Per-state counts** (Virginia, Arizona, Washington, Florida, Michigan, USA, Utah, Massachusetts, North Carolina): bills, vote events, and person votes in the database ddp-sync's runs write to; plus, for Virginia, motion rows with `classification` containing `passage`.
3. **Newest pre-cutover run id** per state in `s3://ddp-openstates-scraper-memory/working-tier/<state>/` (run id, time, object count).
4. **Check-script outputs** you already use for post-run checks (bills/votes drop, duplicate motion rows, Virginia `H VOTE:` text count, Florida House committee votes without members).
5. **Broker confirmations still pending from the deploy:** the first ddp-sync write to the broker after `269cdb29` returned 2xx (the 02:30 / 03:00 UTC jobs on 10-28 will show it, so report again after those), and the 03:15 UTC session cleanup.
6. **In flight now:** any running Fargate scrape/archive task, and the next scheduled launches until Thursday 00:00 UTC.

## Not asked
No canary launch yet (separate request after Ramon's go), no rescrape, no snapshot.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
