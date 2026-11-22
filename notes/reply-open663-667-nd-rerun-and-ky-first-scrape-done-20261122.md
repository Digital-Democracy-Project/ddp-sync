# Reply: North Dakota rerun (OPEN-663/757) and Kentucky first scrape (OPEN-667) both COMPLETED. No repeated-key stop.

From DDP Prod Agent, 2026-11-22 21:50 UTC. Ramon said go in my session (pop-up) for ND, KY, RI and OK, in parallel. Rollout is in `bce6eef`. Checkout `5a0eb41b2` includes #460 (c25b56a1); ky, ri, ok at scraping with cloud_path, archive and people_refresh on, LegBot, routing, search, embedding off.

## North Dakota: run nd-4afc42bc7ff5 (ddp-scrapers:80)
- Triggered 20:11:51 UTC, 202; collection done 20:21:30; load into RDS done 20:25:13 (799 s); archive task done 21:11:52 (2,798 s); `openstates_pipeline: done archived=True failed=0 status=completed duration 5,882.6 s` at 21:49:56 UTC.
- **Vote key fix works:** no repeated key; the load that failed this morning went through.
- **Counts (RDS, read only):** bills 69: 1,089; 69S1: 12; 69S2: 5 (**1,106 total**). Vote events 69: 2,137; 69S1: 23; 69S2: 10 (**2,170 total**). Sessions are the three you expected; I did not read the scraper's "using active sessions" line itself (it is in the Fargate log, not ddp-sync's). Fetch errors and archived-document count: not read.
- Warning seen: `resolve_touched_sessions hit its bill-scan cap ... bills_scanned=500 max_page_seen=56` at 21:11:54 (a distinct session on a later page may have been missed by the hand-off's session list).

## Kentucky: run ky-198bb9b87831 (ddp-scrapers:80)
- Triggered 20:11:45 UTC, 202; collection done 20:50:51; load done 20:53:38 (2,509 s); archive done 21:37:45 (2,648 s); pipeline `done archived=True failed=0 status=completed duration 5,762.0 s` at 21:47:49 UTC.
- **Counts (RDS):** session **2026RS only: 1,737 bills, 886 vote events.** Fetch errors and archived-document count: not read.

## Something you should look at: LegBot hand-offs fired for both
After each archive, the archiver triggered the Mac's LegBot hand-off although legbot_replica is false for ND and KY:
- ND 69: accepted 21:11:54 (legbot-session-96245f1945de); KY 2026RS: accepted 21:37:49 (legbot-session-9be98fdc7803).
- Both ended at **21:47:49 and 21:47:56** with `trigger_failed: the Mac's LegBot run ended without success: 'stopped by a restart: ddp-sync restarted during the run'` (the Mac's ddp-sync restarted then).
- ND 69S1 (12 bills, 0.41 s) and 69S2 (5 bills, 0.26 s) came back `success` almost instantly, which looks like the gate being off, but I cannot see the Mac and have not verified that no LegBot work ran for ND or KY. Please check on the Mac.
- A trigger_failed does not fail the pipeline: both pipelines are completed.

## Not done
No LegBot request from me, no second run of any state. Rhode Island (failed, `9b21ebe`) and Oklahoma (running) are separate.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
