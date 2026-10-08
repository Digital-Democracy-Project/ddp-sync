# MI's archive and ledger pass are done; **no record is expiring any more** (SYNC-95, prod agent, 2026-10-08 15:40 UTC)

Answers section 5 of `request-run-fl-archive-early-and-rescan-fl-mi-records-for-sync95-20261007.md`. **Read-only; nothing was triggered, restarted or changed.** Names and counts only.

## 1. MI's own run (the schedule ran it, Thursday 05:00 UTC)
From ddp-sync's log:
- Archive task: `openstates_archive: fargate task done duration_seconds=182.0 jurisdiction=mi` at 05:03:02.
- LegBot hook: `archiver_triggered_legbot_no_sessions_touched` (since 05:00:00): the archive touched no sessions, so LegBot did not fire.
- Search refresh: `bill_search_refresh_run calls=1 drained=True jurisdiction=mi orphans_removed=0 refreshed=0 with_text=0`.
- **Ledger pass, finished 05:14:14** (about 11 minutes after the archive):
  `knowledge_base_embedding_run bills=4107 chunks=0 complete=True diffs=0 documents=0 failed_bills=0 jurisdiction=mi ledger={'listed': 4107, 'listing_complete': True, 'bills_to_check': 4107, 'selected': 4107, 'missing_documents': 0, 'changed_documents': 7988, 'orphaned_documents': 0, 'failed': 0} mode=ledger orphans=0 orphans_over_budget=0`.
  So it restamped 7,988 documents and **embedded nothing** (`documents=0`, `chunks=0`), the same first-pass behaviour as FL.

## 2. Re-scan of `ddp:bill_version:*` (Redis db 3, 15:38 UTC, every key's expiry read)
| kind | permanent | still expiring |
|---|---|---|
| bills | **61,562** | **0** |
| organizations | 5,121 | 0 |
| **all** | **66,683** | **0** |
**0 records have an expiry now, and none are missing.** (The expected result: MI's 4,107 went permanent.) At the 10-07 23:21 scan there were 66,539 records (57,311 bills + 5,121 organizations + the 4,107 MI ones still expiring), so **144 more bill records exist now**, written by the two US passes below. I did not break the totals down by jurisdiction this time: the keys carry only the bill id, and with 0 expiring there was nothing to map. Redis is still `noeviction` and uses 1.47 GB.

## 3. Pinecone
`ddp-knowledge-base`, namespace `default`: **431,885 vectors** (it was 431,380 at 22:49 on 10-07; **+505**). The +505 is fully explained by two US ledger passes after the US archive: `jurisdiction=us bills=81 chunks=239 documents=81` (04:26) and `bills=68 chunks=266 documents=68` (04:49): 239 + 266 = **505**. MI added none. (Other passes in the window: WA at 03:31, `bills=0`, nothing new.)

## 4. Health (15:40 UTC)
ddp-sync, VoteBot and api-v3 containers `healthy`; broker `/api/status/` 200; ddp-sync 200. In ddp-sync's log since 04:30 UTC there is **one** warning or error, below.

## One thing I noticed, not asked (not investigated)
`2026-10-08 04:44:54 [error] archiver_triggered_legbot_wireguard_trigger_failed error="Client error '409 Conflict' for url 'http://10.0.0.8:8001/ddp-sync/v1/trigger/bill-artifact-generation" jurisdiction=US session_code=119`, right after the US archive (845 s). A 409 from the other host's LegBot trigger usually means a generation run is already going there (I had left a US/119 full-session run in flight there on 09-19, with no result recorded), but I have not checked, so I do not know the cause. A US ledger pass at 04:49 ran fine after it.

## SYNC-95
By this scan, FL, VA and MI are all permanent and nothing in the knowledge base carries a 90-day expiry. The alert threshold and the yaml are not mine to touch, as before. Nothing else changed.
