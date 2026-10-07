# Florida's archive run early: done; FL's 7,684 records are permanent; MI is the only one still expiring (SYNC-95) (prod agent, 2026-10-07 23:30 UTC)

Replies to `request-run-fl-archive-early-and-rescan-fl-mi-records-for-sync95-20261007.md` (and its correction). **One request was sent, once. Nothing was restarted or changed** (no setting, schedule, yaml or compose edit). Names and counts only; no secret values.

## 1. Quiet-window checks (22:49 UTC, before the request)
ddp-sync image **`4ca4105dff58`**, `HEAD` **`faa9630`** (as you expected); **`LEGBOT_SCRAPE_COMPLETION_TRIGGER_ENABLED` is `true`**; 0 backfill locks; every `ddp:flow:*` entry complete (FL scrapes, WA, USA, people refresh `completed`; secondary scrapes `completed_with_errors` from 10-04; `patch_refresh` `failed` from 10-05, before it was turned off); 0 non-health log lines in 30 minutes; nothing scheduled before 00:30 UTC. ECS tasks cannot be listed from this host's role (`ecs:ListClusters` denied), as before. Broker, ddp-sync, api-v3 and VoteBot healthy. Pinecone `ddp-knowledge-base` **before: 431,380 vectors** (namespace `default`; it was 431,287 on 10-06, the 93 more came from the US scrapes of 10-07).

## 2. The request
`POST /ddp-sync/v1/trigger/openstates-archive/fl` at **22:49:26 UTC**, with this instance's key from the container's environment (never printed). **It answered HTTP 200 `{"status":"started","target":"fl"}`, not 202**: the route's decorator carries no explicit 202 (I read the code), so your "202" expectation is off by that. I did not send it twice. The flow did start: a new Fargate log stream appeared at **22:50:06**, and the log lines below followed. (`ddp:flow:openstates_archive` stayed empty throughout, so that key cannot be used to watch this route.)

## 3. What it did
- **Archive task:** `openstates_archive: fargate task done duration_seconds=121.7 jurisdiction=fl` at 22:51:28.
- **LegBot hook:** **`archiver_triggered_legbot_no_sessions_touched`** (`since=2026-10-07T22:49:26`): the archive touched no sessions, so LegBot did not fire.
- **Search refresh:** `bill_search_refresh_run calls=1 drained=True jurisdiction=fl orphans_removed=0 refreshed=0 with_text=0`.
- **Knowledge-base ledger pass, finished 23:08:33 (about 17 minutes 2 seconds after it started at 22:51:31; 19 min 7 s from the request):**
  `knowledge_base_embedding_run bills=7684 chunks=0 complete=True diffs=0 documents=0 failed_bills=0 jurisdiction=fl ledger={'listed': 7684, 'listing_complete': True, 'bills_to_check': 7684, 'selected': 7684, 'missing_documents': 0, 'changed_documents': 13491, 'orphaned_documents': 0, 'failed': 0} mode=ledger orphans=0 orphans_over_budget=0`.
  So it **restamped 13,491 documents and embedded nothing** (`documents=0`, `chunks=0`): the first-pass behaviour you expected. My estimate for it was 22 to 55 minutes; it took about 17.
- **OpenAI request count: I cannot give a measured number.** ddp-sync's request logging is silenced by SYNC-68 (as intended), so there is no per-request line to count. The proxy is the pass line: **0 documents, 0 chunks and 0 diffs went to the embedder**, so no embedding requests were needed. **Pinecone after: 431,380, unchanged** (before 431,380).
- Health during and after: ddp-sync 200, broker 200, api-v3 200, VoteBot healthy; 0 warnings or errors in ddp-sync's log from the request to now; no locks left behind.

## 4. Re-scan of `ddp:bill_version:*` (23:21 UTC, same method as 10-07 13:30)
| | permanent | still expiring |
|---|---|---|
| US | 38,623 | 0 |
| **FL** | **7,684** (was expiring, about 86 days) | **0** |
| VA | 4,382 | 0 |
| WA | 3,411 | 0 |
| AZ | 2,190 | 0 |
| UT | 1,021 | 0 |
| organizations | 5,121 | 0 |
| **MI** | 0 | **4,107** (86.1 to 86.2 days left) |
**66,539 records in all: 62,432 permanent (57,311 bills plus the 5,121 organization records) and 4,107 expiring, all MI.** Every bill mapped to a jurisdiction (0 unmapped), and **no FL record stayed expiring**. Matches your expectation exactly. (Redis is still `maxmemory 0`, `noeviction`.)

## 5. MI (no action to trigger)
MI's own archive and ledger pass run at 05:00 UTC on 10-08 (Thursday). I will re-scan after its pass completes and report the totals and its pass line; expected 0 still expiring. If you want that to be a prompt after 05:30 UTC, it needs someone to ask me then, since I cannot run on my own.

## Not done, as you asked
No `alert_backlog_over`, `delete_orphans`, `max_bills_per_run` or yaml change; the host's uncommitted compose, service and render-script edits are untouched.

Reply on this branch either way.
