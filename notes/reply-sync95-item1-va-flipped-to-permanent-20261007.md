# SYNC-95 item 1, part 1: after the VA archive, VA's 4,382 records are permanent (prod agent, 2026-10-07 13:30 UTC)

Part of my reply `reply-evidence-to-close-sync95-open320-open332-broker170-20261007.md` (item 1 was left open for the archives). Read-only: a Redis TTL scan plus a read-only database lookup to map bill ids to jurisdictions; no values printed. **MI (10-08) and FL (10-11 scrape, 10-12 archive) are still to come.**

## The VA pass (ddp-sync log, 2026-10-07)
- The scheduled job `OpenStates: bill-document archive (va)` started at **05:00:00 UTC**. The Fargate archive task finished in **152.0 s** (05:02:32). `archiver_triggered_legbot_no_sessions_touched` (no new documents were archived, so LegBot was not triggered, as designed) and the search refresh found nothing to refresh (`calls=1 drained=True refreshed=0`).
- The knowledge-base hook then ran its first ledger pass for VA, finishing **05:13:50**, about **11 min 18 s** after the archive task (the whole job took 13 min 50 s): `knowledge_base_embedding_run bills=4382 chunks=0 complete=True diffs=0 documents=0 failed_bills=0 jurisdiction=va`, `mode=ledger orphans=0 orphans_over_budget=0`, `ledger={'listed': 4382, 'listing_complete': True, 'bills_to_check': 4382, 'selected': 4382, 'missing_documents': 0, 'changed_documents': 12254, 'orphaned_documents': 0, 'failed': 0}`. So it restamped 12,254 documents (the first-pass cost) and wrote nothing: no embedding calls expected, none made. My estimate from the earlier passes (0.15 to 0.19 s per bill) predicted about 11 to 14 minutes for VA's 4,382 bills; the pass took about 11.3.

## Redis `ddp:bill_version:*` re-scan at 13:20 UTC (66,539 records)
| | permanent | still expiring |
|---|---|---|
| US | 38,623 | 0 |
| VA | **4,382** (was expiring, about 87 days) | 0 |
| WA | 3,411 | 0 |
| AZ | 2,190 | 0 |
| UT | 1,021 | 0 |
| organizations | 5,121 | 0 |
| FL | 0 | **7,684** (about 86.3 to 86.5 days left) |
| MI | 0 | **4,107** (about 86.5 to 86.6 days left) |

- **Totals: 54,748 permanent, 11,791 still expiring** (all FL and MI); every record mapped to a jurisdiction (0 unmapped). The earlier scan (10-06 21:30 UTC) had 16,173 expiring; minus VA's 4,382 gives 11,791, so nothing else moved. US gained 51 permanent records from newly scraped bills.
- Redis is still `maxmemory 0`, policy `noeviction`.

## Next
I will re-scan after the MI archive (10-08) and again after FL's (10-12) and report the same table plus each pass line and duration. FL's first pass is estimated at about 22 to 55 minutes (see item 2 of my earlier reply).

Reply on this branch either way.
