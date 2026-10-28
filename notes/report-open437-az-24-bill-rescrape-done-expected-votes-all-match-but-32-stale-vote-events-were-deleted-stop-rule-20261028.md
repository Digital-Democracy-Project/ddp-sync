# Report: OPEN-437 Arizona 24-bill rescrape finished OK. All 33 votes match your table exactly; but the load DELETED 32 stale vote events (Arizona 3443 to 3411), which your stop rule names. Nothing repaired

Prod agent, Wed 2026-10-28 01:36 UTC. Ran on the operator's go in my session, after re-reading all four branches and re-checking the gates (in-flight 0, ddp-sync and broker idle, 01:26, WA at 02:30). Output in `/home/bitnami/ops/az24-rescrape-20261028T0125Z/`. No retry, rollback, repair or restoration was done.

## The run
* Launched **01:26:18 UTC** through `run_cloud_scrape("az", "session=57th-2nd-regular bill_no=<the 24 bills>", "/opt/ddp-open-states", <the openstates_scrape config>)`, detached in the ddp-sync container. Run **`az-ab95b8d4bd4a`**, Fargate task **`222cb5eca8b34e7eba02b3c35a8728a7`**. Finished **01:30:32**; total **251.6 s**, `success: True`.
* Scraper line: `{"source": "az", "run_id": "az-ab95b8d4bd4a", "mode": "full", "status": "ok", "session": "57th-2nd-regular", "found": 61, "duration_s": 172}`. Output: **24 bill, 33 vote**, 3 organization, 1 jurisdiction (62 objects, 354,045 bytes). CloudWatch stream: 0 errors, 0 warnings, 0 timeouts, 0 blocked.
* **Nothing else ran:** no archive, LegBot, knowledge-base or bill-artifact log line, no non-local write to the broker, in-flight empty, ddp-sync healthy.
* **Marker:** `prod/az/az/az.ts` = `2026-10-27T23:50:25` and `az.count` = `899:incremental` both **before and after**, same object time (23:50:57): **unchanged**. The expected new marker folder `az_57th-2nd-regular_<the list>/` appeared (2 small files). Only the source-wide `_lock` object was rewritten (01:29:53).

## What matches your expectations
* **All 33 votes match your per-vote table exactly** (yes / no / other / not voting rows per vote, matched on bill, date, motion and chamber): **33 of 33**.
* The 24 bills' person votes: **yes 959, no 446, other 12, not voting 113, total 1530**. Before: 959 / 446 / 125 / 0 / 1530. Arizona person votes **75,720 (unchanged)**; Arizona by option: yes 50,292 and no 19,114 unchanged, other **6,314 to 6,201** (-113), not voting **0 to 113**.
* Vote events **with voter rows** for the 24 bills: **33**. Version counts for all 24 bills in the scrape output equal your table (including SB1215: 6); **no dated Engrossed version name** appears. (I did not check RDS's version documents, only the scraper output.)
* Arizona bills **2,190** (unchanged).

## What did NOT match, and is the stop-rule case
* **Arizona vote events: 3,443 before, 3,411 after: -32.** For the 24 bills, RDS now holds **36** vote events in total (33 with voters + 3 without); before the run the same bills had **68** (your Mac figure; 3,443 - 3,411 = 32 and 36 + 32 = 68 agree). So **32 of the 35 stale empty events were deleted by the load** (3 empty ones remain). Person votes were not touched (the deleted events had no voter rows). Duplicate groups **249 to 247**.
* Your note said the run is additive and a deletion would be a stop; the loader (`os-update --import`) is therefore **not purely additive** here: re-importing a bill whose vote events changed appears to remove that bill's old vote events that the new scrape no longer writes. I did not look at the deleted rows (they are gone from RDS) and cannot name them. I did **not** restore anything. Whether this is wanted (it is the same class as the OPEN-433 cleanup) or a surprise is Ramon's call; the vote events that remain with voters are exactly the 33 expected.
* Not checked: whether the broker's data (the replica-fed scorecards) changed for these bills, and what the other 3 empty events are.

## State now
Nothing running; in-flight empty; next launches WA 02:30 and USA 03:00 UTC. I will not repeat or extend this run.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
