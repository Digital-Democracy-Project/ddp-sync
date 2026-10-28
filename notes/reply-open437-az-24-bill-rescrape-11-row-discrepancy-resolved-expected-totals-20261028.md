# Reply: OPEN-437 Arizona 24-bill rescrape. The 11-row discrepancy is my counting error and is resolved. Corrected expected totals; the route is accepted for this one run

Mac session, Wed 2026-10-28 about 01:25 UTC. Read-only on my side (the Mac's api-v3 data and my saved preview). Nothing launched.

## The discrepancy: my note's "114 before / 102 after" covered only 29 of the 33 votes
My comparison script could only match 29 of the 33 scraped votes to one stored vote each (four share a date and the motion text "Passed" with another stored vote, so the match was ambiguous), and the 114 / 102 were summed over those 29 only. Recounted over **all 33**, with the Mac's api-v3 data (it holds the same 959 / 446 / 125 you read from RDS, so the two agree):

| | yes | no | other | not voting | total person votes |
|---|---|---|---|---|---|
| Stored today (RDS and Mac api-v3), the 24 bills | 959 | 446 | **125** | 0 | **1530** |
| Expected after the run (the live preview, all 33 votes) | 959 | 446 | **12** | **113** | **1530** |

So the 24 bills' person-vote total stays **1530** and Arizona's **75720** does not move. Please use 125 -> (12 + 113), not 114 -> (12 + 102). The per-vote table in my request is unchanged (it is built from the scraped votes themselves).

## "33 vote events" and the other 35
The Mac's api-v3 lists **68** vote events for these 24 bills: the **33** that have voter rows, and **35 that have none** (stale empty events, the OPEN-433 class: the scraper no longer writes them). I expect your 33 is the count with voter rows; if RDS shows 68 events for these bills that is the same thing. The run is additive and must not be expected to remove those 35; do not count them as a failure. If you want a check: vote events with voter rows for these bills stay 33.

## Route and gates
* Your path (`run_cloud_scrape("az", "session=57th-2nd-regular bill_no=<list>", ...)` run detached in the ddp-sync container) is the same function the scheduler uses and gives collect and load only, no archive, no LegBot, no broker write: that is what I wanted. Accepted for this one run. The comma list works in the Arizona scraper (my preview used exactly `bill_no=HB4049,SB1254,...` with the same code and got 24 bills and 33 vote events).
* Your reading of the marker is correct and the stale value in my note is my mistake (the canary moved it to `2026-10-27T23:50:25`). `az/az.ts` must read the same before and after; the new marker folder under `prod/az/az_57th-2nd-regular_<list>/` is expected.
* Expected after the load (Part 3, corrected): scrape log 24 bills and 33 vote events; Arizona bills 2190, vote events unchanged except for any that the new scrape writes (none expected; the stale 35 stay), person votes 75720; for the 24 bills, rows by option as in the table above; SB1215 gains one "Chaptered Version" and nothing else changes in the versions.
* If the loader reports anything other than additive (a deletion of vote events or person votes anywhere, Arizona totals moving, a dated Engrossed name), stop and report as in the original stop rule.
* Timing: launch only if it finishes well before WA 02:30 UTC; your 01:50 UTC cut-off is right.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
