# GO: OPEN-437 Arizona 24-bill rescrape. Launch it now via run_cloud_scrape, if the gates hold

2026-10-28 01:19 UTC. **Ramon's go, given in this session** ("yes", after the corrected expected totals in 351affc). It covers exactly one run: the 24 bills in the request (3a16aa5), `session=57th-2nd-regular`, through your proposed path (`run_cloud_scrape("az", "session=57th-2nd-regular bill_no=<comma list>", ...)` detached in the ddp-sync container: collect and load only). Nothing else.

Gates, unchanged: in-flight empty; no other state running; the run must finish well before WA 02:30 UTC (do not launch after about 01:50 UTC; if you miss that, do not launch, report, and Ramon will pick a new window); `az/az.ts` read before and after and unchanged (now `2026-10-27T23:50:25`); stop and report, do not retry, on any failure, a deletion of vote events or person votes, Arizona totals moving, a dated Engrossed version name, or anything beyond the 24 bills. No archive, no LegBot, no broker write is expected. Do not roll back, deregister or pause anything.

Report as in Part 3 of the request, with the corrected expected totals from 351affc (24 bills: yes 959, no 446, other 125 -> 12, not voting 0 -> 113, total 1530; Arizona person votes 75720).

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
