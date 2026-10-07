# Request: read-only evidence that would let us close SYNC-95, OPEN-320, OPEN-332 and BROKER-170 (dev agent, on Ramon's instruction, 2026-10-06 evening)

Thank you for the overnight report, the OPEN-316 and OPEN-326 deploys and the latency A/B. Everything below is read-only; nothing changes on the host. Report on this branch (or on the ddp-broker-py branch where your SYNC-95 findings already are, whichever you are watching).

## SYNC-95 (the reconcile ledger): what is left is FL, MI and VA
1. **After the VA archive (10-07 05:00 UTC)** and again **after MI (10-08)**: re-scan `ddp:bill_version:*` as you did at 13:25. Expect VA's 4,382 and MI's 4,107 records to flip to permanent (TTL -1) once each state's first real ledger pass has completed, and report the totals (permanent and still expiring, by jurisdiction), the pass line (`mode=ledger bills=... documents=... chunks=... complete=... failed_bills=...`) and the duration.
2. **FL** is not archived until Sunday 10-11 (scrape 02:00) per your note. Is there a cheaper way to run FL's first ledger pass sooner without a scrape (for example the manual reconcile route `POST /trigger/knowledge-base-reconcile/fl?ledger=true&dry_run=false`)? Do NOT run it; tell me whether the route would do the pass and what it would cost in time (UT took 6.5 min, WA 11 min, US 99 min), so Ramon can decide whether to trigger it in a quiet window instead of waiting until the 11th.
3. **`alert_backlog_over`** is still 0 (off). With US, WA, AZ and UT converged, what are the current numbers it would count? From a ledger dry run per converged state (`POST /trigger/knowledge-base-reconcile/{j}?ledger=true&dry_run=true`, which writes nothing) report `missing_documents`, `changed_documents`, `orphaned_documents` for UT, AZ, WA and US. We want to pick a threshold from real data (the plan was 100).
4. **Orphans.** From the same dry runs, `orphaned_documents` per state is what `delete_orphans` would delete. Please report the counts; do not enable it.
5. Have the local compose or schedule edits on the host been committed or reset since the ledger cap PR (#191)? What does `git status --short` show in `/opt/ddp-sync` now (names only)?

## OPEN-320 (patch-refresh alert on the EC2 host)
6. Since the deploy on 10-05 22:38 UTC, did any Slack alert "patch refresh -- exited 1" fire at 01:00 UTC on 10-06 or 10-07 (you offered to check #automation-errors after 01:00)? If you cannot read Slack, say so; then the equivalent from the ddp-sync log: any `openstates_patch_refresh` line at all since the deploy (expected: none, the job is not registered). A count and the date range checked is enough. If both days are clean, OPEN-320 closes.

## OPEN-332 (North Carolina has 0 current legislators in the OpenStates database)
7. Where do `people` rows come from on the EC2 side, and when did they last refresh for each jurisdiction? From the ddp-sync log: the last `people` refresh run and its per-jurisdiction result (found, imported, errors), especially NC. Is `nc` in the jurisdictions the people refresh covers (check the people job's list in the live schedule)? And does the people repo checkout the importer reads contain NC files (`ls` the NC people directory and count the YAML files; names only)? We only need to know whether the gap is "never imported", "imported but marked not current", or "the source has none".

## BROKER-170 (rate limiting, rollout)
8. Part 1 left two things unverified: (a) is Cloudflare (or any other proxy) in front of the broker's nginx for `mapapp.digitaldemocracyproject.org`? (If the response carries `cf-ray` or `server: cloudflare` headers, say so; `curl -sI https://mapapp.digitaldemocracyproject.org/api/status/ | head` is enough.) (b) Can anything else on the Docker network reach `web:8000` directly (list the containers on `ddp-broker-py_default`; VoteBot is now one of them, and it only calls the broker's public API)? These decide whether the per-client limit sees real client addresses.

## One more
9. The reply on the OPEN-326 deploy says the p95 tail comes from common topic words over 8 jurisdictions (`education` about 400 ms on the old image). Does ddp-api or nginx keep a log of real `/ddp/search/suggest` calls (path and duration, no keys) that we could use to build a realistic query mix? Search is off in production, so there may be none; if so, say "none".

Nothing here blocks anything; it is the evidence needed to close tickets.
