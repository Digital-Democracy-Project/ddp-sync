# Status (read-only, no reply needed): Massachusetts' Sunday scrape loaded, but its scrape-triggered archive aborted on connection timeouts. 2026-11-08, 17:20 UTC

Not part of the OPEN-500 checks (Massachusetts was not in your list), but it is a failure on production, so I am telling you. **Nothing was retried or changed**: no relaunch of the archive, no config or schedule change.

## What happened (from ddp-sync's log and the archive task's own CloudWatch log)
- **The scrape was fine:** run `ma-2f0e47217f73` (task `48d1a4a3...`, revision 50 / v48, started 02:00 UTC) finished its collection, `loading into RDS` at 12:23:38 and `cloud_scrape: done` at **12:38:52 UTC, 38,330 s = 10 h 39 min**, inside the 24 h limit (OPEN-476). The watermark advanced: `ma.ts` written 12:22:49, count `12350:incremental`.
- **The archive it triggered failed:** the scrape-triggered archive (`ma-archive-439fe2eb2ec6`, first log line 12:39:34) ran **4,773 s** (4,844 s as ddp-sync saw it) and ended with **exit code 1** at 13:59:36 UTC. ddp-sync logged `openstates_archive: fargate task failed ... exit_code=1` and `scrape_triggered_archive_result ... {'success': False, 'error': 'exit_code_1' ...}`.
- **Why, from the task's last lines:** `ma: aborted -- archive aborted: 5 consecutive connection failures, last fetching https://malegislature.gov/Bills/194/HD178.pdf: HTTPSConnectionPool(host='malegislature.gov', port=443): Max retries exceeded ... (Caused by ConnectTimeoutError ...)`. The log shows repeated connect timeouts of about 2.5 minutes each on `HD172.pdf` and then `HD178.pdf` (13:36 to 13:55), after `ma: heartbeat, 3183 bills processed so far` at 13:43. Result line: `{"source": "ma", "run_id": "ma-archive-439fe2eb2ec6", "status": "failed", "duration_s": 4773}`.
- **A second warning:** `openstates_scrape: no usable filing figures for a successful run — expected marker missing, unreadable, or unparsed expected_path=/opt/ddp-open-states/logs/last-run/ma.imported jurisdiction=ma`.

## What I know and do not know
- **Known:** the archive stopped partway (3,183 bills processed at 13:43; I did not count how many documents it saved before aborting) because `malegislature.gov` stopped accepting connections from the Fargate task for the last stretch. The scrape itself had run for over 10 hours against the same site.
- **Not known:** why the site timed out (a block or rate limit after the 10-hour scrape, a site problem, or something on our network path): I have **no evidence for any cause** and did not test the site from elsewhere. I did not read which documents were already saved.
- Massachusetts' regular weekly archive is **Friday 05:00 UTC** (not scheduled sooner); this scrape-triggered one is the only one that failed. I will not launch another archive without a request and the user's go (the same rule as the Nebraska and Pennsylvania trials: one launch, never retried).

## What I can do if you want it
Read how many Massachusetts documents the archive saved before it aborted, and compare with the previous archive; read the same stream for earlier timeouts. Both read-only.
