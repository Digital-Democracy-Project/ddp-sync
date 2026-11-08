# Reply (read-only): Nebraska's rescrape on v49 / revision 51 ended clean and the votes match your prediction exactly (OPEN-504, OPEN-479). 2026-11-08, 19:25 UTC

End report for `359af62` (launch reply was `95b3e03`). Nothing was changed after the launch: no relaunch, retry, pull, restart or registration.

**The run:** run `ne-bd32ce032203`, task `894d5028cff8422e9999d101bf6bdbe7`, **task definition `ddp-scrapers:51`, image v49**, created 18:05:38 UTC, stopped 19:09, **container exit code 0**. ddp-sync: `collection done, loading into RDS` 19:09:18, `cloud_scrape: done` **19:13:08 UTC (4,049.6 s = 67 min)**, of which the import was 3 min 50 s.

## Checks (a) to (g)
- **(a)** no `unexpected keyword argument`; 0 Tracebacks (I read all 14,513 log events, paging the stream).
- **(b)** `18:06:20 INFO openstates: NE OPEN-500: ignoring start=2026-11-08T02:02:44 and doing a full walk` (Sunday's watermark minus one hour).
- **(c)** the scrape saved **1,847 bills and 1,772 vote events** (`bill: 1847`, `vote_event: 1772`); RDS now holds **1,847 bills, 1,772 vote events and 86,723 person votes**, all equal to the first scrape. No bill created or updated since 18:05 UTC; **0 vote events created and 703 updated**.
- **(d)** the import ran (3 min 50 s) and the harness line is `{"source": "ne", "run_id": "ne-bd32ce032203", "mode": "incremental", "status": "ok", "found": 3621, "duration_s": 3634}`; exit 0. (I still cannot read the importer's own new/updated/noop summary; the RDS figures above are what I can see.)
- **(e)** no `no objects returned from`, no "no changes since cutoff (no-op)". The S3 watermark advanced: `ne.ts` `2026-11-08T19:06:53`, count `3621:incremental` (it was `2026-11-08T03:02:44` after Sunday's run).
- **(f)** the scrape took **1:00:33** (task 3,634 s), against 1:00:24 on Sunday and about 68 min on the first scrape. No 429, 403, captcha, Forbidden or retry text of the HTTP kind. **Two warnings in the whole log:** `no session provided, using active sessions: {'109'}`, and **one** `scrapelib: got ('Connection aborted.', RemoteDisconnected('Remote end closed connection without response')) sleeping for 10 seconds before retry` at 18:37:31, after which the walk went on (a single dropped connection, not a block).
- **(g)** RDS (`ddp-openstates`) CPU in 20-minute buckets from 18:00 to 19:20 UTC: average **6%**, maximum **7, 8, 9 and 15%**; connections maximum **13**. Nothing else ran on ddp-sync in the window (no other scrape, archive or job started between 18:05 and 19:15). Separately, I deployed the broker (a different database) from 19:03 to 19:20; nothing on `ddp-openstates` was involved.

## (h) The vote results, the point of the run (Nebraska vote events in RDS, read-only)
| | before (18:04 UTC) | after (19:20 UTC) | your prediction |
|---|---|---|---|
| `pass` | 818 | **1,521** | 1,521 |
| `fail` | 954 | **251** | 251 |
| total | 1,772 | 1,772 | 1,772 |

- **703 vote events were updated by the run, and pass rose by exactly 703.** Only an updated row can change its result, so all 703 went fail to pass and **none went pass to fail** (the arithmetic: net pass change +703 equals the number of updated rows, which leaves no room for a pass-to-fail move; it assumes `updated_at` moves whenever a row is saved). I did **not** keep a per-event list of the "before" results, only the totals and a checksum of (id, result) (`ef533483...` before, `f7dee2ba...` after), so I cannot list individual transitions.
- **Text checks, independent of that argument:** **0 pass events** have a failure word (`failed`, `lost`, `not adopted`, `withdrawn`) in `motion_text`; **0 fail events** contain `adopted`, `prevailed`, `returned to select file` or `readvanced`; **0 fail events** have neither a failure word nor a pass word.
- **The 251 still `fail`** read: `Motion to return to Select File failed` 4, `Failed to advance to Enrollment and Review Initial` 4, `Failed to become law notwithstanding the objection` 2, `Raybould FA27 lost` 2, `Hansen AM1097 lost` 2, then single `... lost` / `... failed` amendments (for example `Brandt MO275 failed`, `Cavanaugh, J. AM474 lost`): all "failed", "lost" or "Failed ...", as you predicted. **`Returned to Select File for specific amendment` (14) now reads `pass`**, and `Advanced to Enrollment and Review Initial` (407) and the `Passed on Final Reading` rows stay `pass`.
- **Nothing to stop on:** no pass-to-fail, and the counts match your prediction to the row.

## Not done (as you said)
No second Nebraska launch; no other scrape, archive or LegBot run; no `backfill-motion-classification` for Nebraska; no recompute; no pull or restart; no change to `replica_routing`, `legbot_replica` or the broker's state list. The switch-on is yours to request.
