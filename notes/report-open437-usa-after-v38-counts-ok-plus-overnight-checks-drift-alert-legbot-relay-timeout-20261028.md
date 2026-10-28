# Report (read-only): OPEN-437 USA after tonight's v38 runs, plus the overnight items owed. Counts fine; two things to look at: the drift check alerted, and the LegBot relay timed out again

Prod agent, Wed 2026-10-28 14:58 UTC (I read these at the first check after the overnight jobs; the Washington part was posted at 02:42 in `3f061fb`). Read-only; nothing launched or changed. Output in `/home/bitnami/ops/open437-wa-usa-20261028T0236Z/`.

## 1. USA (RDS, 14:54 UTC) against your baseline (39,084 / 1,604 / 561,033 / none / 0)
bills **39,097 (+13)**, vote events **1,604**, person votes **561,033**, duplicate groups **0**, vote events with no voters **0**. No drop; the 13 extra bills are consistent with the runs finding new bills. No vote event was touched (latest written 2026-10-17).

## 2. The USA runs (two, lower then upper)
* **`usa-c4d6bbcd1120`** (lower): launched 03:00:00, done 03:05:34 (ddp-sync 333.3 s); scraper line `mode incremental, status ok, session 119, found 207, duration_s 202`. CloudWatch: 0 errors, 0 warnings, 0 timeouts or blocks.
* **`usa-8bbc935b22b5`** (upper): 04:20:01 to 04:21:39 (96.9 s); `found 22, duration_s 21`. CloudWatch: **1 ERROR**: `Unable to parse vote date in https://www.senate.gov/legislative/LIS/roll_call_votes/vote1192/vote_119_2_00195.xml` (04:20:55); 0 warnings, 0 timeouts or blocks. Vote events did not change, so I cannot say whether that vote was skipped or was already stored.
* Previous nights for comparison: found 4, 4, 59, 20 (durations 5 to 57 s). Tonight's lower run found 207 in 202 s, more than any of them.
* `openstates_usa_scrapes: completed duration_seconds=5780.5 failed=0 total=2`.

## 3. The chain after the USA load
* After the lower run: archive done 03:18:07 (753.0 s). **LegBot ran on the Mac for 20 new bills of 119**: 160 artifacts in the broker between **03:19 and 03:27 UTC** (20 bills x 8 types; 20 concept-statement sets): **146 complete, 14 failed**; the 14 are `bill_opposing_orgs` 10, `bill_pros_cons` 3, `bill_supporting_orgs` 1, all `generation / insufficient_information`. BillTopic rows 109,553 to 109,605 (+52), Bills +20.
* **But ddp-sync logged `archiver_triggered_legbot_wireguard_trigger_failed error= jurisdiction=US session_code=119` at 04:18:08, exactly 3,600 s after the call started (03:18:07)**, with an empty error: the same 3,600 s relay-timeout signature as US 10-24 / 10-25 and MA 10-25. The Mac's work was in the broker by 03:27, so the work was not lost; the relay still waited the full hour.
* Knowledge base: 24 bills, 60 chunks, `failed_bills=0`. After the upper run: archive 785.0 s, `archiver_triggered_legbot_no_sessions_touched`, KB 0 bills; all `success True`.

## 4. Owed from before
* **First real ddp-sync write to the broker after 269cdb29: confirmed 2xx.** In the broker's nginx log (13 h): **17,623 `POST /api/bills/ensure/` 200, 20 `bills/ensure` 201 plus 160 `bill-artifacts` 201 and 20 `concept-statement-sets` 201, from the VPN peers; no 5xx on any write.** I cannot tell the EC2 ddp-sync's own writes apart from the Mac's in that log. nginx's only 5xx in 13 h are two outside scanner requests (`/geoip`, `/geoserver/web/`).
* **03:15 UTC session cleanup: ran**, `clear_expired_sessions` succeeded in 0.14 s (the task logs `None`, so no deleted count is logged).
* **Drift check (first run 06:20:00 UTC): `status=drift alerted=True drift=1`**, with `env_lists_not_set_on_this_host=['LEGBOT_RDS_REPLICA_JURISDICTION_ALLOWLIST']`; `jurisdiction_manifest_agrees` just before it. So it **did send an alert**, and the finding is that this host has no `LEGBOT_RDS_REPLICA_JURISDICTION_ALLOWLIST` set, not the MA difference you expected. I did not look at the Slack message. Whether the variable should be set here is your call.
* Broker web log: 236 `Invalid HTTP_HOST` rejections from scanners (118 tracebacks), nothing else.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
