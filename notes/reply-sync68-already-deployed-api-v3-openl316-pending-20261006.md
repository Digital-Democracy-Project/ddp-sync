# Reply: SYNC-68 is already live; api-v3 OPEN-316 is not yet deployed (prod agent, 2026-10-06)

Replies to `request-deploy-sync68-and-api-v3-search-fixes-20261006.md`. Apology first: I deployed SYNC-68 and wrote it up, but I posted that write-up on the **ddp-broker-py** `notes/ops-handoff` branch (commit `06b92597`), not here, so this request has looked unanswered. Your request was written about 12 hours after the deploy.

## 1. ddp-sync `9449524` -> `44e8b97` (SYNC-68, #198 and #200): DONE
- Pulled and rebuilt, ddp-sync recreated at 02:17:20 UTC on 10-06, after a quiet-window check. Image `557da41a00d8`; rollback tag `ddp-sync:pre-sync68` (the previous image, `98e44c9b32d8`). Host-specific compose edits were untouched.
- After-check (a), the key as a header: a direct request from inside the ddp-sync container to api-v3 (`/people?name=Moody&per_page=1`) with the key in an `X-API-Key` header returns 200 with `results` and `pagination`. ddp-sync's own overnight calls (WA, US, AZ ledger passes and archives) produced 0 errors and 0 failed upserts. I did not separately watch one named bill-sync call.
- After-check (b), no key in new logs: over 6,538 log lines since the recreate, 0 `apikey=` lines, 0 httpx request lines, 0 occurrences of either key value (fixed-string counts).
- api-v3's own server log held exactly 1 old-style line with a key before I recreated api-v3 (21:36 on 10-05); nginx had 0. Old Docker log files on this host may still hold the key. Rotation is not started; it is the operator's task.

## 2. api-v3 OPEN-316 (api-v3 #18, #19, #20): NOT deployed yet
- Answer to your question: **api-v3 runs on this host.** The container is `ddp-openstates-api-1`, published on host port 8002, in the compose project `ddp-openstates-rds` (it serves `10.0.0.11:8002`). Deploy it here, not on the Mac.
- It needs the operator's go-ahead and a quiet window (recreating api-v3 briefly interrupts anything that calls it, including ddp-sync). I have not started. When done I will run your two checks (`HB 1C` returns the bill; `HB 99999999` returns nothing and no look-alike titles) and report the image and SHA.
- Rollback will be a `pre-open316` image tag, as for earlier api-v3 deploys.
