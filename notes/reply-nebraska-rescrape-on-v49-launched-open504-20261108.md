# Reply: Nebraska rescrape on v49 launched once, revision confirmed (OPEN-504, OPEN-479). 2026-11-08, 18:06 UTC

Reply to `359af62`: the launch reply. **Launched exactly once; I will not relaunch or retry whatever happens.**

**The user's go (this session, verbatim):** "Yes, launch Nebraska once now, with the stop conditions (Recommended)". I took Ramon's relayed go as information and asked first.

## Gates (right before)
- No ECS task running (0 at 18:05:33 UTC); S3 lock objects for Nebraska (`_lock`, `_import_lock`) both read as **released** (each has `expires_at` one second before `acquired_at`, holder `ne-e0bd0f09a098`, the Sunday run); no ddp-sync job in the last 10 minutes; 18:05 UTC is outside 02:00 to 03:10. Pennsylvania's 04:00 run is 10 hours away.
- Nebraska's watermark before: `ne.ts` `2026-11-08T03:02:44`.

## Launch
- **18:05:36 UTC:** `POST /ddp-sync/v1/trigger/openstates-scrape/ne` from inside the ddp-sync container with its own API key (not printed): **HTTP 200, `{"status":"started","target":"ne"}`**. One call.
- ddp-sync: `cloud-owned -- triggering Fargate collection + RDS load (OPEN-193) jurisdiction=ne`; run **`ne-bd32ce032203`**.
- **Task `894d5028cff8422e9999d101bf6bdbe7`, created 18:05:38 UTC, task definition `ddp-scrapers:51`, image `ddp-scrapers:v49`** (read with `describe-tasks`): **the revision is the one you expected.**

## (h) before-counts: Nebraska vote events in RDS, read-only, taken before the trigger
- **Total 1,772; `result`: `pass` 818, `fail` 954** (exactly your expectation; the Mac replica's numbers).
- Fingerprint of `(vote event id : result)` for all 1,772 (md5): `ef533483b822b66546b99537d1d4d8b2`, so the "after" can be compared row by row.
- Of the 954 `fail`: **702** have `motion_text` containing adopted, prevailed or "Returned to Select File" (what the new rule should flip; you predict 703, which includes one "Readvanced ..." that my pattern does not look for).
- Top `motion_text` among the current fails: `Returned to Select File for specific amendment` 14, `Adopted` 7, `Motion to return to Select File failed` 4, `Failed to advance to Enrollment and Review Initial` 4, `Hansen AM1097 lost` 2, `Raybould FA27 lost` 2, `Failed to become law notwithstanding the objection` 2, then single `... adopted` amendments (for example `Agriculture AM2886 adopted`).

## Next
The end report when it finishes (the last two runs took 68 min and 1 h 08 min including the import): checks (a) to (g) and the (h) after-counts against your prediction (1,521 pass / 251 fail, none from pass to fail). **Stop conditions I am watching:** a non-zero exit, any 429/403/WAF text, a failed load, or the run still going at 3 hours (21:05 UTC). In each case I report and do not relaunch.
