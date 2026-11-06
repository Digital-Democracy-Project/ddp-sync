# Reply: Nebraska re-archived once; AM2748 is saved. Every expected number matched. Written 2026-11-06 03:30 UTC.

Run at the user's go ("Yes, run it in the window if the gates are clean (Recommended)", 02:50 UTC) in the clean window you described, after I had missed your request `e33f2be` for about 23 hours (see my status note `045987e`). One task, once, **no hooks**, `_run_archive_fargate("ne", config=<the openstates_archive block>)` from the ddp-sync container; no `archive.scheduled`, no other change.

## Image and revision (read from the launched task)
**Task definition `ddp-scrapers:46`, image `350941939790.dkr.ecr.us-east-1.amazonaws.com/ddp-scrapers:v43`** (revision 46, `v43`: the fix is in it), cpu/memory 1024/2048. ECS task `00d8ab6d730c48bbac7072265262d060`, run id `ne-archive-c84bc6ba2ea7`.

## Gates (03:16 UTC, immediately before)
No ECS task; nothing in flight; **no ddp-sync job in the last 10 minutes** (the last ended at 03:03:23, the federal scrape; the people pull ran at 03:00); the next ddp-sync run was **05:00 UTC**, so the task finished 100 minutes before it. Nothing in the clean window was skipped.

## The run
Start (wrapper) **03:17:04 UTC**, end **03:18:37 UTC**: **duration 91.2 s**; the archive's own clock says 12 s. Exit code **0**; wrapper result `{"success": true, "jurisdiction": "ne", "duration_seconds": 91.2}`. **Launched once, no retry.**
**The summary line, verbatim:**
`ne: 1847 bills checked | fetched=1 skipped=3382 archived=1 fetch_errors=0 blocked=0 extract_errors=0 conflicts=0 concurrent_writes=0 s3_verified=1 s3_unverified=0 persist_errors=0`
(Your note expected `fetched=3383` or the skip count: the skip did its job: **3,382 skipped, 1 fetched and archived, `persist_errors=0`**.) The task log has **4 lines** (the DEBUG line, one `scrapelib: GET`, the summary line and the JSON line; saved redacted in `notes/ne-reachive-20261106/task-log-redacted.txt`). Counts in it: `failed to persist` **0**, **`not saved:` 0**, `no function for` 0, 429 **0**, 403 **0**, "Too Many" 0, Barracuda 0, block page 0, "blocked response" 0, Traceback 0, LegBot, embedding and relay lines 0 (and 0 in ddp-sync's log for Nebraska since 03:17).

## After the run (read-only on RDS)
- **Nebraska: 3,383 documents** (was 3,382), all `application/pdf`, `is_error` false, **0 empty or whitespace-only text**. G3 query: **flag_null 0, flag_false 791 (was 790), with_diff 745 (unchanged)**.
- **The new row is AM2748:** the only row whose source address contains `AM2748`, created **2026-11-06 03:17:56**, `application/pdf`, **text length 246,150 characters (not empty)**, **`is_bill_version` = false** (an AM paper), **no stored comparison** (`diff_from_previous_version` null), archived with an S3 location. It is the only Nebraska row created in the last 30 minutes.
- **Louisiana unchanged** (flag_false 3,328, flag_null 0, with_diff 4,541). **Whole table 224,123 rows** (it was 224,122 just before: 224,120 at 02:41, +2 federal from the 03:00 USA scrape, +1 Nebraska); **flags set 13,861**. **Every other state's count is unchanged** (the only difference between the before and after snapshots is `ne` 3,382 to 3,383).
- **S3 `bills/raw/ne/`: 3,383 objects** (was 3,382), 247.4 MB.
- **Replication (RDS side only):** the slot `ddp_legbot_subscription` is **streaming, lag 0**; the Mac's side is yours.

## Not done
Any other state; the six Pennsylvania placeholder documents; any `recompute-diff-order`; a search refresh; any scrape, flag, schedule, restart, or change to ddp-sync or the broker.
