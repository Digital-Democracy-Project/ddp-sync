# Request: re-archive Nebraska once, to save the one document the trial lost (AM2748; OPEN-487), 2026-11-05

**Ramon (Mac session, 2026-11-05): "i think you can do all those tasks yourself, right?"**, said about the open steps after the Nebraska recompute, including this re-archive. This is the request for it. Nothing else is asked.

## Why
Your Nebraska trial report `2df6b64` (11-04): 3,382 of 3,383 saved; `AM2748` was fetched but not saved (`Errno 36 File name too long`), `persist_errors=1`, exit 0. The fix is merged (openstates-core #75: a too-long name is shortened, and every unsaved document is now named in the log) and is in **`ddp-scrapers:v43`, task-definition revision 46** (Ramon registered it 11-05 about 00:40 UTC). Revision 46 has now run on a real scheduled task: the 03:14 UTC federal archive saved 8 documents, each already carrying the new `is_bill_version` value, which only v43's code writes (read by me on the Mac's copy; your reading in `54c2b04` is right).

## What to run (the same method as the 11-04 trial)
One archive task for Nebraska only, once, **no hooks**: `_run_archive_fargate("ne", config=<the openstates_archive block>)` from inside the ddp-sync container, as in `32a108b`. Do not set `archive.scheduled`. **Report which image tag and task-definition revision the task actually ran** (it must be v43 / revision 46; if it launched on revision 45 or older, stop and report, because the fix would not be in it).

## Gates
No ECS task, nothing in flight, no ddp-sync job in the last 10 minutes, and clear of the **05:00 UTC** archive (Thursday: Michigan and North Carolina) and the 02:30 and 03:00 UTC runs; the trial took 58 minutes for 3,382 documents, but the natural-key skip should make this run short, so a 03:55 to 04:45 UTC window is plenty. Stop and report if a gate fails.

## What I expect (tell me if not)
* The summary line shows `fetched=3383` (or the skip count your reading of the code gives) with **`archived=1`** and **`persist_errors=0`**; 0 `failed to persist` lines; 0 lines of the new `not saved:` kind; 0 blocked; 0 extract errors.
* Afterwards `ddp_bill_version_document` for Nebraska: **3,383 rows** (was 3,382); the new row is `AM2748`, `application/pdf`, text not empty, **`is_bill_version = false`** (it is an AM paper) and no stored comparison; Nebraska **flag_false = 791, flag_null = 0**, `with_diff` still **745**; every other state's row count unchanged (total 224,121 if nothing else is archived meanwhile; the federal scrape may add some); S3 `bills/raw/ne/` 3,383 objects.
* The 3,382 existing rows are not rewritten (the archive's skip leaves them as they are).

## Not authorized
Any other state; the six Pennsylvania placeholder documents (a separate, later retry if wanted); any `recompute-diff-order`; a search refresh; any scrape, flag, schedule, restart, or change to ddp-sync or the broker.

## Reply
`reply-nebraska-reachive-am2748-20261105.md` with the summary line verbatim, the image tag and revision, the after counts above, S3 count, and the start and end times (UTC). I check the Mac's copy afterwards (worker, lag, 3,383 and 791) and report.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
