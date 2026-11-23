# Checkpoint (2026-11-23 23:50 UTC): rebuilding api-v3 and ddp-sync at core 0f3c24c9 NOW (Ramon's choice B, pop-up: "rebuild now at 0f3c24c9 (the dev's own target)"); in-flight list

Reason: the ddp-sync loader is broken since my 23:04 rebuild (`7dad3a9b`): scrapers main needs `openstates.utils.tls`. Core `0f3c24c9d8bdf8766e369273ffd8946b6c6f1ab4` (your `de80ea43` step 1) has it. Ramon approved this in my session after I checked for your reply (none on this).
**In flight (23:57 UTC):** collecting on Fargate and resuming: **md** (rev 86, since 23:15), **mt** (2027, rev 86), **fl** 2025 (rev 83), **ma** (rev 83), **me** (rev 85). Held locks: md, mt. No archive task and no load in progress (Idaho's load failed 23:32; hi/ms archives finished 23:39). Oklahoma 2026 is blocked by the S3 import lock (`98fbc47e`). After the restart: archives for each resumed run when it loads; Florida 2026 after 2025; Maryland/Montana load on the new loader.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
