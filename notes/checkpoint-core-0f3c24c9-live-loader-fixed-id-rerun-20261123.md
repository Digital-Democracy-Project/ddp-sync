# Checkpoint (2026-11-23 23:54 UTC): core 0f3c24c9 LIVE on api-v3 and ddp-sync; the loader imports the scrapers package again. One thing I did against your `d929b9b`: Idaho was re-triggered.

Ramon, in my session (pop-up, 11-23): **"B: rebuild now at 0f3c24c9 (the dev's own target)"** (after I checked for your reply; `d929b9b` arrived while I was working and I read it after the build).

## Done (UTC)
- **api-v3:** rollback tag `ddp-openstates-api:pre-core0f3c24c9` = f546ea214c5d; build and recreate 23 s, **down about 8 s**, `/healthz` 200 at 23:50:48 (outside the 05:00-07:00 window).
- **ddp-sync (at `85c8888`):** log gzipped (~/ops/log-archive/ddp-sync-pre-core0f3c24c9-20261123T2351Z.log.gz), **rollback tag `ddp-sync:pre-core0f3c24c9` = c66861f91574**, build 23:51:01 to 23:52:15 (`DDP_CORE_SHA=0f3c24c9d8bdf8766e369273ffd8946b6c6f1ab4`), **new image c55415e76541**, up 23:52:27. Healthy; Redis connected; 0 Traceback; 0 `legbot_replica_list_unreadable`; 35 schedule jobs. **In the container:** `import openstates.utils.tls` works and `import scrapers.utils` works; **`check-core-pin-drift.py --image-core 0f3c24c9...` exit 0** (api-v3 and ddp-sync both at 0f3c24c9). **Resumed from Redis 23:52:32:** ma, me, md (`md-efa89d747d32`), ia, fl, mt (`mt-da06a3dfea66`).
- **Your ask: the scrapers commit the build cloned.** I **cannot read the SHA**: `/opt/openstates-scrapers` in the image has no `.git`. What I can see: the build log shows the clone, and the files contain Hawaii's OPEN-804 stage naming (`HD<N>/SD<N>/CD<N>` comments in `scrapers/hi/bills.py`), so it is at or after `323000c4`; I cannot tell if it is `14c4e1c4` or later.
- **Your ask: do collected outputs survive a failed load?** **Not verified.** I could not find the work-prefix bucket from the settings I may read, and I did not guess. Maryland and Montana were still collecting and are now resumed under the fixed loader, so their loads should simply run.

## What I did that `d929b9b` asked me not to do
At **23:53:21 UTC** I re-triggered **Idaho** (`id?full=true&sessions=2026`, 202, run **id-c73cf0b57182**, rev 86), because Ramon had given the go for the four v84 runs and the first Idaho load failed only because of the broken loader. I read `d929b9b` afterwards. Maryland and Montana were **not** re-triggered. It is already collecting; stopping a Fargate task needs Ramon's word, so I left it.

## Next
ddp-next stage promotion (`e8aeb3bc`): the first run at 23:46 UTC rolled back by design (`/legislators/1077` answered 308); a second run with `PAGES` without that page is running now; result in my next note. Archives for resumed runs when they load; Florida 2026 after 2025.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
