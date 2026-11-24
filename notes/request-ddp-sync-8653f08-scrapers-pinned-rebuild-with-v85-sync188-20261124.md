# REQUEST (waits on Ramon's go): rebuild EC2 ddp-sync at **`8653f08`** (SYNC-188), with the scrapers named by commit: **`DDP_SCRAPERS_SHA` = v85's scrapers commit**, and `DDP_CORE_SHA=0f3c24c9…`. Do it with the v85 release. **From now on, every ddp-sync build needs `DDP_SCRAPERS_SHA`.** 2026-11-24 (Mac side, Onboard New States)

**Not a GO.** The rebuild is yours, after Ramon's go in your session or a GO note.

## What I need from you
1. **Read the new rule now, even before any rebuild.**
    * ddp-sync main at `8653f08` (#306) refuses to build without `DDP_SCRAPERS_SHA`, a full 40-character scrapers commit. `main` is refused too.
    * If you ever pull `/opt/ddp-sync` past `85c8888`, every build needs it.
2. **When v85 is built (ddp-open-states `de80ea4` step 2), rebuild EC2 ddp-sync**, as for `85c8888`/`0f3c24c9` (`692a2ba`, `89ae7ab`):
    * `git -C /opt/ddp-sync merge --ff-only origin/main`. Expect **`8653f08`**: `85c8888..8653f08` is exactly #306 (`infrastructure/Dockerfile`, the compose build args, `primitives.md`, two test files). No `.env`, yaml, schedule or requirements change.
    * Build with:
        * `DDP_CORE_SHA=0f3c24c9d8bdf8766e369273ffd8946b6c6f1ab4` (unchanged);
        * `DDP_SCRAPERS_SHA=<v85's scrapers commit, full>`. If Ramon builds v85 at scrapers main as it is now, that's **`14c4e1c40b1cda93df0c7c993a8bbed931d7291a`**. Use the commit his v85 build reports; the RUNBOOK snippet in ddp-open-states #478 proves it by comparing files.
    * The build runs ddp-open-states' `check-scraper-imports.sh` (from `/opt/ddp-open-states`, the existing `openstates` build context) on the core + scrapers pair. **A failure there stops the build. Report it; don't work around it.**
    * Same gates as before:
        * tag the running image as a rollback point;
        * post the in-flight list first;
        * `up -d --no-deps`;
        * never 02:15–04:45 UTC.
3. **Checks after (report each):**
    * healthy, `Redis connected`, 0 `Traceback`;
    * `cat /opt/openstates-scrapers/DDP_SCRAPERS_COMMIT` in the container equals the commit you passed;
    * `check-core-pin-drift.py --image-core 0f3c24c9… --sync-container ddp-sync-ddp-sync-1` exits 0. The scrapers line appears only after ddp-open-states #478 merges; the PM merges #478 after your report.
4. **If a ddp-sync rebuild is needed before v85 exists** (for example another outage):
    * use `8653f08` with `DDP_SCRAPERS_SHA=14c4e1c40b1cda93df0c7c993a8bbed931d7291a` and core `0f3c24c9…`;
    * or stay on `85c8888`, whose build still clones scrapers main; report the scrapers files you got, as in `89ae7ab`.
    * Either way, on Ramon's go.

## Why
* `7dad3a9`: the 23:04 build cloned scrapers main, which needed a newer core, and every load failed until `89ae7ab`.
* SYNC-188 makes the pair explicit and checked at build time.
* The builder tested both pairs on the Mac:
    * core `9157b6b5` + scrapers `a121b5dd` (the 11-23 pair) fails at the import check: 52 of 56 states;
    * core `0f3c24c9` + the same scrapers builds.

## Rollback
* `git -C /opt/ddp-sync checkout --detach 85c8888`, re-tag the rollback image, then `up -d --no-deps`.
* ddp-open-states #478 stays unmerged until your report, so the drift check doesn't expect a record the old image lacks.

## Not authorized
* Any rebuild before Ramon's go.
* Building without `DDP_SCRAPERS_SHA`, or with a branch name.
* Any scrape, re-trigger, yaml, `.env` or schedule change.

Tickets: SYNC-188 (ddp-sync #306 merged as `8653f08`; ddp-open-states #478 held); OPEN-146.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
