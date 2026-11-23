# REPLY (not a GO) to `7dad3a9`: I prefer **option B**, rebuilding ddp-sync at core **`0f3c24c9`**. The go is Ramon's. 2026-11-23 (Mac side, Onboard New States)

**Not a GO.** You've put A and B to Ramon. This note is only my preference and the reasons for it, so he can decide.

## What I need from you
1. **Act on Ramon's choice, not on this note.**
2. If he picks B: rebuild EC2 ddp-sync **at its current commit** with `DDP_CORE_SHA=0f3c24c9d8bdf8766e369273ffd8946b6c6f1ab4`.
    * This is `de80ea4` step 1's ddp-sync part, in ddp-open-states `notes/ops-handoff`.
    * Post the in-flight list first. Never 02:15–04:45 UTC.
    * Before the restart, record the scrapers commit the build cloned (`git -C /opt/openstates-scrapers rev-parse HEAD` in the new image). Expect `14c4e1c4` or later.
    * Then `check-core-pin-drift.py --image-core 0f3c24c9… --sync-container ddp-sync-ddp-sync-1` exits 0.
3. **Report, read-only:** do Maryland's (`md-efa89d747d32`) and Montana's (`mt-da06a3dfea66`) collected outputs survive a failed load, so their load could run again without a new scrape? And the same for Idaho's `id-5f440952e865`. **Don't re-trigger any of the three.** Re-running them needs Ramon's go.

## Why B rather than A
* **A breaks Montana's check.** Montana's re-import needs core `9157b6b5` or later at load time: the "Bill Draft" stage rank, `ce0bf8b` step 1. The 18:19 image is core `939e4a2c`, which loads Montana's versions without that rank.
* **A doesn't hold.** Rolled back, ddp-sync still clones scrapers main on every rebuild, so the next rebuild breaks the same way, until the build is pinned (below).
* **B is already queued, and is the smallest correct state.**
    * `0f3c24c9` contains `58205e01` (where `openstates.utils.tls` comes from), `9157b6b5` (Montana), and the MO, MS and HI rules.
    * I checked on the Mac replica: across all 42,299 stored version names, it differs from `fd486fa4` and `2013b4a0` only for Hawaii.
    * No migration.
* **What B changes in the loader:** certificate checks are on for any request core makes. The loader reads Fargate's output and makes no outbound fetches, so nothing should behave differently. Report any `SSLError` within an hour, as in `3e6c766`.
* **api-v3 on EC2 is not urgent.** It doesn't import the scrapers package. Move it to `0f3c24c9` with the rest of `de80ea4` step 1, on its own go.

## The build pin (your ask 2)
* Confirmed: `infrastructure/Dockerfile` line 79, `ARG OPENSTATES_SCRAPERS_REF=main`, then `git clone --branch "$OPENSTATES_SCRAPERS_REF" --depth 1` (line 92).
* `--branch` won't take a commit SHA, so passing one as a build argument doesn't work today.
* **I'm filing a SYNC ticket now:** the build will take a scrapers commit and check it out, and the release notes will name that commit next to `DDP_CORE_SHA`. It'll come as a PR for review. **Until it is live, name the scrapers commit you cloned in every ddp-sync release report.**

Tickets: OPEN-800 / OPEN-803 (OPEN-146); the SYNC ticket follows.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
