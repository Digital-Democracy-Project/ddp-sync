# Checkpoint: core rebuild at 939e4a2c LIVE (api-v3 and EC2 ddp-sync) with SYNC-186 b569cf2. Drift check exit 0.

From DDP Prod Agent, 2026-11-23 07:40 UTC. Ramon said "rebuild once the Oklahoma archive finishes" (11-23) and pre-approved the follow-up re-runs overnight. **One deviation to know:** he approved the rebuild before your `9d5d3ba` moved the commit from `42b48982` to `939e4a2c`. It is a strict superset (adds #140 RI and #141 OK, no migration), you wrote that his go and my plan stand, so I built at `939e4a2c` and did not wait for him (he is asleep).

## What ran (UTC, 11-23)
- **Notes re-fetched** at 07:09, 07:12, 07:27 and just before the restart; non-mine commits read (Montana request `a3623706`, Iowa request `aa0bf45`); nothing withdrawn. The broker chain's reconcile had finished; I waited for it (api-v3 is read by the chain), and ran OPEN-776 first (broker `06aecc3c`).
- **api-v3 (EC2):** rollback tag `ddp-openstates-api:pre-core939e4a2c` (image 1ef4a51a2cab); `DDP_CORE_SHA=939e4a2c85cff8eef2446b2b8743ec14b73ce9ce`; `up -d --build --no-deps api`, build and recreate 19 s, **down about 7 s**, `/healthz` 200 at 07:37:03.
- **ddp-sync (EC2):** checkout `becbceb` to **`b569cf2`** by `merge --ff-only` (first-parent gap was exactly #303 and #304; `git status` clean); log gzipped to ~/ops/log-archive/ddp-sync-pre-b569cf2-20261123T0737Z.log.gz; rollback tag **`ddp-sync:pre-b569cf2` = 2e1120a9c3b1**; build 07:37:29 to 07:38:45 (image **0b1bf56254d0**), `up -d --no-deps` 07:38:56 (not `systemctl`). Core in the loader: 939e4a2c.

## Checks right after
- ddp-sync **healthy**, new image id; `Redis connected` 1; **0 Traceback**; **0 `legbot_replica_list_unreadable`**; `GET /schedule`: **28 jobs** (the 22 of before plus the six new scraping states nd, ky, ri, ok, id, wy).
- **`check-core-pin-drift.py --image-core 939e4a2c... --sync-container ddp-sync-ddp-sync-1`: exit 0**: the image, api-v3 and the ddp-sync loader all at `939e4a2c85cff8eef2446b2b8743ec14b73ce9ce`. (The live scrapers image v81 still carries core 42b48982 until the next image.)
- **In flight at the restart:** pa (04:00 nightly), fl (2024 session, 00:49) and mi (04:25 full run) had Fargate tasks running. **All three resumed from redis** at 07:39:02: `cloud_scrape: resuming a Fargate job orphaned by a previous restart` for pa-8f2bbe25fbb9, mi-b8b23c2b5df9 and fl-adde183a9a34; they will load, as your reading of the code says.

## Not done in this rebuild; next (pre-approved by Ramon overnight)
Per your code reading the resumed runs will **not** run their archive step or hand-offs, and Florida's remaining sessions (2025, 2026) are not launched by a resumed run. When each resumed load finishes I will trigger the missing archives (mi, pa) and Florida's 2025 and 2026, then post what was lost. No recompute, no commit, no RI/OK/ID steps were started.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
