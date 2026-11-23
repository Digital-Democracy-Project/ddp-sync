# Checkpoint (2026-11-23 18:22 UTC): ddp-sync 85c8888 (SYNC-187) LIVE on EC2. The 32-slot pool works: the repaired runs launched within seconds.

Ramon: "release as soon as the Montana archive task finishes" (pop-up, 11-23). Montana's and Arizona's archives had finished in Fargate (status ok). G1 and G2 were posted first (`bef13a84`). Notes re-fetched at 18:17 and 18:21 UTC.

## Release (UTC)
- `b569cf2` to **`85c8888`** by `merge --ff-only` (first-parent gap was exactly #305); log gzipped to ~/ops/log-archive/ddp-sync-pre-85c8888-20261123T1818Z.log.gz; **rollback tag `ddp-sync:pre-85c8888` = 0b1bf56254d0** (G3); build 18:18:46 to 18:19:32 (new image **a22694d6fb94**, `DDP_CORE_SHA=939e4a2c...`), `up -d --no-deps` 18:19:43 (no systemctl).
- **Checks:** healthy; `Redis connected` 1; **0 Traceback**; **0 `legbot_replica_list_unreadable`**; `GET /schedule` **35 jobs** (28 before plus the seven new states mt, ia, md, hi, me, ms, mo); `check-core-pin-drift.py --image-core 939e4a2c...` **exit 0**; pipeline locks **cleared** (`[]`) at start.
- **Resumed from Redis at 18:19:48** (collection and load carry on): fl-d54af3a05c37, ma-87867254a85b, me-3bf9a26aba24, ms-7f9707a01532, hi-92a72593b894, md-9b5b1f118106, ut-1777beb302dc, la-90b6159abadb.

## Repairs I triggered (Ramon's earlier approvals; one POST each, no retry; all 202)
18:21:09 to 18:21:34 UTC, **all six scrapes launched within 2 to 4 s** (no queue; there is no `cloud_scrape_waiting_for_slot` line): **mo-26c09b0acead** (plain), **nm-8d6c9f49b3a7** (`full=true&sessions=2026`), **id-4e5d74cdef93** (full 2026), **ri-a181b6dac585** (full 2026), **ok-cd11ca3f302a** (full `2025,2026`), **ia-826c2805ac5e** (`?sessions=2026,2026s1`; Iowa 2025 failed on its own at 17:55 with a 60 s read timeout from www.legis.iowa.gov and was **not** retried), all on **ddp-scrapers:85** (v83) from ECS. Plus Arizona's **manual archive** (202) for its LegBot hand-off (its first archive had finished ok in Fargate, ddp-sync never logged it).
- **Running at once now: 14 Fargate tasks** (the old cap was 8).
- **Not triggered, still to do when due:** manual archives for each resumed run when its load ends; **Florida 2026** after its 2025 loads; Nebraska 109 (pre-approved, after Louisiana's load); Montana needs none (archive ok, LegBot off).

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
