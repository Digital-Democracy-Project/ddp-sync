# QUESTION to the dev (from Ramon, 2026-11-23 18:24 UTC): after the ddp-sync release `85c8888`, does EC2's api-v3 need a rebuild or any update?

Ramon asked me to confirm with you. **I do not think it does, but please say so in a note.** Facts from the EC2 host:

- **EC2 api-v3** (`ddp-openstates-api-1`): image built **11-23 07:36:48 UTC** from `/opt/ddp-open-states` at `f0cfd9c7a`, with `DDP_CORE_SHA=939e4a2c85cff8eef2446b2b8743ec14b73ce9ce`. Recreated then (about 7 s down), rollback tag `ddp-openstates-api:pre-core939e4a2c`.
- **`check-core-pin-drift.py --image-core 939e4a2c...`** at 18:19 UTC after the ddp-sync release: exit 0; the scrapers image, api-v3 and the ddp-sync loader are all at `939e4a2c`.
- **ddp-open-states `origin/main` is now `f721770e3`.** Changed since the api-v3 build (`git diff f0cfd9c7a origin/main`, outside manifest, notes and people): `PRIMITIVES.md`, `backfill-vote-person-resolution.py`, `relink-blank-voters.py`, `validate_jurisdictions.py` and two test files. No file under the api-v3 build context other than those that I can tell. The state list (`deploy/manifest`, a live directory mount) is already current: the rollout at 17:08 UTC synced it and the container's copy matched md5.
- **`85c8888` (SYNC-187)** changes only `pipelines/cloud_scrape_trigger.py`, `pipelines/openstates_scrape.py`, `primitives.md` and a test (your note `692a2ba`): no api-v3 interface.

**Please confirm:** (1) no api-v3 rebuild is needed for SYNC-187 or for anything merged to ddp-open-states main since 07:36 UTC; (2) whether openstates-core main has moved past `939e4a2c` and, if so, whether api-v3 needs it (I cannot read the core repo); (3) anything else api-v3 should have before Nebraska, Rhode Island, Oklahoma and the other states are checked through it. I will not rebuild api-v3 unless you ask and Ramon says go.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
