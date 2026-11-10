# Request (GO for Parts A and B): deploy api-v3 `1cf251b` and ddp-sync `cb6f947` on the EC2-broker host (OPEN-520, SYNC-152, SYNC-153, OPEN-523). The manifest rollout (Part C) HOLDS for a separate GO. 2026-11-10 about 18:00 UTC (Mac side)

**Ramon's words, 2026-11-10:** "let's merge in OPEN-521 and OPEN-523 and ask the prod agent to update api-v3 and ddp-sync".

## What I need from you
1. **Part A:** update the EC2 api-v3 to `main` `1cf251b`.
2. **Part B:** update the EC2 ddp-sync to `main` `cb6f947`, then the read-only checks.
3. **Part C:** the manifest rollout. **Holds** until a `go-…` note.
4. Reply after A and B: `reply-deploy-api-v3-and-ddp-sync-one-pipeline-20261110.md`.

**Do A before B, and B before C.** The new manifest (no `tier`, no `archive.scheduled`) breaks the old api-v3 (its state list answers 503 without `tier`) and the old ddp-sync (reads `tier`). Both new versions accept the old manifest and the new one. The **Mac** api-v3 is Ramon's own `refresh-api-v3.sh`, not yours.

## Part A: api-v3 on EC2 (your usual pull-and-swap)
* **What it brings:**
  * **OPEN-520 (#56):** `/ddp/enrollment` no longer reads or serves `tier`. The broker reads only `replica_routing` from it (checked in code).
  * **OPEN-519 (#55)**, if the host does not have it yet: `bill_types_only` and the bill-type filter in search. Report the host's current api-v3 commit before the pull.
* **Gates:**
  * `git -C /opt/ddp-open-states/api-v3 status` clean.
  * Tag a `pre-open520` rollback image.
* **Change:**
  * `git pull --ff-only` (expect `1cf251b`).
  * `docker compose -p ddp-openstates-rds -f docker-compose.rds.yml build api`.
  * `up -d --no-deps api`. **Only `api`; never an unscoped recreate.**
* **Check (read-only):**
  * `/ddp/enrollment` answers 200, and no entry carries `tier`.
  * `/bills?jurisdiction=pa&per_page=1` answers 200.
  * Postgres's container start time is unchanged.

## Part B: ddp-sync on EC2 (`/opt/ddp-sync`)
* **What it brings:**
  * **SYNC-152 (#269):** one job per scraped state, `openstates_scrape_<state>`: scrape, then archive, then LegBot. Who is scraped comes from the manifest's status (not paused or probing); when comes from `openstates_scrape.timing`, with the same times as today.
  * **SYNC-153 (#270):** no weekly archive days; the archive follows each scrape, with a per-state lock and two retries (30 min, 2 h) then an alert.
  * **OPEN-523 (#271):** `al` added to the two cloud-path lists.
  * Not included: SYNC-154 (the knowledge-base backfill pause is unchanged).
* **G1, starting state:**
  * Report branch and `git rev-parse --short HEAD`.
  * `git -C /opt/ddp-sync status config/` and `git diff config/sync_schedule.yaml` must show **no local edits**. The container reads the host's `config/` copy. If an old yaml survives, every state silently falls to weekly Sunday, WA/USA/PA stop running nightly, the US loses a chamber, and Michigan loses its ScrapeBot fallback. Any local edit: **stop and report**.
* **G2, quiet:**
  * No scrape, archive, load or LegBot relay in flight.
  * At least an hour clear of 02:00, 02:30, 03:00 and 04:00 UTC (Pennsylvania's nightly runs from 04:00 for about 7 hours).
  * The recreate also clears every pipeline lock, by design.
* **G3, rollback point:** the commit and container/image you replace.
* **G4, environment (read, do not print values):** `JURISDICTION_MANIFEST_MODE=authoritative`, and `OPENSTATES_ROOT` pointing at the host's ddp-open-states checkout. Nothing new is needed.
* **Change:**
  * `git pull --ff-only origin main` (expect `cb6f947`).
  * Recreate **only the ddp-sync container**, by the host's usual procedure.
* **Check (read-only; start no run):**
  1. `/health` healthy, Redis connected, scheduler running, no startup errors.
  2. The yaml now has `openstates_scrape.timing`, `scrapebot_fallback` and `escalation`, and no `primary`/`secondary`.
  3. `GET /schedule`:
     * exactly one `openstates_scrape_<state>` for each of **az fl la ma mi nc ne pa ut va wa usa** (12; Alabama is still paused in the host's manifest);
     * none of `openstates_secondary_scrapes`, `openstates_secondary_scrape_*`, `openstates_primary_scrape_*`, `openstates_fl_scrape`, `openstates_wa_scrape`, `openstates_usa_scrape`, `openstates_archive_*`;
     * report each job's next run time: expect WA 02:30, USA 03:00, PA 04:00 nightly, and the others Sunday 02:00.
  4. `POST /ddp-sync/v1/trigger/openstates-scrape/zz` with this host's key must answer **404**, and its list must contain the 12 states, including `pa`, `ne` and `la`. A 404 starts nothing. **Do not** trigger a real scrape to test 202/409: tonight's 04:00 Pennsylvania nightly is the end-to-end check.
* **After tonight's Pennsylvania nightly (report later, read-only):**
  * Its log shows scrape, then archive, then the LegBot trigger, in one job.
  * Its status record `openstates_scrape_pa` says `completed`.
  * No `openstates_archive_*` job ran on its own.

## Part C: manifest rollout (HOLDS until a GO note)
`rollout-manifest.sh` on this host, bringing the host's ddp-open-states checkout to `main` (`a2ade4c` or later): OPEN-521 (#357, `tier` and `archive.scheduled` gone) and OPEN-523 (#358, Alabama `status: scraping`, `cloud_path: true`).
* **Before running it,** confirm the **running** ddp-sync image is the Part B one: the script checks the checkout, not the running image.
* **Effect:** Alabama gets `openstates_scrape_al`, weekly Sunday 02:00 UTC, on scrapers image v54 (revision 57).

## Not authorized
* Part C before its GO note.
* A real scrape or archive trigger; any LegBot run.
* Any environment or yaml edit.
* Any container other than `api` (Part A) and ddp-sync (Part B).
* The votebot/ddp-api host.
* Changing a task definition.

## What I check on the Mac side
* The trigger list and schedule match the manifest (drift check OK for 13 states on `main`, run 2026-11-10 17:58 UTC).
* After tonight: Pennsylvania's run read end to end.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
