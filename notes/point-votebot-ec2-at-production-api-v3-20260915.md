# Ops Handoff: point the VoteBot/ddp-api EC2's ddp-sync at production api-v3

**Date:** 2026-09-15
**Decided by:** Ramon
**Audience:** whoever (human or agent) actually executes this on the VoteBot/ddp-api EC2 host

## Background: ddp-sync runs as three separate instances

This repo's own `CLAUDE.md` only documents two of them (as of this writing) -- there are
actually **three**, each independently configured with no shared config store between them:

1. **Mac Studio** -- the only instance with CAMS/LegBot/MLX access; runs most jurisdictions'
   scraping into local Postgres.
2. **An EC2 instance co-located with production `ddp-broker-py` and `ddp-open-states`/api-v3**
   (`ddp-broker` EC2 below) -- owns Fargate-based scraping + RDS loading (OPEN-193).
3. **A separate EC2 instance co-located with VoteBot and `ddp-api`** (`votebot/ddp-api` EC2
   below) -- **this is the one this document is about.** It's mostly concerned with keeping
   the (deprecated, being-replaced) Webflow CMS current and populating the Pinecone VoteBot
   knowledge base. It has been left alone while the new website was built, so it is
   substantially behind main.

## Why this change

The Fargate scrapers now load into RDS consistently in production, and there's a real
production api-v3 reading that RDS at `http://10.0.0.11:8002`. The goal is to point this
host's Webflow-sync and Pinecone-ingestion pipelines at that instance (for the jurisdictions
RDS actually has data for) instead of the public `v3.openstates.org` API, so Webflow/VoteBot
get better, more current data.

## Confirmed state of the votebot/ddp-api EC2 host (2026-09-15)

- **Deployment**: plain systemd service, not Docker. Unit name `ddp-sync`, checkout at
  `/home/ubuntu/ddp-sync`, user `ubuntu`. This repo's checked-in `infrastructure/ddp-sync.service`
  is presumed to match the live unit, but **confirm with `systemctl cat ddp-sync` before editing**
  -- don't assume they're identical.
- **No `.env` file exists on this host.** Config comes from AWS Secrets Manager
  (confirmed via `GET /ddp-sync/v1/health` -> `"config_source": "secrets_manager"`).
- This host's own Secrets Manager secret is **separate from the ddp-broker EC2's** -- they do
  not share settings.
- This host's checkout is currently pinned at commit `6a97206` (2026-07-02) -- **353 commits
  behind `main`** as of 2026-09-15. That predates SYNC-6 (2026-08-11), which is the actual
  mechanism (`local_openstates_api_base` + `ddp_openstates_jurisdictions` routing) this task
  depends on. **This is a real code upgrade, not a settings-only change.**
- Only one new Python dependency landed since July (`asyncpg`), so the upgrade itself should be
  low-risk on the dependency front.

## Two similarly-named settings that are NOT the same thing -- read this before touching anything

This codebase has two different settings that both sound like "point at our own OpenStates API":

- **`local_openstates_api_base` / `local_openstates_api_key` / `ddp_openstates_jurisdictions`**
  (SYNC-6) -- read by `bill_sync.py`, `legislator_sync.py`, `legislator_bio.py`,
  `federal_legislator_cache.py`, `openstates_people.py`. **This is the one this task needs.**
- **`rds_openstates_api_base` / `rds_openstates_api_key`** (SYNC-59) -- read only by
  `session_pipeline_runner.py` / `replica_freshness.py`, for LegBot's session-resolution and
  replica-freshness checks. Already configured and working, but on the **ddp-broker EC2**, for
  a completely unrelated purpose (LegBot dispatch, not Webflow/Pinecone sync).
  **Do not reuse or copy this value for this task** -- setting it would have zero effect on
  `bill_sync.py` etc., which never read it.

## Steps

### 1. Preflight -- confirm current state before changing anything

On the host, in the ddp-sync checkout:
```
cd /home/ubuntu/ddp-sync
git status
git log -3
systemctl cat ddp-sync
```
Confirm: working tree is clean, currently on `6a97206`, and the live unit file's content is
understood (note any drift from this repo's own `infrastructure/ddp-sync.service`). Stop and
flag before proceeding if anything here looks unexpected.

### 2. Pull the code forward and reinstall

```
git pull origin main
.venv/bin/pip install .
```
Do **not** restart the service yet -- step 3 has to happen first.

### 3. Force off the jobs this host has no business running

Main has picked up roughly 2.5 months of new scheduled jobs since this host's last update, all
defaulting to enabled (per-host opt-out flags, SYNC-51). This host only does Webflow CMS sync +
Pinecone ingestion + VoteBot eval + Voatz/Brevo sync -- it has no CAMS/LegBot/Fargate role, so
these four must be explicitly forced off:

Edit the real unit file (confirm the path with `systemctl cat ddp-sync` in step 1 -- likely
`/etc/systemd/system/ddp-sync.service`) and add these four lines under `[Service]`, after the
existing `Environment=PATH=...` line:
```
Environment=OPENSTATES_SCRAPE_ENABLED=false
Environment=OPENSTATES_ARCHIVE_ENABLED=false
Environment=MI_COOKIE_PUBLISH_ENABLED=false
Environment=SESSION_PIPELINE_BATCH_ENABLED=false
```
Everything else this host already does (bill_sync, legislator_sync, legislator_bio_sync,
organization_sync, voatz_sync, webflow_batch, votebot_eval, api_health_check) keeps its default
(enabled) behavior -- no change needed for those.

No `.env` file is needed for this. These four flags are read directly from the real process
environment (a systemd `Environment=` line sets that directly) regardless of where the rest of
the config comes from -- this bypasses Secrets Manager entirely, by design.

### 4. Add three new values to this host's own Secrets Manager secret

This is a **separate secret from the ddp-broker EC2's own** -- do not edit the broker's secret,
and do not reuse its `rds_openstates_api_key` value (see the warning above).

Add these keys:
```json
"local_openstates_api_base": "http://10.0.0.11:8002",
"local_openstates_api_key": "<real key -- confirmed to already exist; get the actual value from Ramon or wherever it's stored. Do not hardcode a real key into this file or any other file in this repo.>",
"ddp_openstates_jurisdictions": ["FL", "WA", "US", "VA", "MI", "MA", "UT", "AZ", "NC"]
```

**Gotcha:** federal is `"US"` here, not `"USA"`. This codebase separately uses `"usa"` as the
scraper-job identifier elsewhere (`config/sync_schedule.yaml`'s `openstates_scrape.cloud_path.
jurisdictions`), but the routing code that reads `ddp_openstates_jurisdictions`
(`bill_sync.py`/`legislator_sync.py`/`federal_legislator_cache.py`) compares against `"us"`.
Using `"USA"` here would silently mean federal bills never route to the new instance --
there's no error, they'd just keep hitting the public API.

The jurisdiction list above matches `openstates_scrape.cloud_path.jurisdictions` in
`config/sync_schedule.yaml` exactly -- i.e. the jurisdictions RDS actually has data for. Don't
widen this list without first confirming RDS coverage for whatever you're adding -- anything
else would hit `10.0.0.11:8002` and find nothing.

### 5. Reload and restart

```
sudo systemctl daemon-reload
sudo systemctl restart ddp-sync
curl http://localhost:8001/ddp-sync/v1/health
```
Expect `"status": "healthy"` and `"config_source": "secrets_manager"`.

### 6. Verify it's actually working

- Confirm network reachability first, if not already known: from the EC2 host itself, hit
  `10.0.0.11:8002` directly (e.g. `curl http://10.0.0.11:8002/bills?jurisdiction=fl`). This is a
  real VPC/security-group path that has never been exercised from this specific host before --
  don't assume it's open.
- Watch the next scheduled `bill_sync`/`legislator_sync`/`legislator_bio_sync` run (or trigger
  one manually via the `/trigger/*` endpoints) for one of the nine jurisdictions above, and
  confirm it's actually reading from the new instance. The fallback to the public API on a
  misconfiguration is **silent** (no error, it just quietly uses `openstates_api_base` instead)
  -- absence of errors is not proof this worked.

## Open items not resolved as of this writing

- The real value for `local_openstates_api_key` -- confirmed to already exist somewhere, but
  this document doesn't have it. Get it from Ramon before step 4.
- Whether `10.0.0.11:8002` is actually reachable from this host's VPC/security group -- not yet
  confirmed; check in step 6.
- Whether the live `/etc/systemd/system/ddp-sync.service` on this host has drifted from this
  repo's checked-in `infrastructure/ddp-sync.service` -- confirm in step 1 rather than assume
  they match (this exact kind of drift bit `config/sync_schedule.yaml`'s own `cloud_path` block
  once already, per OPEN-231's reconciliation note).
