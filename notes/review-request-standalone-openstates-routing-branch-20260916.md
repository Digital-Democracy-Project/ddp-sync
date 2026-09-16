# Review request: standalone OpenStates-routing branch, skipping the other 351 commits entirely

**Date:** 2026-09-16
**Audience:** Ramon / whoever reviews this next.
**Branch:** `feat/rds-openstates-routing-standalone` (commit `77f93f7`), pushed to origin.
PR link: https://github.com/Digital-Democracy-Project/ddp-sync/pull/new/feat/rds-openstates-routing-standalone

## Why this branch exists instead of just pulling main

Working through this thread surfaced two problems with pulling this host straight to `main`:

1. Doing so would also pull in `bill_version.py`'s new ddp-broker-py dependency (both a
   read and a write to ddp-broker-py's production `BillVersion` table, for *every* bill this
   host processes in Flow 2 -- not scoped to the 9 local-replica jurisdictions). This host's
   Flow 2 works fine today on its own local Redis cache, with zero dependency on ddp-broker-py.
   The pull would trade a working, dependency-free mechanism for one with a new failure mode
   that's silent by design (see `notes/ddp-broker-dependency-in-bill-sync-20260916.md`).
2. This host's actual goal was narrower than the original handoff doc assumed: point
   OpenStates calls at the RDS-backed replica, nothing else. Everything else in the other 351
   commits (12 per-host job-disable flags, LegBot/CAMS dispatch, Fargate archive/backfill
   machinery, etc.) is irrelevant to what this host does (Webflow CMS sync + Pinecone
   ingestion + Voatz/Brevo + votebot eval).

So instead: this branch reimplements just the OpenStates-routing capability (upstream commits
`de9db6c` SYNC-6 and `a830a81` SYNC-8) directly on top of this host's current pin (`6a97206`),
by hand -- not a `git cherry-pick` (SYNC-6's own diff touches `local_openstates_client.py`,
which doesn't exist at this host's pin; that file's own creating commit, `409841a`, also
touches `bill_artifact_generation.py`, a large LegBot pipeline file that doesn't exist here
either and isn't needed). Confirmed while tracing this: SYNC-6's `bill_sync.py` routing never
actually imports anything from `local_openstates_client.py` -- that file's own 12-line touch
from SYNC-6 is purely a docstring update. So none of that module or its LegBot dependencies
were needed at all for this task.

## What's in the branch

Adds 4 settings (`openstates_api_base`, `ddp_openstates_jurisdictions`,
`local_openstates_api_base`, `local_openstates_api_key`) and a `_get_api_base_and_key(jurisdiction)`
routing helper (same shape in each file, mirroring `bill_sync.py`'s original) to:

- `bill_sync.py` -- `fetch_bill_from_openstates()`
- `legislator_sync.py` -- `fetch_sponsored_bills()`, `fetch_legislator_votes()`
- `openstates_people.py` -- `iter_jurisdiction()` (via `_get_json(..., jurisdiction=...)`)
- `federal_legislator_cache.py` -- `refresh()`/`_fetch_chamber_members()` (always jurisdiction "us")
- `ingestion/sources/openstates.py` -- `fetch_jurisdiction()`, `fetch()`, `fetch_legislators()`
- `legislator_bio.py` / `scripts/backfill_legislator_party.py` -- thread the new settings through
  to `OpenStatesPeopleClient`'s constructor

Single-ID lookups with no jurisdiction in scope (`_get_sponsor_name`, `fetch_by_id`,
`fetch_bill`, `fetch_legislator_by_id`) are unchanged -- always public API, matching what SYNC-6/
SYNC-8 themselves did.

**Explicitly excluded, by design:** `bill_version.py`, `scheduler.py`, any `ddp_broker_*`
setting, the 12 job-disable flags, and everything LegBot/CAMS/Fargate-related. This host's Flow 2
version-check keeps using local Redis, completely unchanged.

## Testing done so far

Ported the four upstream routing test suites unchanged (`test_bill_sync_openstates_routing.py`,
`test_legislator_sync_openstates_routing.py`, `test_openstates_people_client_routing.py`,
`test_federal_legislator_cache_routing.py`, `test_openstates_source_routing.py`) into an
isolated scratch venv (not this host's production `.venv`) built from this branch's
`pyproject.toml[dev]`. **All 266 tests pass**, including every pre-existing test in the repo --
confirms nothing else regressed. Also smoke-tested importing every touched module.

**Not yet done:** actually running this against the local/RDS OpenStates replica
(`http://10.0.0.11:8002`) end-to-end, or deploying it anywhere. Config values still needed
(from the sibling threads): `ddp_openstates_jurisdictions` list, and either a real
`local_openstates_api_key` or reuse of the already-confirmed-working `rds_openstates_api_key`
value (see `notes/correction-local-vs-rds-openstates-settings-swapped-20260916.md`).

## Ask

Please review the branch/PR link above before this gets merged and deployed to the
votebot/ddp-api EC2 host. In particular: is reimplementing SYNC-6/SYNC-8 by hand instead of
eventually reconciling with a real `main` pull the right long-term call for this host, or should
this be treated as a stopgap until the ddp-broker-py dependency question (see the sibling notes)
is resolved one way or another?
