# Reworked the branch: rds_openstates_api_base/key instead of a new local_openstates_* field

**Date:** 2026-09-16
**Replying to:** `notes/decision-standalone-branch-is-the-right-call-instance-being-retired-20260916.md`
**Branch:** `feat/rds-openstates-routing-standalone`, now at commit `2b5ddc7` (was `77f93f7`).

## What changed

The first version of this branch added new `local_openstates_api_base`/`local_openstates_api_key`
settings, mirroring upstream SYNC-6/SYNC-8's own field names. Feedback on this: this host has no
local OpenStates DB to point a "local_openstates_api_base" at -- that pair is specifically the
Mac Studio's own separate local Postgres instance. Adding it here, pointed at the production
RDS-backed replica instead, would be a misleading field name for anyone reading this host's
config later.

Reworked to use `rds_openstates_api_base`/`rds_openstates_api_key` instead -- these already
exist as a field name/shape shared across ddp-sync hosts' Secrets Manager schema (added by
upstream SYNC-59), just not wired to any of the bill/legislator/people pipelines yet (today the
only consumer is `openstates_archive.py`'s `resolve_touched_sessions()`, for an unrelated
purpose, on the ddp-broker EC2). This host's `rds_openstates_api_key` already exists in its own
Secrets Manager secret and is already confirmed working against `10.0.0.11:8002` (see
`notes/reachability-confirmed-from-votebot-ddp-api-host-20260916.md`) -- no new key, no new
field, no confusing name.

All 5 routing call sites (`bill_sync.py`, `legislator_sync.py`, `openstates_people.py`,
`federal_legislator_cache.py`, `ingestion/sources/openstates.py`) and the 5 ported test suites
were renamed accordingly. Dropped the placeholder `"http://localhost:8002"` default that had
been copied from the old field's convention -- `rds_openstates_api_base` has no portable default
across environments, matching upstream's own SYNC-59 field (empty by default). 266 tests still
pass.

## What's needed in Secrets Manager now (updated from the prior note)

Only two new keys, not three -- `rds_openstates_api_base` needs a value too (it was never
populated on this host, only `rds_openstates_api_key` was):

```json
"rds_openstates_api_base": "http://10.0.0.11:8002",
"ddp_openstates_jurisdictions": ["FL", "WA", "US", "VA", "MI", "MA", "UT", "AZ", "NC"]
```

`rds_openstates_api_key` already exists on this host -- nothing to add there.
