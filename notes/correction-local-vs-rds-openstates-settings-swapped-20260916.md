# Correction: this whole thread had local_openstates_* and rds_openstates_* backwards

**Date:** 2026-09-16
**Replying to:** `point-votebot-ec2-at-production-api-v3-20260915.md` (the original handoff doc --
step 4's `local_openstates_api_base`/`local_openstates_api_key` values).
**Audience:** Ramon -- this changes what actually needs to go into Secrets Manager. Please
sanity-check the reasoning below; it comes from re-reading `config.py`'s own field comments,
not from re-deriving the architecture independently.

## The mixup

The original doc's step 4 says to set `local_openstates_api_base=http://10.0.0.11:8002` plus a
*new* `local_openstates_api_key` (get it from Ramon). That's backwards. Per `config.py`'s own
comments on `origin/main`:

- **`local_openstates_api_base`/`local_openstates_api_key`** (SYNC-6): *"docker-compose.ddp.yml
  maps its container to host port 8002 **on the Mac Studio**, same box ddp-sync's LegBot
  dispatch already runs on."* Default `http://localhost:8002` -- literally same-box only. This
  is the Mac Studio's own local Postgres replica.
- **`rds_openstates_api_base`/`rds_openstates_api_key`** (SYNC-59): *"a completely separate
  database from the Mac Studio's local Postgres `local_openstates_api_base` above points at,
  which never sees RDS-loaded data."* This is the RDS-backed instance -- i.e. **exactly what
  `10.0.0.11:8002` actually is** (per this same doc's own background section: "Fargate scrapers
  now load into RDS... production api-v3 reading that RDS at `http://10.0.0.11:8002`").

So `10.0.0.11:8002` is semantically an `rds_openstates_*` target, not a `local_openstates_*`
one. This was caught because the user working this upgrade already had an existing
`rds_openstates_api_key` in this host's own Secrets Manager secret and asked to test it against
`10.0.0.11:8002` directly -- it authenticated successfully (`200`, real FL bill data, both via
`X-API-KEY` header and `?apikey=` query param, plus the single-bill detail endpoint) -- before
realizing the field-name mismatch this doc had been carrying the whole time.

## The actual blocker underneath: bill_sync.py doesn't read rds_openstates_* at all

Confirmed by grep: `rds_openstates_api_base`/`rds_openstates_api_key` are read by exactly one
consumer, `openstates_archive.py`'s `resolve_touched_sessions()` (SYNC-59) -- **not** by
`bill_sync.py`, `legislator_sync.py`, `legislator_bio.py`, `federal_legislator_cache.py`, or
`openstates_people.py`, which only read `local_openstates_api_base`/`local_openstates_api_key`.
So even with the correct semantic understanding, there is currently no code path that lets this
host's Flow 1/Flow 2 route through the RDS-backed instance using the "correctly-named" setting.

## Decision made (with the user, not unilaterally)

Given a real, already-working credential exists (`rds_openstates_api_key`, confirmed live
against `10.0.0.11:8002`), and extending `bill_sync.py` etc. to also read `rds_openstates_*` is
a real code change that would need review/tests before shipping with this upgrade, the decision
was: **reuse the already-confirmed-working key value, written into the mismatched field name.**
Concretely, for this host's Secrets Manager secret:

```json
"local_openstates_api_base": "http://10.0.0.11:8002",
"local_openstates_api_key": "<same value as this host's existing rds_openstates_api_key>",
"ddp_openstates_jurisdictions": ["FL", "WA", "US", "VA", "MI", "MA", "UT", "AZ", "NC"]
```

This works today with zero code changes -- `bill_sync.py` only cares about the string values in
`local_openstates_api_base`/`key`, not their field names' documented intent. It does mean
`config.py`'s own comment on `local_openstates_api_base` ("Mac Studio... same box... NOT the
same thing as... a completely separate database from RDS") will no longer accurately describe
what this specific host is actually pointing that field at. **Whoever next reads that comment
while this host is configured this way should not be misled by it** -- flagging here rather
than silently leaving stale-looking comments unexplained.

## Original blocker resolved as a side effect

The original doc's ask ("get the real `local_openstates_api_key` value from Ramon") is now moot
-- no new key needs to be issued or found. The existing `rds_openstates_api_key` already does
the job.

## Open item

Should `bill_sync.py`/`legislator_sync.py`/etc. eventually be extended to read
`rds_openstates_api_base`/`rds_openstates_api_key` directly (the architecturally clean fix),
making this reuse temporary rather than permanent? Not decided here -- flagging for whoever owns
SYNC-6/SYNC-59's long-term shape.
