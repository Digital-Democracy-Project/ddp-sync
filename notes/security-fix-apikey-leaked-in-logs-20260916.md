# Security fix: RDS OpenStates API key was leaking into logs (caught before production impact)

**Date:** 2026-09-16
**Branch:** `feat/rds-openstates-routing-standalone`, fix at commit `ef55b8e`.
**Audience:** whoever reviews this branch, and anyone touching `bill_sync.py`'s OpenStates
client in the future.

## What happened

While manually verifying the RDS routing end-to-end after deploying this branch to the
votebot/ddp-api EC2 host, a pre-existing log line in `bill_sync.py`'s
`fetch_bill_from_openstates()` --

```python
logger.info("OpenStates API response", status_code=response.status_code, url=str(response.url))
```

-- logs the *full request URL including query parameters*. This was harmless against the public
`v3.openstates.org` API (header-based auth, key never in the URL), but the RDS-backed replica's
`apikey_auth` scheme (added by this branch) sends the key as a `?apikey=` query param. Every
RDS-routed fetch (FL/WA/US/VA/MI/MA/UT/AZ/NC -- i.e. most of what this host now does) would have
logged the real `rds_openstates_api_key` value in cleartext into `journalctl`, indefinitely,
going forward.

## Impact assessment

- The key was printed once into an interactive terminal session (a manual verification script,
  not the running service) -- contained to that session, not persisted anywhere production-side.
- Confirmed via `journalctl -u ddp-sync` that **zero** occurrences of `apikey=` exist in the real
  service's logs -- no scheduled job had hit the RDS path for real between this branch's deploy
  and the fix landing, so there was no actual production log exposure.
- Caught and fixed before any real bill sync ran against the RDS replica.

## Fix

Strip the query string before logging (`response.url.copy_with(query=None)`), matching how the
already-safe `api_key_prefix=...[:8] + "..."` pattern elsewhere in this same function avoids
logging the full key. Verified: re-ran the same real fetch against a live FL bill post-fix --
log line now reads `url=http://10.0.0.11:8002/bills/fl/2026F/HJR1F` with no query string.
Grepped the touched files (`legislator_sync.py`, `openstates_people.py`,
`federal_legislator_cache.py`, `ingestion/sources/openstates.py`) for the same pattern -- this
was the only call site referencing `response.url` anywhere in the routing changes.

## Recommendation

No key rotation needed given the confirmed-zero production log exposure above, but worth a
second pair of eyes on that conclusion given this credential is shared across ddp-sync hosts'
Secrets Manager schema. If anyone has independent reason to believe `rds_openstates_api_key` was
exposed elsewhere (e.g. shell history on a shared account, other tooling), treat this note as
the trigger to reconsider that separately.
