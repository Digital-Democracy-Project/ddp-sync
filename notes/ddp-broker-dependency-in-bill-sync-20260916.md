# Addendum: ddp-broker-py dependency in bill_sync's Flow 2 (found during main-upgrade audit)

**Date:** 2026-09-16
**Found by:** pre-upgrade audit of `main` on the votebot/ddp-api EC2 host, scoping the same
upgrade that `notes/point-votebot-ec2-at-production-api-v3-20260915.md` covers.
**Audience:** whoever (human or agent) pulls that host's `ddp-sync` checkout up to `main`.

## Summary

Independent of the OpenStates-repointing task in the sibling handoff doc, pulling this host
from its current pin (`6a97206`, 2026-07-02) up to `main` also picks up a change that gives
`bill_sync`'s Flow 2 (Pinecone bill-text re-ingestion on new versions) a new hard network
dependency on **ddp-broker-py's HTTP API**. This wasn't on the sibling doc's radar because it's
unrelated to OpenStates routing -- it's about how this host decides "have I already seen this
bill version."

## What changed and why (commit `bb8defe`, PR #4, merged 2026-07-26)

**Before:** `check_and_reingest_version()` compared the latest OpenStates version against a
`ddp:bill_version:{webflow_id}` Redis key on this instance's own local Redis.

**After** (per `main`'s own README, Configuration section): that check -- and the
surplus-chunk-deletion `chunk_count` it needs from the *previous* version -- now come from
`ddp-broker-py`'s `BillVersion` table via `broker_client.get_latest_bill_version()` /
`write_bill_version()`, over HTTP, using `settings.ddp_broker_api_base` /
`settings.ddp_broker_api_token`. The stated rationale: the Mac Studio and EC2 `ddp-sync`
instances each had their own Redis, so they could independently (and wrongly) disagree about
"is this a new version" -- a single shared Postgres table owned by ddp-broker-py removes that
divergence risk across *every* `ddp-sync` instance, this host included. It wasn't written with
this specific host in mind; it's a blanket redesign of `bill_sync`'s version-check.

This is **not** gated by any of the 12 per-host toggle flags (`_TASK_ENABLE_FLAG_ENV_VARS` in
`config.py`, the same map the sibling doc's step 3 uses). There is no way to keep
`bill_sync_enabled=true` (which this host needs, for Flow 1's Webflow `status`/`status-date`/
`status-chamber`/`gov-url` writes) while opting Flow 2 out of the ddp-broker-py dependency --
it's baked into the same job.

## Failure mode if `ddp_broker_api_base` isn't reachable/configured for this host

Fails safe, not loud -- which is exactly why it's easy to miss:

- `get_latest_bill_version()` raising `BrokerClientError` (connection refused, timeout, auth
  failure, etc.) is caught in `bill_version.py::check_and_reingest_version()`, which logs
  `"Failed to read BillVersion from ddp-broker-py -- skipping this bill's version check this
  run"` and returns early.
- Flow 1 (Webflow status/date/chamber/gov-url) is unaffected -- it's a separate,
  already-decoupled write path (see this repo's own `TROUBLESHOOTING.md`, "Data Flow
  Decoupling").
- Flow 2 (detecting a new bill version and re-ingesting text + changelog into Pinecone)
  silently never fires. No crash, no scheduler alert, no 5xx -- `daily_bill_sync` in `/health`
  keeps reporting `"status": "completed"` every night regardless. The only visible signal is a
  per-bill `BrokerClientError` line in `journalctl`.

`settings.ddp_broker_api_base` defaults to `http://localhost:8080` (the `_load_from_env()`
fallback) when not supplied, which is certainly wrong on this host. Unlike `redis_url` /
`mac_ddp_sync_base_url` / `rds_openstates_api_base`, it has **no** SYNC-51/OPEN-193-style
env-var override wired into `get_settings()` -- so unlike the sibling doc's four scrape/archive
flags, there is no `Environment=` line in the systemd unit that can correct this after the fact.
It has to be the right value in Secrets Manager itself.

## What needs to happen before/while pulling this host to main

1. Determine whether this host (votebot/ddp-api EC2) actually has network reachability to
   wherever `ddp-broker-py` runs. Per the sibling doc, that's co-located with the *other* EC2
   instance (alongside `ddp-open-states`/api-v3) -- whether the VPC/security-group path is open
   from *this* specific host is unconfirmed, same open caveat the sibling doc raises for
   `10.0.0.11:8002`.
2. Get the correct `ddp_broker_api_base` (and `ddp_broker_api_token`, if ddp-broker-py requires
   auth on this path -- not yet confirmed either way) for this host's own Secrets Manager secret
   (`ddp-sync/credentials` -- separate from the ddp-broker EC2's own secret, same rule as the
   sibling doc's warning against cross-wiring the two hosts' secrets).
3. After adding it and restarting, don't rely on `/health` alone -- trigger
   `/trigger/bill-version-check` (or wait for the 04:00 UTC run) against a bill you know has a
   pending version change, and grep `journalctl` for `"Failed to read BillVersion from
   ddp-broker-py"` to confirm the read path actually succeeds, not just that the service came up
   healthy.

## Correction to an assumption raised in discussion: this host DOES have local Redis

Confirmed directly on the host (2026-09-16): `redis-server.service` is active (running
continuously since 2026-06-12), listening on `127.0.0.1:6379`, responds to `PING`/`PONG`, and
the running `ddp-sync` process's own `/health` already reports `"redis": "connected"`. Redis
isn't going away post-upgrade either -- per the Phase 4 README note above, it's still written
for VoteBot's `webflow_id -> slug` cache reconciliation, just no longer *read* for
version-tracking. Redis availability was never the risk here; the new risk is purely the
ddp-broker-py HTTP reachability described above.

## Open items

- Confirmed reachable `ddp_broker_api_base` value for this host: not yet obtained.
- Whether ddp-broker-py's API requires a Bearer token for these two endpoints, or is
  unauthenticated on the internal network path: not yet confirmed from this side.
