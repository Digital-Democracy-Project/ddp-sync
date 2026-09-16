# Reply: correction + address candidates for the ddp-broker-py dependency note

**Date:** 2026-09-16
**Replying to:** `notes/ddp-broker-dependency-in-bill-sync-20260916.md`
**Audience:** whoever picks this up next on the votebot/ddp-api EC2 upgrade.

## Verified the underlying finding -- it's correct

Checked commit `bb8defe` (2026-07-26, real, in `ddp-sync` history) and the current code:
`bill_version.py::check_and_reingest_version` does call `broker_client.get_latest_bill_version()`
/ `write_bill_version()` over HTTP now, a `BrokerClientError` there is caught and only logged
(`logger.error("Failed to read BillVersion from ddp-broker-py -- skipping this bill's version
check this run")`, no crash, no scheduler alert), and `ddp_broker_api_base`/`ddp_broker_api_token`
really do have no SYNC-51/OPEN-193-style override wired into `get_settings()` -- only
`_load_from_env()` reads them (`config.py` lines 647-648). This is a real 6th instance of that
bug class and needs to go into this host's own Secrets Manager secret, same as the sibling
doc's three OpenStates settings.

Also confirmed: `broker_client.py`'s `get_latest_bill_version` always sends
`Authorization: Bearer {settings.ddp_broker_api_token}` -- if that's empty (its default), the
request goes out as `Bearer ` with nothing after it. If the production ddp-broker-py actually
enforces auth on this endpoint (likely, for a production API, but not directly confirmed from
this side either), a missing token will look exactly like the same silent-skip failure mode as
an unreachable address -- no distinct error to tell the two apart from the logs alone.

## Correction to the previous note

That note said there's no way to keep Flow 1 (Webflow status) running while opting Flow 2
(Pinecone re-ingestion, the part that now depends on ddp-broker-py) out. That's true of the 12
per-host toggle flags, but `config/sync_schedule.yaml` already has a separate, existing lever
for precisely this split:

```yaml
bill_sync:
  webflow_status:
    enabled: true   # Flow 1
  version_check:
    enabled: true   # Flow 2 -- the one with the new ddp-broker-py dependency
```

`scheduler.py::_run_daily_bill_sync` reads these two independently and calls
`sync_bill_statuses()` (Flow 1 only) instead of `sync_bill_versions()` when `version_check.
enabled` is false. So setting `bill_sync.version_check.enabled: false` in that file is a real,
working way to keep Flow 1 running while Flow 2 stays off -- **but it's a shared YAML value
every ddp-sync host reads identically, not a per-host flag.** Flipping it would turn off Flow 2
on any other host that also runs `bill_sync` too. In practice that's probably just this one
host (the Mac and the ddp-broker EC2 both look to run with `BILL_SYNC_ENABLED=false`), so this
could be a reasonable temporary stopgap if the ddp-broker-py address/token isn't ready before
the upgrade window -- just don't treat it as a clean per-host switch, and double check no other
host is quietly relying on `bill_sync` before flipping it.

## Address candidates for `ddp_broker_api_base`

Ramon's best guess: **`http://10.0.0.11:8080`**, possibly instead reachable on `:443`, `:80`, or
behind nginx on `:8001`.

`http://10.0.0.11:8080` is not a new guess -- it already appears as the configured
`ondemand_broker_api_base_prod` value in this repo's own test fixtures
(`tests/test_trigger_legbot_analyze_bill.py`, `tests/test_trigger_legbot_analyze_bill_full.py`),
which is the address the on-demand single-bill LegBot endpoints already use for real production
ddp-broker-py calls. That's reasonable corroborating evidence, not confirmation from this host's
own network path -- `config.py`'s own comment on `ddp_broker_api_base` explicitly separates the
"local Mac Studio dev stack" default (`:8080`) from "production points at the real broker
host/token via env," i.e. don't assume the dev-default port carries over.

One flag on the `:8001` candidate specifically: that's the same port `ddp-sync` itself runs on,
on both the votebot/ddp-api host and (per `infrastructure/docker-compose.prod.yml`) the
ddp-broker EC2. If ddp-broker-py's own API is also reachable at `:8001` there, it has to be via
nginx routing by hostname/path rather than a bare port -- worth confirming it isn't actually
just ddp-sync's own health port being tried by mistake.

**Before writing anything into Secrets Manager:** from the votebot/ddp-api EC2 host itself, try
hitting `GET {candidate}/api/bill-versions/latest/?bill_openstates_id=<any real UUID>` (the
exact endpoint `get_latest_bill_version` calls) against each candidate and see which one
actually responds like ddp-broker-py rather than timing out, connection-refusing, or hitting
nginx's default page. That confirms both reachability and the right port in one step, and will
also show whether an unauthenticated request gets a 401/403 (answering the token question at
the same time).

## Open items (updated)

- Confirmed reachable `ddp_broker_api_base` value + port for this host: still not obtained --
  see the candidates and the verification step above.
- Whether a real `ddp_broker_api_token` is required: still not confirmed either way, but the
  code always sends *some* Bearer header, so if a real token is required this needs to be added
  regardless of which address turns out to be right.
- `bill_sync.version_check.enabled: false` as an interim stopgap: viable in principle (see
  correction above), not yet decided or applied.
