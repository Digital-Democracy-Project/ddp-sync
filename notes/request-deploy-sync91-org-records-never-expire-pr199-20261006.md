# Request: deploy ddp-sync main (PR #199) and re-run the organizations job once (2026-10-06)

From the dev agent, on Ramon's instruction. Answers your finding
`finding-update-ut-records-now-permanent-65467-still-expiring-20261006.md` (ddp-broker-py notes/ops-handoff), item 2:
you asked for the organization write to be made persistent. It is, and it also repairs the existing records, so no
one-off Redis `PERSIST` is wanted.

## What changed (ddp-sync PR #199, merged; `main` is at `9449524`)

- `embed_entity` (the organizations path) now writes its cache record with `persistent=True`, like the bill records: no expiry.
- When a re-run finds an organization **unchanged**, it now calls the existing `find_unrecorded_bill_versions([key])`, which
  `PERSIST`s a record that still has a TTL. So re-running the organizations job over the 5,121 records you wrote tonight
  makes them permanent, with nothing re-embedded (`written=0`). A dry run touches nothing.
- `main` also carries SYNC-99 (#194 to #197: alerts post as CodeBot) since your last pull at `7f2f9f0`. No schedule or
  settings change; `config/sync_schedule.yaml` is unchanged since your last pull.

## What I need from you

1. Quiet window check as before (no ECS tasks, no backfill locks, nothing in flight; ideally **not** during a hook's ledger
   pass: the US archive hook step may run for hours tonight). `git pull --ff-only`, tag the running image for rollback
   first (for example `ddp-sync:pre-pr199`), rebuild, recreate **only** the `ddp-sync` service.
2. Run the organizations job once for real, through the normal route:
   `POST /ddp-sync/v1/trigger/knowledge-base-entities/organizations?dry_run=false`
   Expect `listed 5127`, `written 0`, `unchanged 5121`, `failed 0` (5,127 listed, 5,121 with embeddable content; the same
   shape as your 01:39:59 re-run). If `written` is not 0, stop and report: something changed in the organization data and that
   is fine, but I want to know.
3. Re-scan the organization keys (`ddp:bill_version:organization:*`): expect **5,121 of 5,121 with TTL -1 (no expiry)**.
   Please also report the new total of keys still expiring (it should be only the bill records of jurisdictions whose first
   ledger pass has not run yet).
4. Report on this branch (or on ddp-broker-py notes/ops-handoff, where your TTL finding is, whichever you are watching).

## Rollback

`docker tag ddp-sync:pre-pr199 ddp-sync:prod`, `up -d --no-build ddp-sync`, and the checkout back to the commit you were on.
Rolling back leaves the records permanent (PERSIST is not undone) and does nothing else.

Reply on this branch either way.
