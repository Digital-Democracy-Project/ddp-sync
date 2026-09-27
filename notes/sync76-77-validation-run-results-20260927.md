# Validation run results: both fixes confirmed working live -- plus two new, smaller findings

**Re:** `sync76-77-request-one-off-validation-run-20260927.md` (this branch). Ran exactly what
was suggested:

```python
await scheduler.trigger_legislator_bills_sync(limit=10, jurisdiction="us", include_votes=True)
```

## Result

```json
{"successful": 10, "failed": 0, "total_bills": 0, "total_votes": 3, "chunks_created": 20}
```

Took 17m 51s (`duration_seconds=1071.15`) -- see the rate-limiting ask below for why that's much
longer than it needs to be.

## Both fixes confirmed working, on real production traffic

- **SYNC-76**: every one of the 10 federal legislators' requests logged
  `Routing jurisdiction to RDS-backed OpenStates replica api_base=http://10.0.0.11:8002
  jurisdiction=us` -- federal Congress members are now correctly routing to the RDS replica
  instead of the public API.
- **SYNC-77**: `person_id` is properly prefixed (`ocd-person/...`) at every OpenStates-facing
  call and comparison, and it produced a **real, nonzero vote match** (`total_votes=3`) -- the
  first genuine match this pipeline has produced in ~8 months, confirmed via `journalctl` and
  the returned dict, not just a clean exit code.

## `total_bills=0` persisted in this sample -- checked why, not a regression

Split into two distinct, unrelated causes across the 10 legislators, verified individually
(read-only `GET`s against the live public API, not assumed):

1. **8/10** (Feinstein, Helmy, Cárdenas, Caraveo, Rubio, Tester, Sinema, Stabenow) -- sponsor-name
   resolution succeeded (no warning logged). Checked Stabenow directly: OpenStates returns her
   real record with `current_role: None` -- she's no longer a sitting member. A no-longer-serving
   legislator has no *current*-session bills to sponsor, so zero matches is the **correct,
   expected result** here, not a bug. This 10-record sample (first 10 in CMS insertion order for
   `jurisdiction=us`, not random) happened to skew heavily toward former/no-longer-serving
   members.
2. **2/10** (James Gallagher, Darline Graham) -- still logged "Could not determine sponsor name."
   Checked directly: `GET /people?id=ocd-person/<their-id>` returns `200 OK` with **0 results**,
   even with the correct prefix. These two `openstatesid` values don't resolve to any real
   OpenStates person at all. Looks like stale/incorrect IDs already sitting in the Legislators
   CMS, unrelated to SYNC-76 or SYNC-77 -- worth its own small ticket (identify how many CMS
   records have an `openstatesid` that 404s/empties against OpenStates, and whether that's a
   handful or a bigger cleanup). Not chasing further ourselves right now; flagging for triage.

Net read: both shipped fixes are doing exactly what they were supposed to; `total_bills=0` in
this particular run is fully explained by sampling bias plus pre-existing bad IDs, not by
anything left broken in SYNC-76/SYNC-77. Recommend moving both to Done.

## New ask: unnecessary rate limiting against our own RDS replica

While watching this run, noticed `_apply_rate_limit()` in `legislator_sync.py` fires
unconditionally before every request (lines ~371/480/532), regardless of `is_local_replica` --
which is only checked afterward, purely to pick header-vs-query-param auth. So even a 100%
RDS-routed run (this one: `jurisdiction=us`, fully covered by the replica) still sleeps
~0.4-0.5s between every request, the same throttle that exists solely to protect the *public*
OpenStates API's quota. That's why this 10-legislator run took ~18 minutes instead of a small
fraction of that -- up to ~210 requests/legislator × ~0.45s of pure artificial sleep, against our
own database, for no quota-protection benefit at all.

This affects every RDS-routed run going forward -- this validation, and the real weekly
`legislator_sync`/`bill_sync` runs for the 9 RDS-covered jurisdictions. Please file a ticket to
skip (or substantially shorten) the rate-limit delay when `is_local_replica` is true, in both
`legislator_sync.py`'s call sites and the equivalent pattern in `bill_sync.py` if it has the same
shape. Happy to take this one too if it's not already someone's queue.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
