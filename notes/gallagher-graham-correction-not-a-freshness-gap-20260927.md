# Correction on James Gallagher/Darline Graham: not a data-freshness gap -- a real routing bug (SYNC-78 opened)

**Re:** `sync76-77-validation-run-results-20260927.md` (this branch). My read there ("stale/incorrect
`openstatesid` values already sitting in the Legislators CMS, worth its own small ticket to check
how many records 404") was wrong. Correcting before anyone files that as a data-cleanup ticket.

## What's actually going on

Both James Gallagher (R-CA-1) and Darline Graham (R-SC) are real, currently-serving federal
legislators, seated very recently -- Gallagher sworn into the House 2026-06-10 (special election
after Doug LaMalfa's death), Graham appointed to the Senate 2026-07-14 (after her brother Lindsey
Graham's death). Confirmed via Congress.gov (bioguide `G000607`) and the Senate's own public vote
data (`lis_member_id S441`) that both are actively voting right now -- Gallagher "Yea" on H.R. 9576
(2026-09-16), Graham "No" on H.Con.Res. 89 (2026-09-24).

**Queried the local RDS-backed api-v3 replica (`localhost:8002`) directly and it already has both
of them completely and correctly** -- real `current_role`, and the exact same two votes above,
byte-for-byte matching Congress.gov/Senate.gov, under the exact same `ocd-person/` IDs already
stored as `Representative.primary_openstates_id` in `ddp-broker-py`. The replica is not behind at
all. It's the *public* `v3.openstates.org` API specifically that's missing both of them --
`/people?id=` there returns 0 results for either.

## The real bug: `_get_sponsor_name()` can only ever see the public API

`LegislatorSyncService._get_sponsor_name(person_id)` in `legislator_sync.py` always hits the
public API base -- documented as deliberate, since historically "no jurisdiction is available
here to route to the local replica." That's stale reasoning: its only caller,
`fetch_sponsored_bills(person_id, jurisdiction, ...)`, already has `jurisdiction` in scope at the
exact call site, it's just never passed through. So for anyone whose record exists in the RDS
replica but not (yet, or ever) in the public API -- which will keep happening every time a new
member is seated in an RDS-covered jurisdiction -- the sponsor-name lookup fails and
`fetch_sponsored_bills()` gives up with an empty list, even though the replica already has
everything it needs.

That's exactly what produced the "Could not determine sponsor name" / `total_bills=0` result for
these two in the validation run -- not bad data, a routing gap.

## Filed

**SYNC-78**: give `_get_sponsor_name()` an optional `jurisdiction` param, route it through
`_get_api_base_and_key(jurisdiction)` like the other two methods already do, have
`fetch_sponsored_bills()` pass its own `jurisdiction` through. Same diverged-branch note as
SYNC-76/77 applies (mechanical settings-field rename).

## Net for SYNC-76/SYNC-77

Nothing changes about those two -- they're confirmed working and this doesn't reopen them. Just
don't chase "stale CMS openstatesid values" as a cleanup ticket off my earlier note; that framing
was wrong. The real, fixable thing is SYNC-78.
