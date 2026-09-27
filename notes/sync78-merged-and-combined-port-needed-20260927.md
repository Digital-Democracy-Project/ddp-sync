# SYNC-78 merged to main -- and now shares the same open port-to-diverged-branch gap as SYNC-76/SYNC-77

**Re:** `sync76-77-deployed-live-20260927.md` and `sync76-77-request-one-off-validation-run-20260927.md`
(this branch). A third, related fix has landed and needs folding into the same porting work.

## What shipped

**SYNC-78**: `_get_sponsor_name()` (`legislator_sync.py`) previously always hit the public
OpenStates API, on the (now-stale) reasoning that no jurisdiction was available to route on. Its
only caller, `fetch_sponsored_bills(person_id, jurisdiction, ...)`, already had `jurisdiction` in
scope the whole time -- it just never passed it through. This is the mechanism behind the
"Could not determine sponsor name" failures for James Gallagher/Darline Graham found during
SYNC-76/77 validation: both already exist correctly in the RDS replica (matching roles and votes,
cross-checked against Congress.gov/Senate.gov directly), just not yet in the public API, and
`_get_sponsor_name()` had no way to reach the replica at all.

Fixed: `_get_sponsor_name()` now takes an optional `jurisdiction` param and routes through the
same `_get_api_base_and_key()` `fetch_sponsored_bills`/`fetch_legislator_votes` already use.
Verified safe for the general population (not just Gallagher/Graham) with a 66-person random
production sample across all 8 configured jurisdictions (`US, FL, MI, AZ, VA, WA, UT, NC`) --
zero cases of a person the public API has that the replica doesn't. PR #169, merged to `main`.

## Same gap as SYNC-76/SYNC-77: not yet on this host's actual branch

Checked directly: `feat/rds-openstates-routing-standalone`'s copy of `legislator_sync.py` still
has the old `_get_sponsor_name(self, person_id)` signature, no `jurisdiction` param. This fix
hasn't been ported here either.

**All three tickets (SYNC-76, SYNC-77, SYNC-78) now share one combined, un-actioned follow-up:
port all three fixes to this branch.** Rather than file this a third time as if it were a new
finding, folding it into the same ask already sitting on this branch. If you're already planning
to revisit the SYNC-76/77 port work, SYNC-78 should ride along with it -- same file
(`legislator_sync.py`), same mechanical settings-field-rename adjustment likely applies
(`rds_openstates_api_base`/`rds_openstates_api_key` vs. `local_openstates_api_base`/
`local_openstates_api_key`).

Reply here once ported (or let us know if you'd rather we prepare the combined port PR for you to
review/deploy).
