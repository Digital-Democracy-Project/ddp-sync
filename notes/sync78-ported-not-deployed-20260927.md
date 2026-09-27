# SYNC-78 ported to feat/rds-openstates-routing-standalone -- pushed, NOT deployed yet

**Re:** `sync78-merged-and-combined-port-needed-20260927.md` (this branch). All three
(SYNC-76/77/78) are now on this branch.

## What was done

Cherry-picked `ef91fd1` (SYNC-78) onto `feat/rds-openstates-routing-standalone`, on top of the
SYNC-76/77 commits already ported earlier today.

Unlike SYNC-76/77, this one had a **real conflict** (not just an auto-merge) -- both hunks were
in the docstring/comment text right around `_get_api_base_and_key()` and `_get_sponsor_name()`,
where this branch's own field-renaming sits. Resolved by taking the incoming (SYNC-78) side's
updated comments and logic in both spots; nothing about this branch's `rds_openstates_api_base`/
`rds_openstates_api_key` naming needed to change, since the actual routing calls
(`self.settings.rds_openstates_api_base` etc.) were untouched by SYNC-78's diff. The test file
(`tests/test_legislator_sync_openstates_routing.py`) needed no field-name fixture fix this time --
it was already adapted to this branch's naming back when the original RDS-routing feature was
built here.

Commit: `94c8231`, pushed.

**Full suite: 282/282 passing** (279 before + 3 new SYNC-78 tests).

## Not deployed

Same as SYNC-76/77 -- code is on the branch and on GitHub, not yet running on this host. All
three fixes (SYNC-76, SYNC-77, SYNC-78) are now bundled together for a single deploy whenever
we're ready. Will report the actual deploy separately.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
