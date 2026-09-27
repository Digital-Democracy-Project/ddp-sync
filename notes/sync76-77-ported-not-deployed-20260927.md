# SYNC-76/SYNC-77 ported to feat/rds-openstates-routing-standalone -- pushed, NOT deployed yet

**Re:** `sync76-77-fixes-need-porting-to-diverged-branch-20260926.md` (this branch). Went with
option 1 (port it ourselves) rather than waiting on a follow-up PR.

## What was done

Cherry-picked both fix commits from `main` straight onto `feat/rds-openstates-routing-standalone`:

- `ad3ab87` (was `7833292` on `main`) -- SYNC-76, federal jurisdiction mistag. Applied clean, no
  conflicts.
- `409efd5` (was `b246129` on `main`) -- SYNC-77, `ocd-person/` ID-prefix mismatch. One hunk
  auto-merged in `legislator_sync.py` (expected -- our branch's own `_get_api_base_and_key()`
  region sits nearby), no conflicts.
- `bbb4c48` (new, ours) -- SYNC-77's cherry-picked test fixture (`tests/test_legislator_sync_
  ocd_person_id.py`) used `main`'s `local_openstates_api_base`/`local_openstates_api_key` field
  names in its `SyncSettings` fixture; this branch renamed those to `rds_openstates_api_base`/
  `rds_openstates_api_key` when it added RDS-replica routing. Exactly the mechanical adjustment
  your note predicted -- fixed the fixture, no change to the actual fix logic. 5 tests were
  failing on `TypeError` before this; all pass after.

**Full suite: 279/279 passing.** All three commits pushed to
`origin/feat/rds-openstates-routing-standalone` just now.

## Not deployed

Deliberately holding off on `pip install .` / `systemctl restart ddp-sync` -- the code is on the
branch and on GitHub, but not yet running on this host. Will deploy separately once we're ready;
this note is just to close the loop on the port itself so SYNC-76/SYNC-77 can move past "In
Review" on your side if that status was gated on this host actually having the fix.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
