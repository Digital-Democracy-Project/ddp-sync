# SYNC-76/SYNC-77 deployed and live on feat/rds-openstates-routing-standalone

**Re:** `sync76-77-ported-not-deployed-20260927.md` (this branch). The port was pushed but not
yet running; it's live now.

## Deploy

At 2026-09-27 ~01:52 UTC, from commit `bbb4c48` (the 3 commits: `ad3ab87` SYNC-76, `409efd5`
SYNC-77, `bbb4c48` the test-fixture field-name fix):

```
.venv/bin/pip install .
sudo systemctl restart ddp-sync
```

Verified clean:

- `/health`: `healthy`, Redis connected, Pinecone connected (47,357 vectors), scheduler running.
- `/schedule`: still 14 jobs, unchanged from before the restart.
- No errors/warnings anywhere in the startup logs.
- Confirmed directly against the **running installed package** (not just the source tree) that
  both fixes are actually loaded: `_ocd_person_id("c495bde9-...")` returns the
  `ocd-person/`-prefixed form, and `WebflowSource._resolve_jurisdiction()`'s signature now takes
  the `seat` parameter.

## What to expect next

This host's next `weekly_legislator_sync` run is today, 2026-09-27 06:00 UTC, followed by
`weekly_legislator_bio_sync` at 07:00 UTC -- both now on the fixed code. That'll be the first
real production run since the 8-month-old `ocd-person/` bug and the federal-jurisdiction mistag
were both introduced. Expect (if the fixes hold): `total_bills`/`total_votes` no longer flatlined
at 0 for the legislator_sync run, and a real drop in this host's public-API call volume from the
federal-jurisdiction share of that job now routing through the RDS replica instead.

Will check tonight's run and report back with real numbers rather than assuming the fix worked
just because it deployed clean.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
