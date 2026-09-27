# SYNC-76/SYNC-77/SYNC-78 all deployed and live on feat/rds-openstates-routing-standalone

**Re:** `sync78-ported-not-deployed-20260927.md` (this branch). The combined port (SYNC-76 +
SYNC-77 + SYNC-78, commits `ad3ab87`/`409efd5`/`bbb4c48`/`94c8231`) is deployed. Closing out this
thread.

## Deploy

2026-09-27 ~18:48 UTC, in a confirmed quiet window (checked `/schedule` first: the weekly
`legislator_sync`/`legislator_bio_sync` jobs had already run that morning and don't run again
until 2026-10-04; only the 30-min Voatz interval job was nearby, low-stakes to straddle):

```
.venv/bin/pip install .
sudo systemctl restart ddp-sync
```

Verified clean: `/health` healthy (Redis connected, Pinecone connected), `/schedule` still 14
jobs unchanged, zero errors/warnings in the startup logs. Confirmed directly against the running
installed package (not just the source tree) that all three fixes are actually loaded:

- `_get_sponsor_name(self, person_id, jurisdiction=None)` -- SYNC-78
- `WebflowSource._resolve_jurisdiction(self, jurisdiction_ref, seat=None)` -- SYNC-76
- `_ocd_person_id()` correctly prefixes bare UUIDs -- SYNC-77

## Where this leaves things

All three tickets' code is confirmed live on the branch this host actually runs. Today's earlier
manual validation run (`sync76-77-validation-run-results-20260927.md`) already exercised SYNC-76/
77 for real (RDS routing confirmed, `total_votes=3` real match); SYNC-78 wasn't in that run since
it landed after, but its own cherry-pick's test suite (282/282, including the new SYNC-78-specific
tests) passed clean. Next real-world exercise of all three together will be next Sunday's
(2026-10-04) `weekly_legislator_sync`/`weekly_legislator_bio_sync` runs -- will check those and
report real numbers rather than assume this is the end of it.

Still open, separately: the rate-limiting-against-our-own-replica ask from
`sync76-77-validation-run-results-20260927.md` -- no update on that from this side, flagging again
in case it needs its own ticket filed independently of this thread closing.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
