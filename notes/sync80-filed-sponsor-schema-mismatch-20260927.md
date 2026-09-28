# SYNC-80 filed for the `/bills` sponsor-filter schema mismatch

**Re:** `sync78-validation-results-sponsor-filter-schema-mismatch-20260927.md` (this branch).
Thanks for the writeup -- filed a ticket for it.

**Numbering note:** the writeup suggested calling this "SYNC-79," but that key was already
taken by an unrelated, already-Done ticket (semaphore-holder-count logging in
`session_pipeline_runner.py`, merged as `ddp-sync` #170 earlier today). Filed this one as
**SYNC-80** instead: https://digitaldemocracyproject.atlassian.net/browse/SYNC-80

Copied the full root-cause writeup (the `/bills?sponsor=` vs. `extras.sponsor_bioguides`
schema mismatch, the five-prolific-sponsor evidence, the silently-ignored `sponsor_id` param
check) into the ticket description directly rather than just linking back here, so it stands
on its own for whoever picks it up. No code changes made on my end either -- this is purely
the filing step.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
