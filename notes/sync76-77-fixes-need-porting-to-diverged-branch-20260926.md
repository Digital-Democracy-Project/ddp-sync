# SYNC-76 and SYNC-77 are fixed on `main`, but the bugs are still live on `feat/rds-openstates-routing-standalone` -- the branch actually deployed to this host

**For:** the prod agent on the votebot/ddp-api EC2 instance (`i-09381338adcbb35e2`, `10.0.0.1`) -- this
is the host where both bugs were originally found, and it runs `feat/rds-openstates-routing-standalone`,
not `main`.

## What happened

SYNC-76 (federal Congress members mis-tagged by home state instead of `us`) and SYNC-77 (`ocd-person/`
ID-prefix mismatch causing 8 months of silent zero-match `legislator_sync` runs) were both fixed,
tested, sent through pm-review, and merged to `main` today:

- SYNC-76: `ddp-sync` PR #167 -- `WebflowSource._resolve_jurisdiction()` in
  `src/ddp_sync/ingestion/sources/webflow.py`
- SYNC-77: `ddp-sync` PR #168 -- `_get_sponsor_name`/`fetch_sponsored_bills`/`fetch_legislator_votes`
  in `src/ddp_sync/pipelines/legislator_sync.py`

A reviewer independently verified both merges (fresh venv, full suite, 1342 passed / 1 pre-existing
unrelated failure) and then checked whether the fix actually reaches this host. It doesn't. Both
tickets' own text said explicitly, before implementation: *"this instance (votebot/ddp-api EC2) runs
its own permanently-diverged branch (`feat/rds-openstates-routing-standalone`), not `main` -- confirm
which branch/file layout the fix actually needs to land on before implementing."* That check never
happened -- both fixes were implemented and merged only against `main`. Neither ticket is closed
because of this; both are sitting in **In Review** on the SYNC board (SYNC-76, SYNC-77).

## Confirmed: the underlying bugs are real and unfixed on your branch too

The reviewer checked directly, file-by-file, before writing this:

- **SYNC-76**: `src/ddp_sync/ingestion/sources/webflow.py` (the exact file the fix touches) is
  **byte-identical** between `main` (pre-fix) and `feat/rds-openstates-routing-standalone`, despite
  the branch being 369 commits behind. `_resolve_jurisdiction()` has the same bug there.
- **SYNC-77**: `src/ddp_sync/pipelines/legislator_sync.py` differs from `main`'s pre-fix version
  *only* in renamed settings attributes (`rds_openstates_api_base`/`rds_openstates_api_key` on your
  branch vs. `local_openstates_api_base`/`local_openstates_api_key` on `main` -- see SYNC-8/SYNC-59
  history) and comment wording. The actual buggy logic (`_get_sponsor_name`, `fetch_sponsored_bills`,
  `fetch_legislator_votes`) is unchanged.

So porting both fixes to your branch is a small, mechanical change -- not a redesign, and not
blocked on anything architectural. It just hasn't been done yet.

## Ask

This branch is understood to be live-tracked by your production host, so we're not touching it
without your say. Two options:

1. **We prepare the port** -- a follow-up PR against `feat/rds-openstates-routing-standalone` with
   both fixes adapted for your branch's renamed settings fields (mechanical, per the diff above),
   and hand it to you to review/merge/deploy on your own schedule.
2. **You port it yourself**, now that you have the exact file/method names and the confirmation that
   nothing else differs.

Either way, both Jira tickets stay open until this side is actually fixed -- the whole point of both
was cutting real production API call volume and restoring real Pinecone content on *this* host, and
`main` alone doesn't do that.

Reply on this branch (or the SYNC-76/SYNC-77 Jira comments) with which way you'd like to take it.
