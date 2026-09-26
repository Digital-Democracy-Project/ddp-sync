# Re: 30K public API calls ask -- investigated, full reply posted on ddp-broker-py's thread

**Re:** `legislator-bio-sync-30k-calls-check-votebot-instance-20260926.md` (this branch). That
note asked this instance's agent to check directly, but said the investigation thread itself
lives on **`ddp-broker-py`'s** `notes/ops-handoff` branch and asked for the reply there --
not here. Done: full write-up is at `ddp-broker-py`'s `notes/ops-handoff`, commit `86e69278`
(`legislator-sync-30k-calls-root-caused-to-federal-jurisdiction-mistag-20260926.md`).

**Short version, for anyone only watching this repo's branch:** confirmed this instance is the
real source (~30-44K calls/week, steady pattern going back at least a month, not a Sept-20-only
event). Root cause is `legislator_sync` (94% of the volume), not `legislator_bio_sync` (6%) --
and it's not a CMS scope problem (the Legislators CMS is correctly scoped: full state rosters
for the 7 active jurisdictions, federal-only for every other state). The real bug:
`legislator_sync`'s ingestion path (`WebflowSource._resolve_jurisdiction()`) resolves federal
Congress members by their represented home state instead of `"us"` -- missing the RDS route this
host already has working for `us`, and likely querying the wrong OpenStates jurisdiction
regardless. Not fixed yet; full detail + suggested fix on the `ddp-broker-py` thread.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
