# Flag: Pinecone bill-text ingestion still live-fetches from the public legislative site -- not covered by this cutover

**Date:** 2026-09-16
**Found by:** an agent walking the votebot/ddp-api EC2 upgrade with the user, while confirming
an assumption before executing the sibling handoff docs.
**Audience:** Ramon -- flagging a scope gap, not a blocker for the OpenStates-repointing or
ddp-broker-py steps already documented in the sibling notes.

## The assumption that turned out to be wrong

The working assumption going into this upgrade was: once this host points at the local
OpenStates RDS-backed instance (`local_openstates_api_base`, SYNC-6), bill *documents* for
Pinecone ingestion would also come from wherever ddp-open-states archives them (S3/Postgres)
instead of being re-downloaded from the public state legislative website on every new version.

That's not what the current code on `main` does.

## What actually happens (verified against `bill_version.py` on `main`)

- `_get_best_text_url()` derives the document URL from the OpenStates version data's own
  `links[]`/`text_url` -- i.e. the state site's PDF/HTML link (e.g. `flsenate.gov/...`) --
  regardless of whether that OpenStates data came from the public API or the local RDS-backed
  instance. SYNC-6's routing only changes *where bill metadata/actions* come from; it doesn't
  change what URL a bill *version* points to.
- `_ingest_bill_text()` -- the method that produces the primary `bill-text` Pinecone chunks --
  downloads that URL live via `WebflowSource._process_bill_pdf()` / `_process_bill_html()`,
  exactly as it does today. No S3/archive path is involved.
- The **only** place archived text is used is `_generate_and_ingest_changelog()`, and only for
  the *old* version's side of the diff: it prefers
  `local_openstates_client.get_archived_changelog_inputs()` (api-v3's precomputed
  `diff_from_previous_version` + prior `raw_text`, backed by Postgres, not S3) over re-downloading
  `old_version["text_url"]`. That's a re-fetch-avoidance optimization for the changelog only --
  it doesn't touch the primary `bill-text`/`bill-text-history` ingestion path at all.
- `local_openstates_client.py`'s own docstring says this directly: it is *"Deliberately NOT the
  site-wide OPENSTATES_API_BASE cutover PLAN-local-openstates-migration.md scopes (Pinecone
  re-keying, VoteBot, universal ingestion) -- that's a much bigger, explicitly out-of-scope
  migration."*
- `S3_BILL_ARCHIVE_BUCKET` does exist in this codebase, but it's only read by
  `openstates_backfill.py`'s Fargate `os-text-extract` maintenance commands
  (`reextract`/`refresh-extraction`/`recompute-diff-order`) -- an on-demand data-quality tool,
  unrelated to the nightly `bill_sync`/Pinecone ingestion path this host runs.

## Net effect for this host

Nothing in the sibling handoff docs' steps needs to change -- the OpenStates repointing and the
ddp-broker-py dependency are both real and both still land as described. But after completing
both, this host's Pinecone bill-text ingestion will still hit the public legislative website
directly for every new bill version, same as before the upgrade. If "stop hitting the public
site for documents too" was assumed to be part of this cutover, it isn't yet -- that's the
separate, not-yet-scheduled `PLAN-local-openstates-migration.md` universal-ingestion cutover.

## Open item

Is there already a ticket tracking the universal-ingestion / Pinecone-re-keying cutover
referenced by `PLAN-local-openstates-migration.md`? Asked the dev agent to check OPEN/SYNC/INFRA
and file one if not, scoped narrowly to "route `bill_version.py`'s document-text fetch through
the local archive instead of the public site" -- separate from the two items already in flight
in the sibling docs.
