# `legislator_sync` has been silently matching zero bills and zero votes for ~8 months -- an ID-prefix bug, not (just) the federal-jurisdiction mistag

**Re:** follow-up to `legislator-sync-30k-calls-pointer-to-ddp-broker-py-reply-20260926.md` (this
branch) / `legislator-sync-30k-calls-root-caused-to-federal-jurisdiction-mistag-20260926.md`
(`ddp-broker-py`'s `notes/ops-handoff`). That note correctly identified `legislator_sync` as the
volume driver and found a real jurisdiction-routing bug for federal Congress members. This note
covers a **second, separate, more severe bug** found while digging into whether that mistag was
causing silent match failures. Investigated read-only: `journalctl`, live read-only `GET`s
against the public OpenStates API (matching normal production traffic, no writes), and `git log`
across both this repo and `votebot` (where this code originated before extraction). No code
changed.

## What `legislator_sync` is meant to do

Keeps each legislator's **sponsored-bills and voting-record history** current in Pinecone
(VoteBot's RAG/chat vector store), so VoteBot can cite "what bills has this legislator sponsored"
/ "how did they vote on X" with a DDP-linked source. Distinct from `legislator_bio_sync` (bio/
contact fields written to the Webflow CMS, not Pinecone) and `bill_sync` (bill status/text, not
per-legislator activity). Runs weekly (Sundays 06:00 UTC, config: `legislator_sync` in
`config/sync_schedule.yaml`), rotating 200 of the CMS's 1,472 legislators per run via a
date-based offset to conserve OpenStates API quota.

## The bug: every legislator's sponsor/vote lookup fails silently, every week, for ~8 months

Confirmed via `journalctl` across all 4 available weekly runs (Aug 30, Sep 6, Sep 13, Sep 20 --
logs only go back to this host's 2026-07-29 boot, so this is as far back as this host itself can
confirm, but see history below for how far the bug itself goes back):

```
Legislator sync complete  successful=200 failed=0 total_bills=0 total_votes=0
```

Identical every single week. 200/200 legislators marked "successful," zero real matches, ever,
no errors, no failures -- the exact silent-failure shape, not a crash or a logged error anywhere.

**Root cause: an `ocd-person/` ID-prefix mismatch.** The Legislators CMS's `openstatesid` field
stores a bare UUID (e.g. `c495bde9-29b3-5e71-ab0e-dedff0de9a84`), but OpenStates always uses the
full `ocd-person/<uuid>` format for person IDs, in both `/people?id=` lookups and every
`voter.id` / sponsor `person.id` field in bill and vote data. `legislator_sync.py` never adds
this prefix before using the raw CMS value. Verified live against the real public API (read-only,
same traffic shape as normal production calls):

```
GET /people?id=c495bde9-...            (bare, as stored in CMS) -> 200 OK, 0 results
GET /people?id=ocd-person/c495bde9-... (correct format)          -> 200 OK, 1 result (Trent Kelly)
```

And a real FL bill's actual vote record: `voter.id = 'ocd-person/e4a84025-...'` -- always
prefixed, never bare, confirming the same mismatch also breaks the votes-matching comparison
(`voter_id == person_id` in `_extract_legislator_votes_from_bill()`, a plain Python string
comparison on already-downloaded data, not a second API call).

**Crucially, this never surfaces as an HTTP failure.** Confirmed the full day's status-code
breakdown for 2026-09-20 was 200/429/502 only -- no 404s, nothing that would look like an error.
OpenStates responds `200 OK` with an empty/non-matching result either way; the code has no signal
that the ID it used was wrong. That's why this went undetected for 8 months: it never throws,
never logs an error, never shows up in `failed=` counts -- only in `total_bills=0`/`total_votes=0`
at the bottom of an otherwise "successful" run summary.

## This affects every legislator, not just the federal-mistagged ones

Unlike the jurisdiction-routing bug (which only misroutes ~400 federal Congress members outside
the 7 RDS-covered jurisdictions), this ID mismatch breaks matching universally -- the 7
DDP-active-state legislators (FL/VA/MI/WA/UT/AZ/MA) are affected exactly the same as everyone
else. The two bugs are independent and compound: fixing the jurisdiction mistag alone would not
fix this; fixing this alone would not fix the jurisdiction mistag.

## How long this has been broken

Traced through both `ddp-sync`'s and `votebot`'s git history (this pipeline was ported wholesale
from VoteBot -- `ddp-sync`'s very first commit already contains the bug):

- **`_get_sponsor_name()`'s bare-ID lookup**: introduced in VoteBot commit `5d68f44` ("Fix
  OpenStates API sponsor filtering in legislator sync"), **2026-01-31**. The docstring literally
  shows `"ocd-person/6a3fae94-..."` as the expected format, but the code passes `person_id`
  through unprefixed, assuming the caller already formats it correctly. It never did.
- **`fetch_legislator_votes`'s `voter_id == person_id` bug**: introduced alongside vote-syncing
  itself, VoteBot commit `59d1402` ("Add legislator vote syncing..."), **2026-02-03**.
- **`ddp-sync`'s first commit** (`a0f2c5f`, "Phase 1: Create ddp-sync service with VoteBot sync
  code", **2026-03-10**) already contains this code unchanged, and none of the 507 commits since
  have touched it.
- **This exact bug class was already found and fixed once, elsewhere, but never here**: commit
  `6a97206` ("fix(scripts): prepend ocd-person/ prefix before OpenStates lookup") fixed the same
  ID-format problem in `scripts/backfill_legislator_party.py`, a separate one-off script -- that
  fix was never applied back to `legislator_sync.py`'s core pipeline.

Net: ~8 months of `sync_votes: true` / `sync_sponsored_bills: true` running weekly, at real
OpenStates API cost, producing zero usable Pinecone content the entire time.

## Reconciling the API-call volume with "everything fails" (a fair question raised while digging into this)

Worth spelling out since it wasn't obvious at first: the ~30K/week call volume this thread has
been tracking is **not reduced by this bug, and isn't really "caused" by it either** -- it comes
from a separate part of the same job. `fetch_legislator_votes()` does a full per-bill sweep
(paginate the bill list, then fetch each individual bill's full detail with `include=votes`) for
up to 200 bills **per legislator**, unconditionally, regardless of whether the eventual
`voter_id == person_id` check will succeed. Confirmed exactly against 2026-09-20's logs:

```
27,939  GET /bills/{id}?include=votes    (individual bill-detail fetches)
 2,354  GET /bills?jurisdiction=...      (paginated bill-list fetches)
────────
30,293  total -- exact match to the day's public-API /bills total
```

The `{bill_id}` in those detail-fetch URLs is never derived from the legislator's `person_id` --
it comes straight from OpenStates' own preceding bill-list response, already correctly formatted.
So those requests were never going to fail on ID-format grounds; they succeed normally, and the
silent mismatch only discards the result afterward, client-side, once vote data has already been
fully downloaded. The one piece of this job that *is* directly gated by the bad ID
(`_get_sponsor_name()`'s `/people?id=` lookup, feeding `fetch_sponsored_bills()`) fails fast and
contributes only ~200 calls/week to the total -- not the volume driver.

## Not applied -- flagging both fixes together for whoever picks this up

Neither the jurisdiction mistag nor this ID-prefix bug has been fixed. Suggested approach for
this one: normalize `person_id` to the `ocd-person/`-prefixed form once, at the point it's read
from the CMS (`fields.get("openstatesid", "")`) or immediately before it's used in
`_get_sponsor_name()` / compared in `_extract_legislator_votes_from_bill()` -- mirroring the
pattern `scripts/backfill_legislator_party.py` already uses. Combined with the federal-seat
jurisdiction fix from the earlier note, this should let `legislator_sync` actually produce real
content instead of just paying for API calls that can never match.

Happy to implement either or both fixes if wanted -- held off so far since this was purely an
investigation ask.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
