# REQUEST (not a GO): OPEN-665, Rhode Island's first scrape again, on v79 (ddp-scrapers task revision 81), before Sunday. ONE manual run, no rollout. 2026-11-23 (Mac side, Onboard New States)

**This waits for Ramon's go in your session or through the PM session.** Per the PM session, Ramon wants the rerun now on v79 rather than leaving it to the weekly Sunday 11-29 02:00 UTC run. This note isn't that go; start only on it.

## Why
* Your `9b21ebe` (11-22 20:49 UTC): run `ri-8151f1638a5a` (rev 80) failed at the load with "already imported as HB 8500 in Rhode Island 2026 Regular Session". Nothing was written (RI has 0 bills and 0 vote events).
* **The cause:** the scraper searches Rhode Island's bill numbers in ranges whose ends overlapped, so a bill on a range edge (HB 8500) was collected twice.
    * OPEN-771 (scrapers #224, `3be4dfce`) makes the ranges end one number earlier.
    * Offline over the whole 2026 session: 11 repeated bills before the fix, 0 after.
* **v79 carries it:** ddp-scrapers:81, built about 03:30 UTC 11-23 at scrapers `0292506c` and core `42b48982`. The PM session checked OPEN-771 inside it (ddp-open-states notes `35b5cda`, `7b7eec0`).

## What I need from you (on Ramon's go)
### 1. Gates
* **New tasks run ddp-scrapers:81 (v79) or later.** Report the revision and image tag. Any revision before 81: stop and report.
* **Nothing for `ri` is running.**
* **Not on Sunday from 02:00 UTC.**
* **No rollout is needed:** RI is already at `scraping` on EC2 (`bce6eef`, checkout `5a0eb41b2`). Confirm the manifest still says `scraping` for ri, with `archive.enabled`, `people_refresh` and `cloud_path` true.

### 2. One manual run
* Call `POST /ddp-sync/v1/trigger/openstates-scrape/ri` once, with no retry, **without** `skip_people_check`. RI's people are loaded (`793d07c`; your last run read 116). A 409 for people: stop and report.

**Gates (report each), as in `request-open665-ri-first-scrape-v78-20261122.md`:**
* **Task revision:** 81 or later.
* **Sessions:** the "using active sessions" line names `2026`. Its stored dates are 2026-01-06 to 2026-06-11, from the House and Senate Journals.
* **Load:** it completes. **Another "already imported as ..." (bill or vote): stop and report the full message**, with the head of the message if you can get it, and the two objects' identifiers.
* **Votes:** vote events > 0. Rhode Island reads votes in a separate pass after the bills, by vote date.
* **Completion:** `status=completed failed=0`, and the archive finished. Report fetched and `fetch_errors`.
* **Counts:** the run id, bill count, vote-event count and archived-document count.
* **Time:** the last run's collection took about 30 min. The limit is 6 h.

### 3. Hand back
* Send me the run id in a `reply-` note here.
* I'll do the read-only Mac replica checks listed in the v78 request: Stage 3 gates, vote names, motion labels, consent-calendar links and action order. I'll also check that no bill number repeats.

## Things to know
* **LegBot hand-off:** RI's `legbot_replica` is false. If SYNC-186 (`9825134`) isn't released on EC2 by then, the archive may still hand RI to the Mac's LegBot, as for ND and KY. Report it if it happens; don't act on it.
* **Load core:** if EC2 ddp-sync hasn't been rebuilt at core `42b48982` yet (notes `22d7100`), the load uses the older core. RI doesn't need any core change since then.
* **The weekly Sunday 11-29 02:00 UTC run stays as it is.** If this rerun succeeds, Sunday's run is an ordinary weekly update.

## If it goes wrong
* A failed run or gate: stop and report; don't retry.
* The way back: a PR returning RI to `probing` (I'll write it), then a rollout. Don't hand-edit the manifest.

## Not authorized
* A second run.
* Any other state.
* LegBot, routing, website, search or embedding for RI.
* Editing ddp-sync's schedule or the manifest by hand.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
