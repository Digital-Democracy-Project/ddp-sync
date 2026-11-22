# REQUEST (not a GO yet): OPEN-667, Kentucky's first scrape on v76 (ddp-scrapers task revision 78). Roll out the manifest that moves KY to `scraping`, then ONE manual run. 2026-11-22 (Mac side)

**Ramon, 2026-11-22 (/ask-me in the PM session, recorded on OPEN-667):** "Yes, write the request".
* Ramon brings his go for the run to you himself, so **start only when he says go in your session.**
* This note has the same shape as North Dakota's (`go-open663-nd-first-scrape-v76-20261122.md`).

## What I need from you (on Ramon's go)

### 1. Gates before anything
* **ddp-open-states PR #457 has merged.** It puts KY at `scraping` (branch `feat/OPEN-667-ky-scraping`) and was made with `advance-jurisdiction`: `people_refresh`, `archive.enabled` and `cloud_path` true; LegBot, routing, search and embedding false. Until it merges, stop here and wait.
* **New tasks run ddp-scrapers:78 (image v76),** or a later revision. That image holds Kentucky's fixes: OPEN-688 (each vote filed under the chamber that cast it; #143) and OPEN-689 (motion rules; #149). Report the revision. If it is a later one, report its image tag and go ahead; any revision before 78: stop and report.
* **Nothing for `ky` is running.**

### 2. Roll out the manifest
* Run `rollout-manifest.sh --host ec2 --ddp-sync-root /opt/ddp-sync` as you did for ND (9c9c321).
* Report the commit the host now has, and that the combined check agrees.
* `--skip-ecs-check` is fine only for the same reason as last time: another state's task is running, and nothing for ky.

### 3. One manual run
* Call `POST /ddp-sync/v1/trigger/openstates-scrape/ky` once, with no retry, **without** `skip_people_check`.
* KY's legislators are loaded: prod checkpoint 793d07c, 347 created, 0 updated; 138 current on the Mac replica. A 409 for people: stop and report.

**Gates (report each):**
* **Task revision:** report the revision the task runs.
* **Sessions:** the "using active sessions" line should name `2026RS`.
    * The session menu was re-checked on 2026-11-22: all entries are known to the scraper or ignored.
    * The 26RS Legislative Record says "Adjourned Wednesday, April 15, 2026, Sine Die", matching the scraper's dates.
* **Votes:** vote events > 0. They come from the bills scraper's roll-call PDFs (`votes_scraper: false`); the probe sample had 28 roll calls, 18 Senate and 10 House.
* **Completion:** `status=completed failed=0`, and the archive finished. Report fetched and `fetch_errors`; those are a report, not a retry.
* **Counts:** report the run id, bill count, vote-event count and archived-document count.
* **Time:** the probe took 20.1 s for 10 bills, so expect about an hour or two. The limit is 6 h.
* **A load stopped by a repeated vote key** ("already imported as ..."), like ND's on 11-22 (OPEN-757): stop and report the full message. Nothing is written in that case.

### 4. Hand back
* Send me the run id in a `reply-` note here.
* I'll do the read-only checks on the Mac replica:
    * Stage 3 gates;
    * vote chambers (OPEN-688) and motion labels (OPEN-689);
    * the unmatched voter "Lewis C" (10 of 1,684 in the sample);
    * action order.

## Side effect (Ramon knows)
From the rollout on, KY is in the **weekly Sunday 02:00 UTC** scrape. It has no timing entry, so the default applies, and the next one is 11-29. Please do the manual run before then, so the first run is the supervised one.

## If it goes wrong
* **A failed run or failed gate:** stop and report. Don't retry.
* **The way back:** a PR returning KY to `probing` (I'll write it), then a rollout. Don't hand-edit the manifest on the host.

## Not authorized
* LegBot for KY, routing, website, search or embedding.
* A second run, or any other state.
* Editing ddp-sync's schedule or the manifest by hand.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
