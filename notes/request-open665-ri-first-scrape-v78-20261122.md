# REQUEST (not a GO yet): OPEN-665, Rhode Island's first scrape on v78 (ddp-scrapers task revision 80). Roll out the manifest that moves RI to `scraping`, then ONE manual run. 2026-11-22 (Mac side)

**This waits for Ramon's go.** Ramon gives it in your session or through the PM session; start only on his go. This note has the same shape as ND's (`go-open663-nd-first-scrape-v76-20261122.md`) and KY's (`request-open667-ky-first-scrape-v76-20261122.md`).

## What I need from you (on Ramon's go)

### 1. Gates before anything
* **ddp-open-states PR #462 has merged.** It moves RI to `scraping` (branch `feat/OPEN-665-ri-scraping`) and was made with `advance-jurisdiction`: `people_refresh`, `archive.enabled` and `cloud_path` true; LegBot, routing, search and embedding false. Until it merges, stop and wait.
* **New tasks run ddp-scrapers:80 (image v78) or later.** State Specific Handling verified v78 (ddp-open-states ops-handoff e27e5b0): scrapers f0ac9ef8, core 7f531958. It holds Rhode Island's fixes:
    * OPEN-683, OPEN-701 and OPEN-711 (votes, motion rules, Senate consent calendars);
    * OPEN-754 (session dates from the journals);
    * OPEN-756 (2021 ends 2022-01-04).
    * Report the revision and its image tag. Any revision before 80: stop and report.
* **Nothing for `ri` is running.**

### 2. Roll out the manifest
* Run `rollout-manifest.sh --host ec2 --ddp-sync-root /opt/ddp-sync`, as you did for ND (9c9c321).
* If the host's validator is older than main's, pull first, then run the script (OPEN-532).
* Report the host's commit, and that the combined check agrees.

### 3. One manual run
* Call `POST /ddp-sync/v1/trigger/openstates-scrape/ri` once, with no retry, **without** `skip_people_check`.
* RI's people are loaded: 793d07c, 264 created, 0 updated; 112 current. A 409 for people: stop and report.

**Gates (report each):**
* **Task revision:** the revision the task runs.
* **Sessions:** the "using active sessions" line should name `2026`.
    * The menu was re-checked on 2026-11-22: all entries are known to the scraper or ignored.
    * 2026's stored dates are 2026-01-06 to 2026-06-11, from the House and Senate Journals.
* **Votes:** vote events > 0.
    * Rhode Island reads votes in a separate pass after the bills, by vote date (the 10-31 probe ended before that pass and saw none).
    * Senate consent calendars now link to their bills (OPEN-711; offline, about 410 bill votes from 59 roll calls).
* **Completion:** `status=completed failed=0`, and the archive finished. Report fetched and `fetch_errors`.
* **Counts:** the run id, bill count, vote-event count and archived-document count.
* **Time:** the probe took 2.0 s for 10 bills, plus the votes pass. The limit is 6 h.
* **A load stopped by a repeated vote key** ("already imported as ..."): stop and report the full message. RI sets a vote key, so this isn't expected.

### 4. Hand back
* Send me the run id in a `reply-` note here.
* I'll do the read-only checks on the Mac replica:
    * Stage 3 gates;
    * vote names (gate J was never measured for RI);
    * motion labels;
    * consent-calendar links;
    * action order.

## Side effect (Ramon knows)
* From the rollout on, RI is in the **weekly Sunday 02:00 UTC** scrape (no timing entry).
* The next one is 11-29, so please do the manual run before then.

## If it goes wrong
* **A failed run or failed gate:** stop and report; don't retry.
* **The way back:** a PR returning RI to `probing` (I'll write it), then a rollout. Don't hand-edit the manifest on the host.

## Not authorized
* LegBot, routing, website, search or embedding for RI.
* A second run, or any other state.
* Editing ddp-sync's schedule or the manifest by hand.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
