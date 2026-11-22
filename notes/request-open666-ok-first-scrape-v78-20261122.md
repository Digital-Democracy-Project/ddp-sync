# REQUEST (not a GO yet): OPEN-666, Oklahoma's first scrape on v78 (ddp-scrapers task revision 80). Roll out the manifest that moves OK to `scraping`, then ONE manual run. 2026-11-22 (Mac side)

**This waits for Ramon's go.** Ramon gives it in your session or through the PM session; start only on his go. This note has the same shape as ND's and RI's (`request-open665-ri-first-scrape-v78-20261122.md`).

## What I need from you (on Ramon's go)

### 1. Gates before anything
* **ddp-open-states PR #463 has merged.** It moves OK to `scraping` (branch `feat/OPEN-666-ok-scraping`) and was made with `advance-jurisdiction`: `people_refresh`, `archive.enabled` and `cloud_path` true; LegBot, routing, search and embedding false. Until it merges, stop and wait.
* **New tasks run ddp-scrapers:80 (image v78) or later.** v78 was verified in ddp-open-states ops-handoff e27e5b0 (scrapers f0ac9ef8). It holds Oklahoma's fixes:
    * OPEN-687 and OPEN-704 (motion rules, supermajority votes);
    * OPEN-727 (session types);
    * OPEN-755 (session dates from the journals);
    * OPEN-758 (bills by filing year).
    * Report the revision and its image tag. Any revision before 80: stop and report.
* **Nothing for `ok` is running.**

### 2. Roll out the manifest
* Run `rollout-manifest.sh --host ec2 --ddp-sync-root /opt/ddp-sync`, as for ND.
* If the host's validator is older than main's, pull first, then run the script (OPEN-532).
* Report the host's commit, and that the combined check agrees.

### 3. One manual run
* Call `POST /ddp-sync/v1/trigger/openstates-scrape/ok` once, with no retry, **without** `skip_people_check`.
* OK's people are loaded: 793d07c, 409 created, 0 updated; 146 current. A 409 for people: stop and report.

**Gates (report each):**
* **Task revision:** the revision the task runs.
* **Sessions:** the "using active sessions" line should name **`2025` and `2026`**.
    * Both are active on purpose (OPEN-758). Each reads the legislature's 2026 list and keeps the bills first acted on in its own year, so 2025's carried-over bills stay under 2025.
    * The session menu was re-checked on 2026-11-22: all 54 entries are known to the scraper or ignored.
* **Votes:** vote events > 0, in both sessions. A roll call is filed under the session of its own date.
* **Completion:** `status=completed failed=0`, and the archive finished. Report fetched and `fetch_errors`.
* **Counts:** the run id, the bill count per session, the vote-event count and the archived-document count.
* **Time:** this is the one to watch. There are about 12,000 bill pages across the two sessions, at about 1 s each in the probe, so expect roughly 3.5–5 h. The limit is the default 6 h.
    * Report the total runtime.
    * If it hits the limit, stop and report; don't retry. I'll propose a per-state timeout.
* **A load stopped by a repeated vote key:** stop and report the full message. OK sets a vote key (`url#rcs`), so this isn't expected.

### 4. Hand back
* Send me the run id in a `reply-` note here.
* I'll do the read-only checks on the Mac replica:
    * Stage 3 gates;
    * vote names;
    * motion labels;
    * that no bill sits under both 2025 and 2026;
    * action order.

## Side effect (Ramon knows)
* From the rollout on, OK is in the **weekly Sunday 02:00 UTC** scrape (no timing entry).
* The next one is 11-29, so please do the manual run before then.
* Ramon accepted the longer run (~+1.75 h) from OPEN-758.

## If it goes wrong
* **A failed run or failed gate:** stop and report; don't retry.
* **The way back:** a PR returning OK to `probing` (I'll write it), then a rollout. Don't hand-edit the manifest on the host.

## Not authorized
* LegBot, routing, website, search or embedding for OK.
* A second run, any other state, or older Oklahoma sessions (a later decision of Ramon's).
* Editing ddp-sync's schedule or the manifest by hand.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
