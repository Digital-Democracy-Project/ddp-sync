# REQUEST (not a GO yet): OPEN-664, Idaho's first scrape on v79 (ddp-scrapers task revision 81). Roll out the manifest that moves ID to `scraping`, then ONE manual run. 2026-11-23 (Mac side, Onboard New States)

**This waits for Ramon's go.** Ramon gives it in your session or through the PM session; start only on his go. This note has the same shape as OK's (`request-open666-ok-first-scrape-v78-20261122.md`) and RI's.

## What I need from you (on Ramon's go)

### 1. Gates before anything
* **ddp-open-states PR #461 has merged.** It moves ID to `scraping` (branch `feat/OPEN-664-id-scraping`, head `3b165e35`, mergeable). It was made with `advance-jurisdiction`: `people_refresh`, `archive.enabled` and `cloud_path` true; LegBot, routing, search and embedding false. Until it merges, stop and wait.
* **New tasks run ddp-scrapers:81 (image v79) or later.**
    * v79 was built by Ramon at about 03:30 UTC on 11-23, at scrapers main `0292506c` and core `42b48982`; the PM session checked inside it.
    * It holds Idaho's fixes:
        * OPEN-680 (district-suffixed names and "Mr. Speaker");
        * OPEN-682 (motion rules);
        * OPEN-753 (session dates);
        * OPEN-769 (each vote gets its own key, the North Dakota failure's fix);
        * OPEN-770 (each roll call stored under the chamber that took it; ddp-open-states notes `7b7eec0`).
    * Report the revision and its image tag. Any revision before 81: stop and report.
* **Nothing for `id` is running.**
* **Not on Sunday from 02:00 UTC** (the weekly states start then).

### 2. Roll out the manifest
* Run `rollout-manifest.sh --host ec2 --ddp-sync-root /opt/ddp-sync`, as for OK.
* If the host's validator is older than main's, pull first, then run the script (OPEN-532).
* Report the host's commit, and that the combined check agrees.

### 3. One manual run
* Call `POST /ddp-sync/v1/trigger/openstates-scrape/id` once, with no retry, **without** `skip_people_check`.
* Idaho's people are loaded (your `793d07c`: 321 created, 0 updated, 104 read back). On the Mac replica (read-only, 11-23): 312 people, **105 current**, which is all 35 senators and 70 representatives. A 409 for people: stop and report.

**Gates (report each):**
* **Task revision:** the revision the task runs.
* **Sessions:** the "using active sessions" line should name **`2026`**.
    * The session menu was re-checked on 2026-11-23 against legislature.idaho.gov/sessioninfo/: all 34 entries are known to the scraper or ignored.
    * 2026's dates are **2026-01-12 to 2026-04-02**, from the legislature's own Session Dates page (OPEN-753).
* **Votes:** vote events > 0, from both chambers.
* **Completion:** `status=completed failed=0`, and the archive finished. Report fetched and `fetch_errors`.
* **Counts:** the run id, the bill count, the vote-event count and the archived-document count. The OPEN-770 offline fetch on 11-22 found 763 bill pages for 2026.
* **Time:** the probe took 12.4 s for 10 bills, and the bills scraper always walks the whole session, so expect well under 1 h plus the archive. The limit is 6 h.
* **A load stopped by a repeated vote key** ("already imported as ..."): stop and report the full message. ID sets a vote key now (OPEN-769), so this isn't expected.

### 4. Hand back
* Send me the run id in a `reply-` note here.
* I'll do the read-only checks on the Mac replica:
    * Stage 3 gates;
    * vote names (OPEN-680 and OPEN-681);
    * motion labels (OPEN-682);
    * vote chambers (OPEN-770: 2026 H 760's 03/25 roll call should be a Senate vote);
    * action order.

## Things to know
* **LegBot hand-off:** ID's `legbot_replica` is false. If SYNC-186 (ddp-sync `b569cf2`, request `9825134`) is **not** released on EC2 by then, the archive may still hand ID to the Mac's LegBot, as for ND and KY on 11-22. Report it if it happens; don't act on it.
* **Load core:** cloud loads use EC2 ddp-sync's own core (SYNC-180). If the ddp-sync rebuild at core `42b48982` (notes `22d7100`) hasn't happened, Idaho loads with the older core. Idaho doesn't need any core change since then, so that is fine. Just report the core the load used, if your logs show it.

## Side effect (Ramon knows)
* From the rollout on, ID is in the **weekly Sunday 02:00 UTC** scrape (no timing entry).
* The next one is 11-29, so please do the manual run before then.

## If it goes wrong
* **A failed run or failed gate:** stop and report; don't retry.
* **The way back:** a PR returning ID to `probing` (I'll write it), then a rollout. Don't hand-edit the manifest on the host.

## Not authorized
* LegBot, routing, website, search or embedding for ID.
* A second run, any other state, or older Idaho sessions.
* Editing ddp-sync's schedule or the manifest by hand.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
