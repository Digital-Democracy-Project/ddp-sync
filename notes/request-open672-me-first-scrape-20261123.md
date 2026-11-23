# REQUEST (not a GO yet): OPEN-672, Maine's first scrape. Roll out the manifest that moves ME to `scraping`, then ONE manual run of its active sessions (`132R1`, `132S1`, `132R2`: the 132nd Legislature). 2026-11-23 (Mac side, Onboard New States)

**This waits for Ramon's go.** He gives it in your session or through the PM session; start only on it. Same shape as Maryland's (`2672ac8`).

## What I need from you (on Ramon's go)

### 1. Gates before anything
* **ddp-open-states PR #475 has merged.** It moves ME to `scraping` (branch `feat/OPEN-672-me-scraping`, head `d7e8786e`). It was made with `advance-jurisdiction`: `people_refresh`, `archive.enabled` and `cloud_path` true; LegBot, routing, search and embedding false. Until it merges, stop and wait.
* **New tasks run ddp-scrapers:81 (v79) or later.** Maine's fixes are all on scrapers main before v79:
    * OPEN-698 (#158, motion rules: Ought Not to Pass, per Ramon's 11-20 decision);
    * OPEN-739 (#188, session types);
    * OPEN-747 (#203): the First Regular, First Special and Second Regular Sessions are their own sessions; each bill and roll call is filed under the session it belongs to.
    * Report the revision and its image tag.
* **Nothing for `me` is running.** Not on Sunday from 02:00 UTC.

### 2. Roll out the manifest
* Pull first (main includes `ce2d4cc7`'s validator change), then `rollout-manifest.sh --host ec2 --ddp-sync-root /opt/ddp-sync`.
* If another rollout already brought main past #475, report the commit and skip.
* Report the host's commit, and that the combined check agrees.

### 3. One manual run
* Call `POST /ddp-sync/v1/trigger/openstates-scrape/me` once, with no retry, **without** `skip_people_check`.
* ME's people are loaded (`793d07c`: 623 created, 0 updated, with people fix OPEN-697). On the Mac replica: **188 current**: all 35 senators, and 153 representatives (the 151 seats plus the 2 tribal representatives). A 409 for people: stop and report.

**Gates (report each):**
* **Task revision:** 81 or later.
* **Sessions:** the "using active sessions" line names **`132R1`, `132S1` and `132R2`**.
    * The session menu was re-checked on 2026-11-23 against mainelegislature.org LawMakerWeb: all 13 entries are known to the scraper or ignored.
    * Stored dates match the Laws of Maine Legislative Statistics pages (Convened / Adjourned): 132R1 2024-12-04 to 2025-03-21; 132S1 2025-03-25 to 2025-06-25; 132R2 2026-01-07 to 2026-04-29.
* **Votes:** vote events > 0 in each of the three sessions, from both chambers.
* **Completion:** `status=completed failed=0`, and the archive finished. Report fetched and `fetch_errors`.
* **Counts:** the run id, bill and vote-event counts per session, and the archived-document count.
* **Time: watch this one.** LawMakerWeb keeps one bill list per Legislature, so each of the three sessions reads the 132nd's whole list and keeps its own bills (OPEN-747). The run reads every bill page about three times, and the scraper takes no `start=`. The probe took 63.8 s for 10 bills. The limit is 6 h per phase. If a phase passes it: stop and report the phase, the elapsed time and how far it got. Don't retry.
* **A load stopped by "already imported as ...":** stop and report the full message. ME sets vote keys (one per roll-call page), so this isn't expected.

### 4. Hand back
* Send me the run id in a `reply-` note here.
* I'll do the read-only checks on the Mac replica:
    * Stage 3 gates;
    * Senate vote names (OPEN-697);
    * motion labels (OPEN-698);
    * that each bill sits under one session only (OPEN-747);
    * action order;
    * version names.

## Things to know
* **LegBot hand-off:** ME's `legbot_replica` is false. The Mac refuses it; EC2 skips it once SYNC-186 is live there.
* **Weekly run:** from the rollout on, ME is in the weekly Sunday 02:00 UTC scrape (no timing entry).
* **Scorecards:** BROKER-564 (a yes on "Ought Not to Pass" is a vote against the bill) is a go-live gate, not part of this scrape.

## If it goes wrong
* **A failed run or failed gate:** stop and report; don't retry.
* **The way back:** a PR returning ME to `probing` (I'll write it), then a rollout.

## Not authorized
* Sessions before the 132nd Legislature.
* LegBot, routing, website, search or embedding for ME.
* A second run, or any other state.
* Editing ddp-sync's schedule or the manifest by hand.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
