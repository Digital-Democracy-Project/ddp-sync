# REQUEST (not a GO yet): OPEN-668, Hawaii's first scrape. Roll out the manifest that moves HI to `scraping`, then ONE manual run of its active session (`2026`). 2026-11-23 (Mac side, Onboard New States)

**This waits for Ramon's go.** He gives it in your session or through the PM session; start only on it. Same shape as Maryland's (`2672ac8`).

## What I need from you (on Ramon's go)

### 1. Gates before anything
* **ddp-open-states PR #474 has merged.** It moves HI to `scraping` (branch `feat/OPEN-668-hi-scraping`, head `fd0fc429`). It was made with `advance-jurisdiction`: `people_refresh`, `archive.enabled` and `cloud_path` true; LegBot, routing, search and embedding false. Until it merges, stop and wait.
* **New tasks run ddp-scrapers:81 (v79) or later.** Hawaii's fixes are all on scrapers main before v79:
    * OPEN-691 (#155, motion rules);
    * OPEN-709 (#164, #165): House floor roll calls are kept (the probe saw none), plus the Senate tally forms;
    * OPEN-710 (#167): the vote result is the stated outcome, so adopted resolutions and overrides aren't stored as failed;
    * OPEN-729 (#178, session types);
    * OPEN-748 (#205, special sessions as their own sessions).
    * Report the revision and its image tag.
* **Nothing for `hi` is running.** Not on Sunday from 02:00 UTC.

### 2. Roll out the manifest
* Pull first (main includes `ce2d4cc7`'s validator change), then `rollout-manifest.sh --host ec2 --ddp-sync-root /opt/ddp-sync`.
* If another rollout already brought main past #474, report the commit and skip.
* Report the host's commit, and that the combined check agrees.

### 3. One manual run
* Call `POST /ddp-sync/v1/trigger/openstates-scrape/hi` once, with no retry, **without** `skip_people_check`.
* HI's people are loaded (`793d07c`: 194 created, 0 updated, with people fixes OPEN-690 and OPEN-716). On the Mac replica: **76 current**, all 25 senators and 51 representatives. A 409 for people: stop and report.

**Gates (report each):**
* **Task revision:** 81 or later.
* **Sessions:** the "using active sessions" line names **`2026`** only.
    * The session menu was re-checked on 2026-11-23 against data.capitol.hawaii.gov: all 27 entries are known to the scraper or ignored.
    * Stored dates: 2026 runs 2026-01-21 to 2026-05-08. The Senate journal for 05-08-26 (Day 58) records the sine die adjournment.
    * No 2025 or 2026 special session is in the legislature's journals.
* **Votes:** vote events > 0 in 2026, from **both chambers**. House floor roll calls are OPEN-709's fix: the probe saw none.
* **Completion:** `status=completed failed=0`, and the archive finished. Report fetched and `fetch_errors`.
* **Counts:** the run id, bill and vote-event counts, and the archived-document count.
* **Time:** the bills scraper takes no `start=`, so this is a full walk of 2026 (every weekly run will be too). The probe took 16.7 s for 10 bills. The limit is 6 h per phase. If a phase passes it: stop and report.
* **A load stopped by "already imported as ...":** stop and report the full message. HI sets vote keys, so this isn't expected.

### 4. Hand back
* Send me the run id in a `reply-` note here.
* I'll do the read-only checks on the Mac replica:
    * Stage 3 gates;
    * vote names (OPEN-690);
    * motion labels (OPEN-691);
    * House floor roll calls and results (OPEN-709, OPEN-710);
    * action order;
    * version names.

## Things to know
* **LegBot hand-off:** HI's `legbot_replica` is false. The Mac refuses it; EC2 skips it once SYNC-186 is live there.
* **Weekly run:** from the rollout on, HI is in the weekly Sunday 02:00 UTC scrape (no timing entry).

## If it goes wrong
* **A failed run or failed gate:** stop and report; don't retry.
* **The way back:** a PR returning HI to `probing` (I'll write it), then a rollout.

## Not authorized
* Older Hawaii sessions, special sessions included.
* LegBot, routing, website, search or embedding for HI.
* A second run, or any other state.
* Editing ddp-sync's schedule or the manifest by hand.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
