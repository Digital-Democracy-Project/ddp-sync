# REQUEST (not a GO yet): OPEN-674, Maryland's first scrape. Roll out the manifest that moves MD to `scraping`, then ONE manual run of its active sessions (`2026`, `2026s1`). 2026-11-23 (Mac side, Onboard New States)

**This waits for Ramon's go.** He gives it in your session or through the PM session; start only on it. Same shape as Idaho's (`46ab1e0`).

## What I need from you (on Ramon's go)

### 1. Gates before anything
* **ddp-open-states PR #473 has merged.** It moves MD to `scraping` (branch `feat/OPEN-674-md-scraping`, head `4b2b66ef`). It was made with `advance-jurisdiction`: `people_refresh`, `archive.enabled` and `cloud_path` true; LegBot, routing, search and embedding false. Until it merges, stop and wait.
* **New tasks run ddp-scrapers:81 (v79) or later.** Maryland's fixes are all on scrapers main before v79:
    * OPEN-700 (#166, motion rules);
    * OPEN-717 (#168): the votes scraper asks for the session, instead of the site's empty 2027 default, which gave 0 votes;
    * OPEN-718 (#169, veto overrides and full consent calendars);
    * OPEN-742 (#192, the 2026 Special Session).
    * Report the revision and its image tag.
* **Nothing for `md` is running.** Not on Sunday from 02:00 UTC.

### 2. Roll out the manifest
* Pull first (main includes `ce2d4cc7`'s validator change), then `rollout-manifest.sh --host ec2 --ddp-sync-root /opt/ddp-sync`.
* If another rollout already brought main past #473, report the commit and skip.
* Report the host's commit, and that the combined check agrees.

### 3. One manual run
* Call `POST /ddp-sync/v1/trigger/openstates-scrape/md` once, with no retry, **without** `skip_people_check`.
* MD's people are loaded (`793d07c`: 469 created, 0 updated). On the Mac replica: **188 current**, all 47 senators and 141 delegates. A 409 for people: stop and report.

**Gates (report each):**
* **Task revision:** 81 or later.
* **Sessions:** the "using active sessions" line names **`2026` and `2026s1`**.
    * The session menu was re-checked on 2026-11-23 against mgaleg.maryland.gov: all 41 entries are known to the scraper or ignored.
    * Stored dates: 2026 runs 2026-01-14 to 2026-04-13; 2026s1 runs 2026-08-03 to 2026-08-04.
* **Votes:** vote events > 0 in 2026, from both chambers. That is OPEN-717's fix: the probe saw 0.
* **Completion:** `status=completed failed=0`, and the archive finished. Report fetched and `fetch_errors`.
* **Counts:** the run id, bill and vote-event counts per session, and the archived-document count.
* **Time:** the limit is 6 h per phase. If a phase passes it: stop and report.
* **A load stopped by "already imported as ...":** stop and report the full message. MD sets vote keys, so this isn't expected.

### 4. Hand back
* Send me the run id in a `reply-` note here.
* I'll do the read-only checks on the Mac replica:
    * Stage 3 gates;
    * vote names (OPEN-719);
    * motion labels (OPEN-700);
    * overrides and consent calendars (OPEN-718);
    * action order;
    * version names.

## Things to know
* **LegBot hand-off:** MD's `legbot_replica` is false. The Mac refuses it; EC2 skips it once SYNC-186 is live there.
* **Weekly run:** from the rollout on, MD is in the weekly Sunday 02:00 UTC scrape (no timing entry).

## If it goes wrong
* **A failed run or failed gate:** stop and report; don't retry.
* **The way back:** a PR returning MD to `probing` (I'll write it), then a rollout.

## Not authorized
* Older Maryland sessions.
* LegBot, routing, website, search or embedding for MD.
* A second run, or any other state.
* Editing ddp-sync's schedule or the manifest by hand.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
