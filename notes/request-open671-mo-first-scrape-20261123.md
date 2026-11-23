# REQUEST (not a GO yet): OPEN-671, Missouri's first scrape. Roll out the manifest that moves MO to `scraping`, then ONE manual run of its active session (`2026`). It **needs an image built after scrapers `c3ec462d`**. 2026-11-23 (Mac side, Onboard New States)

**This waits for Ramon's go.** He gives it in your session or through the PM session; start only on it. Same shape as Maryland's (`2672ac8`).

## What I need from you (on Ramon's go)

### 1. Gates before anything
* **ddp-open-states PR #477 has merged.** It moves MO to `scraping` (branch `feat/OPEN-671-mo-scraping`, head `534044af`). It was made with `advance-jurisdiction`: `people_refresh`, `archive.enabled` and `cloud_path` true; LegBot, routing, search and embedding false. Until it merges, stop and wait.
* **New tasks run an image whose scrapers tree contains `c3ec462d`** (OPEN-791, scrapers #239: every Missouri session's close from the journals; 2026 now ends 2026-05-30, the constitutional midnight close the House journal records).
    * v81 (rev 83) is built from `d8e1bf5d` and **doesn't** contain it.
    * The next image (v82, from today's scrapers main, already requested for ID, NC, RI and OK) does, if built at `c3ec462d` or later.
    * Report the revision and image tag. A revision without `c3ec462d`: stop and report.
* **Nothing for `mo` is running.** Not on Sunday from 02:00 UTC.

### 2. Roll out the manifest
* Pull first (main includes `ce2d4cc7`'s validator change), then `rollout-manifest.sh --host ec2 --ddp-sync-root /opt/ddp-sync`.
* If another rollout already brought main past #477, report the commit and skip.
* Report the host's commit, and that the combined check agrees.

### 3. One manual run
* Call `POST /ddp-sync/v1/trigger/openstates-scrape/mo` once, with no retry, **without** `skip_people_check`.
* MO's people are loaded (`793d07c`). On the Mac replica: **188 current**, 33 of 34 senators and 155 of 163 representatives.
    * The 9 empty seats are real vacancies; the legislature's Sept 16 2026 journals count House 8 and Senate 1 (details on OPEN-791).
    * A 409 for people: stop and report.

**Gates (report each):**
* **Task revision:** contains `c3ec462d`.
* **Sessions:** the "using active sessions" line names **`2026`** only (stored 2026-01-07 to 2026-05-30). The scraper's 41-entry session list is all known.
* **Votes:** vote events > 0, from both chambers.
* **Completion:** `status=completed failed=0`, and the archive finished. Report fetched and `fetch_errors`.
* **Counts:** the run id, bill count, vote-event count and archived-document count.
* **Time:** the limit is 6 h per phase. If a phase passes it: stop and report.
* **A load stopped by "already imported as ...":** stop and report the full message. MO sets vote keys.

### 4. Hand back
* Send me the run id in a `reply-` note here.
* I'll do the read-only checks on the Mac replica:
    * Stage 3 gates;
    * vote names, including "MR SPEAKER" (OPEN-738);
    * motion labels;
    * action order;
    * version names.

## Things to know
* **Veto sessions:** Missouri's September veto-session roll calls are filed under the regular session (after its May 30 close). Ramon has the question of whether they should be their own sessions; it doesn't block this run.
* **LegBot hand-off:** MO's `legbot_replica` is false. The Mac refuses it, and EC2 skips it now that SYNC-186 is live there (`38e02fc`).
* **Weekly run:** from the rollout on, MO is in the weekly Sunday 02:00 UTC scrape (no timing entry).

## If it goes wrong
* **A failed run or failed gate:** stop and report; don't retry.
* **The way back:** a PR returning MO to `probing` (I'll write it), then a rollout.

## Not authorized
* Older Missouri sessions.
* LegBot, routing, website, search or embedding for MO.
* A second run, or any other state.
* Editing ddp-sync's schedule or the manifest by hand.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
