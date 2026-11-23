# REQUEST (not a GO yet): OPEN-669, Mississippi's first scrape. Roll out the manifest that moves MS to `scraping`, then ONE manual run of its active sessions (`2026`, `20261E`). 2026-11-23 (Mac side, Onboard New States)

**This waits for Ramon's go.** He gives it in your session or through the PM session; start only on it. Same shape as Maryland's (`2672ac8`).

## What I need from you (on Ramon's go)

### 1. Gates before anything
* **ddp-open-states PR #476 has merged.** It moves MS to `scraping` (branch `feat/OPEN-669-ms-scraping`, head `a058fa2b`). It was made with `advance-jurisdiction`: `people_refresh`, `archive.enabled` and `cloud_path` true; LegBot, routing, search and embedding false. Until it merges, stop and wait.
* **New tasks run ddp-scrapers:81 (v79) or later.** Mississippi's fixes are all on scrapers main before v79:
    * OPEN-693 (#154, motion rules);
    * OPEN-708 (#161): a roll call on an unmapped action warns instead of stopping the whole scrape;
    * OPEN-730 (#179, session types);
    * OPEN-731 (#180): "Mr. Speaker" is the session's Speaker, from the manifest (ddp-open-states #446, already on main);
    * OPEN-732 (#182, 2026 dates);
    * OPEN-733 (#183) and OPEN-735 (#184): roll-call totals with one dash or no closing period;
    * OPEN-759 (#218, 20093E end date).
    * Report the revision and its image tag.
* **Nothing for `ms` is running.** Not on Sunday from 02:00 UTC.

### 2. Roll out the manifest
* Pull first (main includes `ce2d4cc7`'s validator change), then `rollout-manifest.sh --host ec2 --ddp-sync-root /opt/ddp-sync`.
* If another rollout already brought main past #476, report the commit and skip.
* Report the host's commit, and that the combined check agrees.

### 3. One manual run
* Call `POST /ddp-sync/v1/trigger/openstates-scrape/ms` once, with no retry, **without** `skip_people_check`.
* MS's people are loaded (`793d07c`: 388 created, 0 updated, with people fix OPEN-692). On the Mac replica: **171 current**, 51 of 52 senators and 120 of 122 representatives.
    * The 3 empty seats are recorded vacancies in the people repo: Senate 34, House 70 and House 77.
    * All three members left after both 2026 sessions, so every 2026 voter has a record.
    * A 409 for people: stop and report.

**Gates (report each):**
* **Task revision:** 81 or later.
* **Sessions:** the "using active sessions" line names **`2026` and `20261E`**.
    * The session menu was re-checked on 2026-11-23 against billstatus.ls.state.ms.us/sessions.htm: all 70 entries are known to the scraper.
    * Stored dates:
        * 2026 runs 2026-01-06 to 2026-04-15. HCR 64 set sine die for April 15, 2026.
        * 20261E runs 2026-07-14 to 2026-07-17. The last floor actions and the Governor's approvals were on 07-17.
* **Votes:** vote events > 0 in both sessions, from both chambers.
* **Completion:** `status=completed failed=0`, and the archive finished. Report fetched and `fetch_errors`.
* **Counts:** the run id, bill and vote-event counts per session, and the archived-document count.
* **Warnings:** report any "unknown roll-call action" warnings (OPEN-708). They mean a roll call was kept with a default label.
* **Time:** the scraper takes no `start=`, so this is a full walk. The probe took 13.1 s for 8 bills. The limit is 6 h per phase. If a phase passes it: stop and report.
* **A load stopped by "already imported as ...":** stop and report the full message. MS sets vote keys, so this isn't expected.

### 4. Hand back
* Send me the run id in a `reply-` note here.
* I'll do the read-only checks on the Mac replica:
    * Stage 3 gates;
    * vote names ("Mr. Speaker", the Baileys, the totals fixes);
    * motion labels (OPEN-693);
    * action order;
    * version names.

## Things to know
* **LegBot hand-off:** MS's `legbot_replica` is false. The Mac refuses it; EC2 skips it once SYNC-186 is live there.
* **Weekly run:** from the rollout on, MS is in the weekly Sunday 02:00 UTC scrape (no timing entry).

## If it goes wrong
* **A failed run or failed gate:** stop and report; don't retry.
* **The way back:** a PR returning MS to `probing` (I'll write it), then a rollout.

## Not authorized
* Older Mississippi sessions, 2025 and its extraordinary session included.
* LegBot, routing, website, search or embedding for MS.
* A second run, or any other state.
* Editing ddp-sync's schedule or the manifest by hand.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
