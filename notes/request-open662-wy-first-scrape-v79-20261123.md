# REQUEST (not a GO yet): OPEN-662, Wyoming's first scrape on v79 (ddp-scrapers task revision 81). Roll out the manifest that moves WY to `scraping`, then ONE manual run. 2026-11-23 (Mac side, Onboard New States)

**This waits for Ramon's go.** Ramon decided "Yes, prepare it" (2026-11-23, PM session /ask-me, OPEN-662); the go for the run is separate, in your session or through the PM session. This note has the same shape as Idaho's (`46ab1e0`).

## What I need from you (on Ramon's go)

### 1. Gates before anything
* **ddp-open-states PR #467 has merged.** It moves WY to `scraping` (branch `feat/OPEN-662-wy-scraping`, head `601691ba`). It was made with `advance-jurisdiction`: `people_refresh`, `archive.enabled` and `cloud_path` true; LegBot, routing, search and embedding false. Until it merges, stop and wait.
* **New tasks run ddp-scrapers:81 (image v79) or later.**
    * v79: scrapers `0292506c`, core `42b48982`.
    * It holds Wyoming's fixes:
        * OPEN-752 (#208, session types that mirror the legislature);
        * OPEN-766 (#220, 2011–2015 and 2017 end on the last working day);
        * OPEN-769 (#223, each vote keyed on the source's own roll-call identity, the North Dakota failure's fix).
    * Report the revision and its image tag. Any revision before 81: stop and report.
* **Nothing for `wy` is running.**
* **Not on Sunday from 02:00 UTC.**

### 2. Roll out the manifest
* `rollout-manifest.sh --host ec2 --ddp-sync-root /opt/ddp-sync`. If the host's validator is older than main's, pull first (OPEN-532).
* If the PA go-live (ddp-open-states notes `945ce66`) or Idaho rollout has already run, this one only adds #467.
* Report the host's commit, and that the combined check agrees.

### 3. One manual run
* Call `POST /ddp-sync/v1/trigger/openstates-scrape/wy` once, with no retry, **without** `skip_people_check`.
* WY's people are loaded (your `793d07c`: 264 created, 0 updated, 93 read back). On the Mac replica (read-only, 11-23): 255 people, **93 current**, all 31 senators and 62 representatives. A 409 for people: stop and report.

**Gates (report each):**
* **Task revision:** the revision the task runs.
* **Sessions:** the "using active sessions" line should name **`2026`** (the 2026 Budget Session, stored 2026-02-09 to 2026-03-11).
    * Checked on 2026-11-23 against the legislature's own bill service (lsoservice.wyoleg.gov):
        * 2026 has 335 bills and no special session;
        * 2025 has 556 bills and no special session;
        * the scraper knows 2011–2026, with 2020S1 and 2021S1 as their own sessions.
    * Wyoming's session list is the scraper's own fixed list, not a menu read from the site.
* **Votes:** vote events > 0, from both chambers.
* **Completion:** `status=completed failed=0`, and the archive finished. Report fetched and `fetch_errors`.
* **Counts:** the run id, bill count (expect about 335), vote-event count and archived-document count.
* **Time:** the limit is 6 h; expect well under it for 335 bills.
* **A load stopped by a repeated vote or bill key** ("already imported as ..."): stop and report the full message.

### 4. Hand back
* Send me the run id in a `reply-` note here.
* I'll do the read-only checks on the Mac replica:
    * Stage 3 gates;
    * vote names;
    * motion labels;
    * session dates;
    * action order.

## Things to know
* **LegBot hand-off:** WY's `legbot_replica` is false. Until SYNC-186 (`9825134`) is live on EC2, the archive may still hand WY to the Mac's LegBot, as for ND and KY. Report it if it happens; don't act on it.
* **Load core:** if EC2 ddp-sync isn't rebuilt at core `42b48982` yet (notes `22d7100`), the load uses the older core. Wyoming doesn't need any core change since then.

## Side effect (Ramon knows)
* From the rollout on, WY is in the **weekly Sunday 02:00 UTC** scrape (no timing entry).
* The next one is 11-29, so please do the manual run before then.

## If it goes wrong
* **A failed run or failed gate:** stop and report; don't retry.
* **The way back:** a PR returning WY to `probing` (I'll write it), then a rollout. Don't hand-edit the manifest.

## Not authorized
* LegBot, routing, website, search or embedding for WY.
* A second run, any other state, or older Wyoming sessions.
* Editing ddp-sync's schedule or the manifest by hand.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
