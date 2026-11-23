# REQUEST (not a GO yet): OPEN-670, Montana's first scrape. Roll out the manifest that moves MT to `scraping`, then ONE manual run of its active session (`2027`, pre-filing). 2026-11-23 (Mac side, Onboard New States)

**This waits for Ramon's go.** He gives it in your session or through the PM session; start only on it. This note has the same shape as Idaho's (`46ab1e0`) and Wyoming's (`994956e`).

**One open question for Ramon, asked through the PM:** should Montana's first load also take the **2025** session (the last one with votes), as a separate full import (part 4)? Without it, Montana has no votes until the 2027 session opens on 2027-01-04. Part 4 runs only if he says so.

## What I need from you (on Ramon's go)

### 1. Gates before anything
* **ddp-open-states PR #471 has merged.** It moves MT to `scraping` (branch `feat/OPEN-670-mt-scraping`, head `941e3948`). It was made with `advance-jurisdiction`: `people_refresh`, `archive.enabled` and `cloud_path` true; LegBot, routing, search and embedding false. Until it merges, stop and wait.
* **New tasks run ddp-scrapers:81 (v79) or later.** Montana's fixes are on scrapers main well before v79:
    * OPEN-694 (#160, motion rules);
    * OPEN-734 (#181, session types);
    * OPEN-749 (#200);
    * OPEN-750 (#202, `legislatureOrdinal` for 2011–2021);
    * OPEN-760 (#216, a vote key per roll call).
    * Report the revision and its image tag.
* **Nothing for `mt` is running.**
* **Not on Sunday from 02:00 UTC.**

### 2. Roll out the manifest
* Pull first (main includes the validator change from `ce2d4cc7`), then run `rollout-manifest.sh --host ec2 --ddp-sync-root /opt/ddp-sync`.
* If the RI/OK rollout (ddp-open-states notes `98e5dcb` / `8108de3`, step 2) has already run, this only adds #471.
* Report the host's commit, and that the combined check agrees.

### 3. One manual run
* Call `POST /ddp-sync/v1/trigger/openstates-scrape/mt` once, with no retry, **without** `skip_people_check`.
* MT's people are loaded (`793d07c`: 451 created, 0 updated). On the Mac replica: **150 current**, all 50 senators and 100 representatives. A 409 for people: stop and report.

**Gates (report each):**
* **Task revision:** 81 or later.
* **Sessions:** the "using active sessions" line should name **`2027`** only.
    * The session menu was re-checked on 2026-11-23 against api.legmt.gov: all 18 entries are known to the scraper or ignored.
    * 2027 opens 2027-01-04. **Expect pre-filed bills and drafts only, and 0 vote events.** That is correct, not a failure.
* **Completion:** `status=completed failed=0`, and the archive finished. Report fetched and `fetch_errors`.
* **Counts:** the run id, bill count, vote-event count (expect 0) and archived-document count.
* **Time:** the probe took 16.1 s for 10 bills; the limit is 6 h.
* **A load stopped by "already imported as ...":** stop and report the full message.

### 4. Only if Ramon says yes to 2025: one full import of `2025`
```
POST /ddp-sync/v1/trigger/openstates-scrape/mt?full=true&sessions=2025
```
* After part 3 completes, and not at the same time.
* The answer must be 202 with `"full": true`.
* Report run id, `"mode": "full"`, bill and vote counts, and the archive result.
* Before you run it, I'll post an UPDATE with 2025's checked dates and expected counts.

### 5. Hand back
* Send me the run ids in a `reply-` note here.
* I'll do the read-only checks on the Mac replica:
    * Stage 3 gates;
    * vote names and keys, if 2025 ran;
    * motion labels (OPEN-694);
    * action order.

## Things to know
* **LegBot hand-off:** MT's `legbot_replica` is false. The Mac refuses a hand-off (`legbot_replica_off`), and EC2 skips it once SYNC-186 is live there.
* **Weekly run:** from the rollout on, MT is in the weekly Sunday 02:00 UTC scrape (no timing entry). The next one is 11-29.

## If it goes wrong
* **A failed run or failed gate:** stop and report; don't retry.
* **The way back:** a PR returning MT to `probing` (I'll write it), then a rollout.

## Not authorized
* Part 4 without Ramon's yes.
* LegBot, routing, website, search or embedding for MT.
* A second run, any other state, or sessions before 2025.
* Editing ddp-sync's schedule or the manifest by hand.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
