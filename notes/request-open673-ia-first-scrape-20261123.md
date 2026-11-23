# REQUEST (not a GO yet): OPEN-673, Iowa's first scrape. Roll out the manifest that moves IA to `scraping`, then ONE manual run of its active sessions (`2025`, `2026`). 2026-11-23 (Mac side, Onboard New States)

**This waits for Ramon's go.** He gives it in your session or through the PM session; start only on it. Same shape as Montana's (`a362370`).

**One open question for Ramon, asked through the PM:** should the first run also take Iowa's **2026 Extraordinary Session** (`2026s1`, sat 2026-10-02, the legislature's most recent business)?
* It is a session of its own, but not active, so a normal run skips its bills and roll calls (OPEN-745 files them under `2026s1`, not `2026`).
* If he says yes, part 3 runs with `?sessions=2025,2026,2026s1` (SYNC-181's parameter, no `full=`). If not, plain.

## What I need from you (on Ramon's go)

### 1. Gates before anything
* **ddp-open-states PR #472 has merged.** It moves IA to `scraping` (branch `feat/OPEN-673-ia-scraping`, head `2dabcdcc`). It was made with `advance-jurisdiction`: `people_refresh`, `archive.enabled` and `cloud_path` true; LegBot, routing, search and embedding false. Until it merges, stop and wait.
* **New tasks run ddp-scrapers:81 (v79) or later.** Iowa's fixes are all on scrapers main before v79:
    * OPEN-741 (#191);
    * OPEN-743 (#193);
    * OPEN-744 (#196);
    * OPEN-745 (#198, extraordinary sessions and bills by filing year);
    * OPEN-760 (#216, a vote key per roll call).
    * Report the revision and its image tag.
* **Nothing for `ia` is running.** Not on Sunday from 02:00 UTC.

### 2. Roll out the manifest
* Pull first (main includes `ce2d4cc7`'s validator change), then `rollout-manifest.sh --host ec2 --ddp-sync-root /opt/ddp-sync`.
* If another rollout already brought main past #472, report the commit and skip.
* Report the host's commit, and that the combined check agrees.

### 3. One manual run
```
POST /ddp-sync/v1/trigger/openstates-scrape/ia                          # if Ramon says no to 2026s1
POST /ddp-sync/v1/trigger/openstates-scrape/ia?sessions=2025,2026,2026s1 # if he says yes
```
* Once, with no retry, **without** `skip_people_check`.
* IA's people are loaded (`793d07c`). On the Mac replica: **148 current** of the 150 seats. Your load read back 146 of 148 files, so two seats are probably vacant; not a blocker. A 409 for people: stop and report.

**Gates (report each):**
* **Task revision:** 81 or later.
* **Sessions:** the "using active sessions" line names **`2025` and `2026`** (plus `2026s1` if requested).
    * The session menu was re-checked on 2026-11-23 against legis.iowa.gov: all 16 General Assemblies are known to the scraper.
    * Each year keeps the bills first acted on in that year (OPEN-745); either year's run reads both years' journals.
* **Votes:** vote events > 0 in both years.
* **No bill sits under two sessions.** I check this on the replica.
* **Completion:** `status=completed failed=0`, and the archive finished. Report fetched and `fetch_errors`.
* **Counts:** the run id, bill and vote-event counts per session, and the archived-document count.
* **Time:** the limit is 6 h per phase. Iowa's scraper notes `--http-resilience`, so expect a slow site. If a phase passes 6 h: stop and report.
* **A load stopped by "already imported as ...":** stop and report the full message.

### 4. Hand back
* Send me the run id in a `reply-` note here.
* I'll do the read-only checks on the Mac replica:
    * Stage 3 gates;
    * vote names and keys;
    * that no bill is under two sessions;
    * motion labels;
    * action order;
    * version names.

## Things to know
* **LegBot hand-off:** IA's `legbot_replica` is false. The Mac refuses it; EC2 skips it once SYNC-186 is live there.
* **Weekly run:** from the rollout on, IA is in the weekly Sunday 02:00 UTC scrape (no timing entry).

## If it goes wrong
* **A failed run or failed gate:** stop and report; don't retry.
* **The way back:** a PR returning IA to `probing` (I'll write it), then a rollout.

## Not authorized
* `2026s1` without Ramon's yes.
* Any session before 2025.
* LegBot, routing, website, search or embedding for IA.
* A second run, or any other state.
* Editing ddp-sync's schedule or the manifest by hand.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
