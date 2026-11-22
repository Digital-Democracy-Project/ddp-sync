# GO: OPEN-663, North Dakota's first scrape on v76 (ddp-scrapers task revision 78). Roll out the manifest that moves ND to `scraping`, then run ONE manual scrape. 2026-11-22 (Mac side)

**Ramon, 2026-11-22 (pop-up in the onboarding session), choosing "Go: first scrape":**
> If you say go: PR #456 (which switches North Dakota on) is merged and rolled out, then the production agent runs one supervised scrape on Fargate of the 2025-26 assembly and its two 2025 special sessions, and I check the results on the Mac afterwards. From then on North Dakota is also in the weekly Sunday scrape. LegBot, the website and search stay off.

This is the same shape as New Mexico's go, `go-open618-nm-first-scrape-v65-20261118.md`.

## What I need from you

### 1. Gates before anything
* **PR #456 has merged.** That is ddp-open-states #456: ND at `scraping`, branch `feat/OPEN-663-nd-scraping`. It was made with `advance-jurisdiction`, which sets `people_refresh`, `archive.enabled` and `cloud_path` to true; LegBot, routing, search and embedding stay false. Until #456 has merged, stop here and wait.
* **Image v76 is what runs.** New tasks use **ddp-scrapers:78** (image v76) and **ddp-scrapers-db:5**.
    * v76 was built by Ramon on 2026-11-22 at about 16:40 UTC, from core 7f531958 and scrapers main 8cf0b939; the PM session verified the build.
    * It contains North Dakota's fixes: OPEN-679 motion rules, OPEN-721 session types, OPEN-722 names and OPEN-746 special sessions (scrapers #201).
    * Please confirm the revision read-only. If it isn't 78, stop and report.
* **Nothing for `nd` is running.** The trigger takes the state's lock anyway.

### 2. Roll out the manifest
Run `rollout-manifest.sh` as usual, on both hosts, so the trigger knows `nd`. If a host's validator is older than main's, pull first, then run the script (OPEN-532).

Report:
* the commit each host now has;
* that the drift check is clean.

### 3. One manual run
* Call `POST /ddp-sync/v1/trigger/openstates-scrape/nd` once, with no retry.
* Start it **without** `skip_people_check`. ND's legislators are already loaded: prod checkpoint 793d07c on 11-21, 337 created, 0 updated, and 141 current on the Mac replica. If it answers 409 for people anyway, stop and report; don't add the flag.

**Gates (report each):**
* **Task revision:** the ECS task runs **revision 78**. Any other revision: stop and report.
* **Sessions:** report the sessions named on the scraper's "no session provided, using active sessions: {…}" line. Expect `69`, `69S1` and `69S2`.
    * Each is filed as its own session (OPEN-746): the 69th Legislative Assembly, plus the two 2025 special sessions.
    * The session menu was re-checked on ndlegis.gov/assembly at 17:22 UTC today. All 70 listed assemblies are known to the scraper or ignored, so `check_session_list` should pass.
    * If it stops on an unknown session, stop and report; I'll fix it in the scraper.
* **Votes:** vote events > 0. ND's roll calls come from the bills scraper (`votes_scraper: false`; there is no separate votes scraper). The probe sample had 29 roll calls.
* **Completion:** `status=completed failed=0`, and the archive finished. Report fetched and `fetch_errors`; fetch errors are a report, not a retry.
* **Counts:** report the run id, bill count (per session if the log gives it), vote-event count and archived-document count.
* **Time:** the probe's dry run took 50.8 s for 10 bills, so the run may take a few hours. The limit is the default 6 h. If it hits the limit, stop and report.

### 4. Hand back
* Send me the run id in a `reply-` note here.
* I'll do the read-only checks on the Mac replica:
    * Stage 3 gates;
    * vote names (OPEN-722 and OPEN-723);
    * motion labels (OPEN-679);
    * action order;
    * that each special session's bills are filed under their own session.

## Side effect (Ramon knows)
* From the rollout on, ND is in the **weekly Sunday 02:00 UTC** scrape: it has no timing entry, so the default applies.
* Today is Sunday 11-22, so the next scheduled run is 11-29. Please do the manual run well before then, so the first run is this supervised one.

## If it goes wrong
* **A failed run or failed gate:** stop and report. Don't retry.
* **The way back:** a PR returning ND to `probing` (I'll write it), then a rollout. **Don't hand-edit the manifest on the host.**

## Not authorized
* LegBot for ND (`legbot_replica` stays false).
* Any routing or website switch-on, search or embedding.
* A second run, or any other state. Wyoming and the other 11 new states each get their own go.
* Editing ddp-sync's schedule or the manifest by hand.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
