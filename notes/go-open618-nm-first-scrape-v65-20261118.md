# GO: OPEN-618, New Mexico's first scrape on v65 (ddp-scrapers task revision 68): roll out the manifest that moves NM to `scraping`, then ONE manual run after Pennsylvania's 04:00 run on v65 has finished cleanly. 2026-11-18 (Mac side)

**Ramon, 2026-11-18 (relayed by the PM session):**
* "Go, after Pennsylvania": NM's first scrape as a manual one-off run on Fargate, then read-only checks on the Mac.
* On how: "Same as GA/NV/OR" (advance to `scraping` the way Georgia, Nevada and Oregon were on 11-16).

## What I need from you

### 1. Gates before anything
* ddp-open-states **PR #418 is merged**: NM at `scraping`, head 294b3222. Until then, stop here and wait.
* Pennsylvania's 04:00 UTC run on revision 68 has finished cleanly: `status=completed failed=0`, archive finished. This is NM's first run on a new image, and one state goes first.
* Nothing for `nm` is running (the trigger takes the state's lock anyway).

### 2. Roll out the manifest
`rollout-manifest.sh` as usual, on both hosts, so the trigger knows `nm`. Report:
* the commit each host now has;
* that the drift check is clean.

### 3. One manual run
* `POST /ddp-sync/v1/trigger/openstates-scrape/nm`: one call, no retry.
* It should start without `skip_people_check`: NM's legislators are already loaded (people load 11-17: 291 created, 109 current). If it answers 409 for people anyway, stop and report; don't add the flag.

**Gates (report each):**
* the ECS task runs **revision 68** (image v65). Any other revision: stop and report.
* the scraper's "no session provided, using active sessions: {…}" line: report the sessions it names.
* NM has a separate votes scraper (`votes_scraper: true`, OPEN-592), so the cloud run also runs it: report vote events > 0.
* `status=completed failed=0`, and the archive finished: report fetched and `fetch_errors` (fetch errors are a report, not a retry).
* Report the run id, bill count, vote-event count and archived-document count.

### 4. Hand back
* Send me the run id in a `reply-` note here.
* I do the read-only checks on the Mac replica: Stage 3 gates, action order (OPEN-620), motion texts.

## Side effect (Ramon knows)
* From the rollout on, NM is in the **weekly Sunday 02:00 UTC** scrape (it has no timing entry, so the default applies).
* Today is Wednesday 11-18. Do the manual run well before Sunday 11-22, so the first run is this supervised one.

## If it goes wrong
* A failed run or failed gate: stop and report. Don't retry.
* The way back is a PR returning NM to `probing` (I'll write it), then a rollout. **Don't hand-edit the manifest on the host.**

## Not authorized
* LegBot for NM (it stays off: `legbot_replica` false).
* Any routing or website switch-on, or search or embedding.
* A second run; any other state.
* Editing ddp-sync's schedule or the manifest by hand.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
