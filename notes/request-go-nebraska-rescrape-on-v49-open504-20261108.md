# Request (production change, GO given): image v49 / revision 51 is registered; rescrape Nebraska once on it, so its adopted and prevailed votes are stored as passed (OPEN-504, OPEN-479). 2026-11-08 about 17:45 UTC (Mac side)

**Ramon's go** (2026-11-08, his words): "yes, let's release the new version and rescrape nebraska". This covers **one manual Nebraska scrape** on the new image. Thank you for Sunday's reports (`58752e8`, `b13565c`, `f0ff8c9`): all three states ran clean on v48.

**What changed.** Ramon ran the deploy script on the Mac at about 17:35 UTC. It printed **"tag v49, revision 51"**, with digest `sha256:0ce8672a96b957235c4d46110ffad35077dc8bdf04e2a93a9a0853c85e5e1116` (previous: v48 / revision 50). v49 is v48 plus one scrapers change: **openstates-scrapers #82** (merge `a178f6ab`, OPEN-504). Nebraska's scraper used to count a vote as passed only when its text said "Passed" or "Advanced". Now amendments and motions that were "adopted" or "prevailed", and "Returned to Select File for specific amendment", also count as passed. A failure word ("failed", "lost", "not adopted", "withdrawn") still always means failed. I confirmed inside the v49 image that `/opt/openstates-scrapers/scrapers/ne/bills.py` uses the new rule. Nothing else changed: core and ddp-open-states `main` are the same as for v48.

## What I need from you
1. **Gate, before you trigger:** no Nebraska task or lock (`prod/ne/_lock`) in flight. Do it outside 02:00 to 03:10 UTC (the Sunday batch is over; Pennsylvania's nightly is 04:00 UTC and may overlap, which is fine: Sunday showed RDS at 25% CPU at most).
2. **Trigger Nebraska once:** `POST /ddp-sync/v1/trigger/openstates-scrape/ne`, the same way as the first scrape on 11-02. Expect `{"status":"started","target":"ne"}`. **Launch it once. Never relaunch or retry**, whatever happens.
3. **Confirm the revision** from the launched task (describe-tasks): it must be **`ddp-scrapers:51`, image v49**. If it is anything else, tell me (the scrape is still fine to finish; it just won't fix the votes).

## End report: Sunday's checks (a) to (g), plus the vote results
- (a) to (g) as in your `58752e8` Nebraska report: no `unexpected keyword argument`; the `NE OPEN-500: ignoring start=...` line; bills and vote events against 1,847 / 1,772 / 86,723 person votes; import ran, exit code 0; no `no objects returned from` and no "no-op" summary; duration (about 1 h) and any 429/403/block; RDS CPU and connections.
- **(h) Vote results, the point of this run.** Read-only, Nebraska vote events in RDS:
  - **Before you trigger:** `result` counts. I expect **818 pass, 954 fail**, matching the Mac replica.
  - **After the run:** the same counts, then `motion_text` grouped by `result` for the ones still `fail`.
  - **My prediction, from running the new rule over the replica's 1,772 events:**
    - **1,521 pass and 251 fail**: 703 go from fail to pass (624 "... adopted", 64 "... prevailed", 14 "Returned to Select File for specific amendment", 1 "Readvanced ...").
    - **None go from pass to fail.**
    - The 251 left as fail should all read "... failed", "... lost" or "Failed ...".
  - **If any event goes from pass to fail, or the after-counts differ from the prediction by more than a handful, stop and tell me.** Do not do anything about it.

## Stop conditions
- The trigger answers anything but `started`.
- The task cannot be placed.
- Any 429/403/WAF text.
- A non-zero exit.
- The load fails.
- The run is still going at 3 hours.
In every case: report, do not relaunch.

## Not authorized
- A second Nebraska launch.
- Any other state's scrape, any archive, any LegBot run.
- `backfill-motion-classification.py` for Nebraska (never: see PLAN-push-button-onboarding, 2026-11-06).
- `recompute-diff-order`.
- Registering or deregistering a task definition.
- Any pull or restart.
- Any change to `replica_routing`, `legbot_replica` or the broker's state list. The switch-on is a separate request, still to come.

## Reply
`reply-nebraska-rescrape-on-v49-open504-<date>.md`: one at launch (time, run id, task id, revision, the before-counts), one at the end.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
