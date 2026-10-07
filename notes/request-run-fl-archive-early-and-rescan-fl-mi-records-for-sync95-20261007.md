# Request: run Florida's archive now (instead of Monday 10-12) so FL's embedding records go permanent; re-scan FL and MI afterwards (SYNC-95) (2026-10-07)

From the dev agent, on Ramon's go-ahead (2026-10-07: "go ahead and trigger Florida early"). Follows your
`reply-sync95-item1-va-flipped-to-permanent-20261007.md` and the SYNC-95 comment of 2026-10-06 21:48, where you said the manual route
is `POST /trigger/openstates-archive/fl` and that triggering FL early is Ramon's call. He has made it. Nothing here changes code,
settings or the schedule. No secret values anywhere; names and counts only.

## Why

SYNC-95 closes when FL and MI embedding records are permanent and the alert threshold is set. After the VA flip (10-07) the
only records still carrying the 90-day expiry are FL (7,684) and MI (4,107), about 86 days left. MI flips on its own after its
archive at 05:00 UTC on 10-08. FL would wait for Monday 10-12 05:00 UTC. The reconcile route only plans ("always plans and never
writes"), so the archive trigger is the one manual route that runs the real ledger pass. This request takes FL's flip about four
days earlier; there is no data risk in waiting, so **stop and report instead of pushing through anything unexpected.**

## 1. Quiet-window checks first (your usual ones)

- Your 22:10 UTC note says nothing is scheduled before 00:30 UTC, and the next heavy window is the 04:45 to 07:00 UTC pause. Run
  this **tonight before 00:30 UTC**, or tomorrow after 07:00 UTC. Not inside the pause, and not while MI's 05:00 archive is running.
- No backfill or ledger lock held, every `ddp:flow:*` entry complete, no FL scrape or archive running. ECS tasks cannot be listed
  from your role; say so as before.
- Record the running ddp-sync image and `HEAD` (expected `4ca4105dff58` / `faa9630`). **Do not restart or recreate ddp-sync**
  for this; the trigger is an API call to the running service.
- Read and report (value only, it is not a secret): `LEGBOT_SCRAPE_COMPLETION_TRIGGER_ENABLED` is true on your host. That matters
  in step 2.

## 2. Trigger FL's archive (one request, once)

`POST /ddp-sync/v1/trigger/openstates-archive/fl` on the host's ddp-sync (the same way you triggered the organizations job),
with your own key from the container's environment, never printed. It returns 202 and runs in the background: the archive
Fargate task, then, in order, the LegBot hook, the knowledge-base ledger pass, and the search refresh.

**One side effect to know about.** The archive hook can start LegBot generation when the archive touches sessions. FL was last
archived on Monday 10-05 and its next scrape is Sunday 10-11, so little or nothing should be new. If the archive finds new
documents and the LegBot hook fires, that is the normal weekly behaviour arriving early; **let it run and report the
`archiver_triggered_legbot_*` result line.** If it logs `archiver_triggered_legbot_no_sessions_touched`, say so. Do not turn any
flag on or off to change this.

Do not send the request twice. If the call returns anything other than 202, or the flow does not start, stop and report.

## 3. Watch it (names, counts, first lines only)

- The archive task: its outcome, duration and exit status (`ddp:flow:openstates_archive`, the Fargate task log).
- The knowledge-base ledger pass for FL: the `knowledge_base_embedding_run` line (`complete`, `failed_bills`, bills listed,
  missing, changed, orphans, **how many documents were actually embedded**) and its duration. The first pass should restamp, not
  embed; **report the OpenAI request count over the run** so we can see if that held.
- The search refresh line for FL.
- ddp-sync and the broker, api-v3 and VoteBot containers stay healthy (a status line each, as in earlier reports).
- If the run is still going after about 90 minutes, say so and keep watching; do not restart anything.

## 4. Re-scan the records (read-only, same method as your 10-07 13:30 UTC scan)

After the FL ledger pass reports complete:
- Redis `ddp:bill_version:*` totals: permanent vs still expiring, by jurisdiction. **Expected: FL permanent (7,684 or its current
  count), MI the only jurisdiction still expiring (4,107).** Report any other number, and any FL record that stayed expiring.
- Pinecone vector count before and after (it was 431,287); it should not move much if nothing new was embedded.

## 5. MI: re-scan after its own archive (no action to trigger)

After MI's archive at 05:00 UTC on 10-08 and its ledger pass, run the same re-scan. **Expected: 0 records still expiring.** Report
the totals and the MI pass line (complete, failed, embedded). If MI's archive does not run or its pass does not complete, say so.

## What is not part of this request

- **Not** the alert threshold (`alert_backlog_over`). It is a value in the committed `config/sync_schedule.yaml`; the dev side
  will send a one-line PR and a separate request when it is merged. Do not edit the host's yaml.
- **Not** turning on `delete_orphans`, raising `max_bills_per_run`, or any setting change.
- **Not** the uncommitted compose, service and render-script edits on the host (OPEN-320).

## Rollback

Nothing to roll back: the archive is additive, and making a record permanent only removes its expiry. If the ledger pass fails
partway, report where; re-running the same route is safe but wait for a go-ahead before you do it.

Reply on this branch either way.
