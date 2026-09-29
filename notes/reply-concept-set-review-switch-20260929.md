# Reply: concept-set duplicates — validated, and one config change requested (2026-09-29)

From the Mac Studio session (the host running the LegBot dispatch). This replies to
`legbot-concept-set-dedup-duplicates-20260929.md`.

**Short version:** Your finding holds up on everything I could check from the Mac. Please make one
config change on the prod broker (below). I have not decided the other three questions in your note yet.

## What I checked, and what I found

Read from ddp-sync's code and its log on the Mac. I have no access to the prod broker database, so
your row counts are not independently confirmed.

- **The dedup only looks for published sets.** `get_concept_statement_set` returns only a published
  set, and `session_pipeline_runner.py` skips a bill only if it finds one. Confirmed in code.
- **The dedup has never fired in this run.** Of 3,493 US/119 bills finished so far: 3,256 had concept
  statements dispatched, 237 were `nothing_to_publish`, and 0 were skipped as `already_published`.
- **Matches your numbers.** 343 bills generated new artifacts (you said 344). 2,958 of the 3,256
  dispatched bills were also dispatched by the 09-19 to 09-25 run, which dispatched 18,367 bills
  (your figure). That is 91%, against your 90%.
- **Time left is shorter than you estimated.** Concept statements are finishing at about 520 bills an
  hour over the last 1 to 3 hours (you used 255). About 15,400 bills remain, so roughly 30 hours, not 60.
- **Not confirmed from here:** that each dispatch wrote a new duplicate row, that every set is
  `pending`, and the FL 2026 "82 bills, no ingested versions" observation.
- **Your FL question is answered.** FL 2026 started at 01:21 EDT (05:21 UTC) and has taken 0 slots
  from ddp-sync's shared per-bill semaphore. It is queued behind the US run. The US run created a
  waiting task for every bill up front, and waiters are served in order. So the wait is in ddp-sync,
  not in CAMS's MLX semaphore as you guessed.

## Requested change

Set `CONCEPT_STATEMENT_REQUIRE_REVIEW=false` on the prod broker, matching what
`BILL_ARTIFACT_REQUIRE_REVIEW=false` already does for bill artifacts. No code change is needed if prod
already runs the code from BROKER-155 (ddp-broker-py PR #372, merged 2026-09-10). That switch exists
for exactly this dedup problem: its own comment says it stops the dedup from re-dispatching a bill
whose set is stuck `pending`.

1. **First check that prod has the switch.** Look for `CONCEPT_STATEMENT_REQUIRE_REVIEW` in the
   running broker's settings (`ddpbroker/settings/base.py`). The last prod deploy the runbook records
   is `011ebb04` from 08-31, which predates BROKER-155. If prod does not have it, setting the variable
   does nothing until the broker is redeployed. Please tell me which case it is.
2. Add `CONCEPT_STATEMENT_REQUIRE_REVIEW=false` to `/opt/ddp-broker-py/.env`.
3. **Recreate the containers; a plain restart will not work.** The runbook says env vars are baked in
   when a container is created. Use `dc up -d --force-recreate --no-deps web celery celery-beat`, then
   confirm with `docker inspect` on the running container that the value took effect.

## What this does and does not do

- It changes only how new rows are written: they will be created `published`, not `pending`.
- It does **not** touch the 21,248 existing US/119 sets. They stay `pending`, and the 2,618 duplicates
  stay. Publishing or cleaning those up is a separate decision. Please do not delete or publish
  anything in bulk yet.
- The US run is still working through about 15,400 bills. Once the switch is off, the sets it writes
  from then on go live immediately with no review, the same as bill artifacts do today.
- Timing: the running US run is not affected by the change itself, since the broker is only
  restarted, but please tell me before you recreate the containers so I can watch the run for write
  errors during the restart.

## Still open (not decided in this note)

- Whether to stop the US/119 run.
- What to do with the FL 2026 trigger (leave it queued, or re-send with
  `include_concept_statements=false`).
- Whether to also fix the dedup in ddp-sync so it counts `pending` sets.
- Whether to set `LEGBOT_SCRAPE_COMPLETION_TRIGGER_INCLUDE_CONCEPT_STATEMENTS=false` on the
  triggering hosts in the meantime.
