# Reply: ddp-sync on the EC2-broker host is on a01094d (SYNC-132/137/138). Route live, one test alert accepted (CAMS 202). Gates all passed

Prod agent, Thu 2026-10-29 01:08 UTC. The operator said "Option A, run the ddp-sync deploy and broker Stage 0 now. check for a quiet window." Output in `/home/bitnami/ops/sync132-20261029T0100Z/`.

## Gates
* **G0 no hold:** re-read all four branches (nothing new), nothing in flight, no OPEN-437 step running.
* **G1:** `/opt/ddp-sync` on `main`, HEAD `b6d5efa` (the commit you expected), clean; `a01094d` a fast-forward (merges #252, #253, #254, #255).
* **G2 quiet window:** Thu 01:00 UTC: in-flight 0, no ddp-sync log activity, no broker writers, no api-v3 or VoteBot traffic in 5 minutes; the next launches are WA 02:30 and USA 03:00 (about 1 h 30 m and 2 h clear of the restart; the restart finished at 01:04). The only thing running was an ordinary broker tally task.
* **G3 rollback point:** commit `b6d5efa42cca64bed04aba19c5722043f6b07931`; replaced container `f0d99a05a4eb` (started 10-28 00:43:17), image `23703f202324`, tagged **`ddp-sync:pre-sync132`**.
* **G4 secret key NAMES:** `operator_alert_token` **is listed**; **`cams_api_token` is NOT listed** (so no Mac signal). The full name list is in the output folder.
* **G5:** `curl -m 5 http://10.0.0.8:8000/api/v1/health` at 01:00:39 UTC: **HTTP 200**.
* **G6:** no env, yaml, compose or secret edited by hand.
* **G7 jobs before:** 19 (list saved).

## Part 1: deploy
Old log saved. `git merge --ff-only a01094d` (HEAD `a01094d`); **`sudo systemctl restart ddp-sync` issued 01:00:51; `systemctl` returned after 160 s; the container started 01:03:31 and was healthy at 01:04:37 (226 s)**, restarts 0, `systemctl is-active` = active; new image `12bb8d030751`. `/ddp-sync/v1/health`: healthy, scheduler running, redis connected.
* **Job list after: 19, identical to before.** **`votebot_eval` is NOT in this host's scheduler** (0 in either list).
* No error or traceback in the startup log; one `jurisdiction_manifest_agrees` line (no `scheduled` warning).
* `OPERATOR_ALERT_URL` = `http://10.0.0.8:8000/api/v1/operator-alerts`; the token is **set** (it was unset before the restart). `grep -c operator_alert_token infrastructure/render-env.sh` prints **2**, not 1 as the note says (two lines mention it; I did not read them). The drift flag is still True, `cams_api_token` still unset. No scrape or Fargate launch since the restart; in-flight 0.

## Part 2: the one test alert
Sent once at **01:05:28 UTC** with exactly your command (`post_alert('TEST (SYNC-132): EC2 ddp-sync route check, please ignore', source='sync132.test')`). It printed **`True`**, and the call's own output carried `operator_alert_sent_via_cams http_status=202 source=sync132.test`. **My count of the container's log (`docker logs`) is 0 for `operator_alert_sent_via_cams`, `operator_alert_rejected` and `operator_alert_cams_unreachable`**, because the `docker exec` process logs to its own output, not the service's; so those three counts "since the restart" are 0 in the service log and the one acceptance line is only in the exec output. No second alert was sent. I cannot see Slack: the Mac session will confirm the CodeBot message in `#automation-errors`.

Nothing else changed. The broker deploy request (`1be6822d`) is separate; only its read-only Stage 0 was run (Checkpoint A is on the broker branch).

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
