# GO (SYNC-132, SYNC-137, SYNC-138): bring the EC2-broker host's ddp-sync to `main` `a01094d`; this deploy also switches the host's alert route on (Mac side, 2026-10-28)

**STATUS: GO**, from Ramon (Mac session, 2026-10-28): "we're preparing to deploy a bunch of broker fixes, and I want the ddp-sync fixes deployed as well." Do each part only if every gate passes. If a gate fails, stop and report; do not work around it.

**Scope: this host only.** **Do NOT touch the votebot/ddp-api instance.** VoteBot 2.0 is deprecated and its code is frozen (Ramon, 2026-10-28); nothing here touches it or its host. The broker's own deploy is a separate request. Do not combine the two restarts in the same minute: ddp-sync writes to the broker, so a broker restart in the middle of a ddp-sync job fails writes. Each deploy stands alone and either may go first.

## 1. What the pull changes (`b6d5efa..a01094d`, checked on the Mac)
`b6d5efa` is the last commit recorded for this host (the 2026-10-27 docs note). Read your own `HEAD` in G1; if it is a different commit, say so before pulling. Four merges, no dependency, Dockerfile or schedule changes, and no job added or removed:
- **PR #252:** docs only.
- **PR #253 SYNC-132:** `post_alert` tries CAMS's operator-alert route first and falls back to the direct Slack post on any non-2xx, a timeout (5 s) or no answer. **`infrastructure/docker-compose.prod.yml` now sets `OPERATOR_ALERT_URL=http://10.0.0.8:8000/api/v1/operator-alerts`, and `render-env.sh` maps the secret key `operator_alert_token` to `OPERATOR_ALERT_TOKEN`.** So after this deploy the route is **live on this host if and only if the token is present**; with no token the code posts directly, exactly as today. A CAMS-sent alert logs `operator_alert_sent_via_cams`; a refusal logs `operator_alert_rejected`; no answer logs `operator_alert_cams_unreachable`.
- **PR #254 SYNC-137:** every async job now sends its alert on a side thread (23 call sites, six files), so a slow or unreachable CAMS can no longer freeze the scheduler and API for up to about 20 s. No wording, channel or retry change. This is the fix that had to be on EC2 before the route was switched on here.
- **PR #255 SYNC-138:** the weekly VoteBot eval also posts its bad-news cases (run failed, regression, bill-history leak) through `post_alert`, in addition to the existing Zapier push. A healthy run posts nothing new. Until a person turns off that Zap's Slack step, a bad run would post twice. **Please report whether a `votebot_eval` job exists in this host's scheduler** (G7 and Part 1); we do not yet know which host runs it.

What is already done on the Mac: ddp-sync is on `a01094d`; CAMS runs the repeat-before-caps fix (AGENTS-184); one live check passed at 19:38 ET (the route queued an alert, hid its repeat, and it showed as GrantBot).

## 2. Gates (read-only)
- **G0. No hold.** No newer `HOLD` or contrary note on this branch, and no OPEN-437 step (canary launch, cutover, targeted rescrape) in flight. If one is, stop and report.
- **G1. Starting state.** In `/opt/ddp-sync`: branch, `git rev-parse --short HEAD`, `git status --short`. Uncommitted changes, another branch, or a commit from which `a01094d` is not a fast-forward: **stop and report.** Do not stash, discard or reset.
- **G2. Quiet window.** Nothing in flight: no scrape, archive, RDS load, LegBot relay or backfill. Stay at least an hour clear of **Sunday 02:00 UTC**, **Monday 05:00 UTC**, **Thursday 05:00 UTC** and the nightly USA runs (about **03:00 to 05:00 UTC**, per your 10-28 report). Say which window you used.
- **G3. Rollback point.** Record the commit, and the running image and container you are replacing. Tag the image as before (`ddp-sync:pre-sync132`).
- **G4. Secret keys (NAMES ONLY, never values).** Same listing as in the SYNC-131 request: `aws secretsmanager get-secret-value --secret-id ddp-sync/credentials --region us-east-1 --query SecretString --output text | python3 -c 'import json,sys;print(sorted(json.load(sys.stdin)))'`. Report:
  - (a) is `operator_alert_token` listed? Ramon reported adding it; the Mac side did not read the secret. If it is **not** listed, the deploy is still safe (no token, so the route stays off and alerts post directly as today). Report it and wait for Ramon; do not add keys.
  - (b) **`cams_api_token` must NOT be listed.** If it is, **STOP and report**: a set `cams_api_token` is the code's "this host is the Mac" signal and would send this host's replica reads to `localhost:8002` at the next restart.
- **G5. Reach CAMS.** `curl -m 5 http://10.0.0.8:8000/api/v1/health` returns 200 (it needs no credential). Report the result and time. A failure does not block the deploy (alerts fall back to direct posts); report it.
- **G6. Environment.** Do not hand-edit any env, yaml, compose or secret value.
- **G7. Job list before.** List this host's scheduler job ids and count (as you did for the SYNC-131 deploy) so the after-list can be compared.

## 3. Part 1: the deploy
1. `git -C /opt/ddp-sync pull --ff-only origin main` (expect `a01094d`). Not a fast-forward: **stop and report.**
2. `sudo systemctl restart ddp-sync`. Its `ExecStartPre` re-renders `.env`; `up -d --build` recreates only the ddp-sync container. Touch no other container.
3. Verify:
   - `systemctl is-active ddp-sync` prints `active`; the container is healthy, restarts 0; `/ddp-sync/v1/health` healthy, scheduler running, redis connected.
   - The job list matches G7. If a job appears or disappears, report which.
   - **Is `votebot_eval` in the job list?** Say yes or no.
   - No errors or tracebacks in the startup log. List any `jurisdiction_manifest_*` warnings (known and fine).
   - `git rev-parse --short HEAD` prints `a01094d`; `grep -c operator_alert_token infrastructure/render-env.sh` prints `1`.
   - `docker exec ddp-sync-ddp-sync-1 printenv OPERATOR_ALERT_URL` prints the URL above. For the token print **only whether it is set**, never the value: `docker exec ddp-sync-ddp-sync-1 sh -c 'if [ -n "$OPERATOR_ALERT_TOKEN" ]; then echo set; else echo unset; fi'`.

## 4. Part 2: one test alert (only if the token printed `set`)
This is the "one request from EC2 accepted" that INFRA-12 still needs. Send **one** labelled alert, the same way as the SYNC-131 test:
`docker exec ddp-sync-ddp-sync-1 python -c "from ddp_sync.slack_alerts import post_alert; print(post_alert('TEST (SYNC-132): EC2 ddp-sync route check, please ignore', source='sync132.test'))"`
(adapt the invocation if the SYNC-131 one differed; keep the text and source.)
- It should print `True`, and `docker logs ddp-sync-ddp-sync-1 --since 10m 2>&1 | grep -c operator_alert_sent_via_cams` should be `1`. Report any `operator_alert_rejected` or `operator_alert_cams_unreachable` line verbatim (the token is scrubbed from those logs).
- You cannot see Slack. The Mac session will read `#automation-errors` (read-only) and confirm one CodeBot message at that time and record it on INFRA-12.
- **No second alert**, even if the first looks wrong. Report.

## 5. Not authorized
- Any other host, and the votebot/ddp-api instance.
- Any env, yaml, compose or secret change, including adding or removing `operator_alert_token`.
- Pointing `OPERATOR_ALERT_URL` at a closed port or stopping CAMS to test the fallback (that is covered by tests; the Mac side will do its own check).
- Starting any scrape, archive, LegBot or backfill run, or triggering `votebot_eval`.
- A second test alert.
- A rollback without authorization, unless the container fails to come up healthy: then restore the recorded commit (`git checkout <commit>` plus `sudo systemctl restart ddp-sync`), retag the image if needed, and report. If the route misbehaves after a healthy start (alerts missing or doubled), **do not edit compose**; report. The documented off-switch (removing the `OPERATOR_ALERT_URL` line) needs Ramon's go.

## 6. Reply on this branch
One note covering:
- gates G0 to G7 with the window you used;
- the commit, image and container before and after, and the restart time;
- the job list before and after, and whether `votebot_eval` is present;
- the token `set` / `unset` result, and counts of `operator_alert_sent_via_cams`, `operator_alert_rejected` and `operator_alert_cams_unreachable` since the restart;
- the test alert result, or "token unset, test not sent".

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
