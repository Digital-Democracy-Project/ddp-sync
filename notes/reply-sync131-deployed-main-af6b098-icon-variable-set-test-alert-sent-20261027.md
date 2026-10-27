# REPLY (SYNC-131): ddp-sync on the EC2-broker host is at `main` `af6b098`; the icon variable is `:codebot:`; one test alert sent. Please check the picture in #automation-errors (2026-10-27 UTC)

Operator said "Option A, run it now" in this session after Ramon's GO (`d693f19`), and had added the secret key beforehand. This host only; the votebot/ddp-api instance was not touched. Nothing outside the scope below was changed.

## Gates (13:43 to 13:44 UTC, Tuesday; the nearest listed zones are Monday 05:00 and Thursday 05:00, so more than an hour clear)
* **G1:** branch `main`, `HEAD` `74d72fe`, working tree clean; `origin/main` `af6b098`; fast-forward possible: PASS.
* **G2:** quiet: no ddp-sync job in 30 min, no in-flight Fargate record, no scrape/archive/load/loader process, 0 Mac artifact writes and 0 jurisdiction requests in 10 min: PASS.
* **G3 (rollback point):** commit `74d72fe`; container `ddp-sync-ddp-sync-1` (id `1df91f9cf08f...`, started 2026-10-22 21:45:20); image **`4b4a3bc5aaee`**, tagged **`ddp-sync:pre-sync131`**. Health before: healthy, scheduler running, **18 jobs**, redis connected, `config_source` secrets_manager.
* **G4:** `JURISDICTION_MANIFEST_MODE` is **unset** in the container and the startup log reports `mode=shadow`: not `authoritative`, so OPEN-418 changes warnings only: continue.
* **G5 (key names only):** `ddp-sync/credentials` **lists `codebot_slack_icon_emoji`** (37 keys; a person added it today before the deploy; 36 this morning).

## Part 1: the deploy
* Old log saved first to `/home/bitnami/ops/log-archive/ddp-sync-ddp-sync-1-20261027T134423Z.log.gz`. `git pull --ff-only origin main`: **`74d72fe` to `af6b098`** (13:44:14); `sudo systemctl restart ddp-sync` **13:44:23 to 13:46:22**.
* **After:** `systemctl is-active` **active**; container **healthy**, restarts 0, image **`437a491155a6`** (started 13:46:22); `/ddp-sync/v1/health`: healthy, scheduler running, redis connected, **18 jobs (same as before)**, `config_source` secrets_manager; `git rev-parse --short HEAD` = `af6b098`; `grep -c codebot_slack_icon_emoji infrastructure/render-env.sh` = **1**; **no errors or tracebacks in the startup log**; in-flight Fargate 0.
* **Startup warnings (listed as asked):** no `jurisdiction_manifest_disagrees`. Two manifest lines at 13:46:25: **`jurisdiction_manifest_unusable error="missing field 'scheduled'" feature=archive`** (a warning) and `jurisdiction_manifest_agrees manifest_mtime=2026-10-22T21:42:25 mode=shadow`. **Cause (checked):** this host's `/opt/ddp-open-states` checkout is at `9baa61ed59` (pulled 10-22, now **10 commits behind main**); its `jurisdictions.yaml` has **0** `scheduled:` lines, while main has 10 (the `archive.scheduled` field came with ddp-open-states commit `1c69cb57d5`, "OPEN-418: split archive.enabled from archive.scheduled", also 10-22 but after the checkout). So the new ddp-sync code cannot read `archive.scheduled` from the older manifest and says so. In shadow mode it is a warning only; the scheduler registered all 18 jobs as before. I did not pull the ddp-open-states checkout (not part of this GO); doing so would clear the warning and is a separate decision.

## Part 2: the icon
* `docker exec ddp-sync-ddp-sync-1 printenv CODEBOT_SLACK_ICON_EMOJI` prints **`:codebot:`**; `CODEBOT_SLACK_USERNAME` is unset, so the default (CodeBot) applies.
* **One labelled test alert** sent at **13:47:39 UTC** with the exact command from the request (`post_alert('TEST (SYNC-131): EC2 ddp-sync alert identity check, please ignore', source='sync131.test')`): it **printed `True`**. **I cannot see Slack, so I cannot confirm the name and picture; whoever has #automation-errors open, please check the message at 13:47 UTC shows CodeBot with the :codebot: picture** and tell me. No second alert was sent.

## Rollback (not needed; only on authorization or if the container had not come up healthy)
`docker tag ddp-sync:pre-sync131 ddp-sync:prod`, `git checkout 74d72fe`, `sudo systemctl restart ddp-sync`.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
