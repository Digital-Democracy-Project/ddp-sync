# REQUEST (waiting for Ramon's go): give EC2's ddp-sync alerts the CodeBot icon (SYNC-131, 2026-10-27)

For the agent on the EC2-broker host. **Do nothing until Ramon writes "go" for this note** (in a commit on this branch that names this file, or directly to you). Ramon chose to have this done by you, through this note.

## Why
Alerts from this host post to #automation-errors as "CodeBot" with the plain :robot_face: icon, because the icon setting never reaches the container. ddp-sync PR #243 (merged, commit af6b098) taught `infrastructure/render-env.sh` to copy two optional keys from the `ddp-sync/credentials` secret into the rendered `.env`: `codebot_slack_icon_emoji` -> `CODEBOT_SLACK_ICON_EMOJI` and `codebot_slack_username` -> `CODEBOT_SLACK_USERNAME`. Only the icon needs setting; the name already defaults to CodeBot.

## Gates (read-only; stop and report if any fails)
1. `/opt/ddp-sync` contains af6b098: `git -C /opt/ddp-sync merge-base --is-ancestor af6b098 HEAD && echo ok`. If not, **do not pull**: pulling main would also deploy everything merged since this host's commit. Report `git -C /opt/ddp-sync log --oneline -1` and `git -C /opt/ddp-sync log --oneline HEAD..origin/main | wc -l`, and stop.
2. `grep -c 'codebot_slack_icon_emoji' /opt/ddp-sync/infrastructure/render-env.sh` prints `1`.
3. A quiet window: no scrape, archive or RDS load in progress (your usual pre-restart check). The restart rebuilds and recreates the container (`up -d --build`).

## Steps
1. **Production write, the secret.** Add one key to `ddp-sync/credentials` (us-east-1): `codebot_slack_icon_emoji` = `:codebot:`. Leave every other key as it is. This host's role is read-only, so this probably needs a person with write access (AWS console: Secrets Manager -> ddp-sync/credentials -> Retrieve secret value -> Edit -> add a row). If you do have write access, do a read-modify-write of the JSON that never prints a value, and show the exact command to Ramon before running it.
   Check, names only: `aws secretsmanager get-secret-value --secret-id ddp-sync/credentials --region us-east-1 --query SecretString --output text | python3 -c 'import json,sys;print(sorted(json.load(sys.stdin)))'` lists `codebot_slack_icon_emoji`.
2. Restart: `sudo systemctl restart ddp-sync` (its `ExecStartPre` re-renders `.env`).
3. Verify (not a secret): `docker exec ddp-sync-ddp-sync-1 printenv CODEBOT_SLACK_ICON_EMOJI` prints `:codebot:`. `systemctl is-active ddp-sync` prints `active` and `curl -s localhost:8001/health` is healthy.
4. One labelled test alert:
   `docker exec ddp-sync-ddp-sync-1 python -c "from ddp_sync.slack_alerts import post_alert; print(post_alert('TEST (SYNC-131): EC2 ddp-sync alert identity check, please ignore', source='sync131.test'))"` prints `True`, and the message in #automation-errors shows the name CodeBot with the :codebot: picture.

## Report back
On this branch: the gate results, who added the key and when, the `printenv` output, the test alert's time, and whether the picture showed. Then the SYNC-131 ticket can be closed.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
