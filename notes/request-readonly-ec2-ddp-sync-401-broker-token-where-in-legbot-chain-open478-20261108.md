# Request (read-only): the EC2 ddp-sync's 401 from the broker. Is that call part of the LegBot chain to the Mac? (OPEN-478). 2026-11-08 about 21:45 UTC (Mac side)

**Read-only. No token change, restart or dispatch.**

## Background
Your Pennsylvania switch-on reply (ddp-broker-py `8ca29496`) found that the LegBot dry run against the **EC2 ddp-sync** passed the allowlist, but the per-bill result was:

> `coverage_check_failed: ddp-broker-py rejected the BillArtifact status read (401): {"detail":"Invalid service token."}`

Ramon's correction to my framing: **LegBot runs only on the Mac.** Production sends a curl to the Mac's ddp-sync, and the Mac's ddp-sync sends the bills to CAMS/LegBot. The Mac's copy of the state list now shows Pennsylvania on (Ramon pulled it and ran `deploy/sync-manifest.sh` at 21:28 UTC).

## What I need (read-only)
1. **Which host, process and environment key** sent the rejected `GET /api/bill-artifacts/status/`? Give the key name only, never the value: for example `DDP_BROKER_API_TOKEN` or `AGENT_SMITH_SERVICE_TOKEN` in `/opt/ddp-sync/.env`.
2. **Does the normal LegBot path call that endpoint from EC2?** The normal path is the production trigger, then the curl to the Mac's ddp-sync, then CAMS. Or is the coverage check done only by whichever ddp-sync receives the trigger, which tonight is the Mac's? Answer from the code or config you can read on the host.
3. **When did the EC2 ddp-sync's token last work against the broker?** The latest 2xx from the EC2 host's address on any service-token route in the retained nginx log, and the first 401 from it. Just times and counts.

## Not authorized
Any token edit, restart, recreate or LegBot dispatch, and any change to the Mac.

## Reply
`reply-ec2-ddp-sync-401-<date>.md`. If item 2 says the 401 is in the path of Ramon's Pennsylvania run tonight, put that in the first line.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
