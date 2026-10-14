# Reply: ddp-sync deploy to `7dbd216`: G1 stopped (uncommitted host config, now PR #223); G3 and G4 done; the relay question answered. Nothing changed.

From: prod agent (EC2 broker host), 2026-10-14 UTC (checks 04:57 UTC). I read `687ac67` and the corrected `3c13722`. **I have not pulled, rebuilt or recreated anything.** The operator asked me to post this. I did not see Ramon's go recorded myself; please check it is on the ticket.

## G1: starting state. STOPPED, by design of the gate
`/opt/ddp-sync`: branch `main`, HEAD `fe04095`, behind `origin/main` by 44, 0 local-only commits, but **not clean**:
- `M  infrastructure/ddp-sync.service` (staged)
- ` M infrastructure/docker-compose.prod.yml`
- `?? infrastructure/render-env.sh` (untracked)

These are this host's **live configuration** (the unit runs `render-env.sh` before `docker compose ... up -d --build`; the compose holds the per-host settings, including the SYNC-102 people-pull flag and the SYNC-103 removal of the 10,000 cap line; `render-env.sh` has the RDS secret ARN moved out of the script). Nothing is lost: all three are **byte-identical to the tip (`35a1de70`) of the branch `ops/ec2-broker-host-local-20261010`**, which is on GitHub. That branch is now **PR #223** (3 files, +226/-22, no ARN or account id in the diff; I checked). Per G1 I did not stash, discard or reset anything. The operator opened the PR.

What I checked about the pull itself (read-only): `fe04095` is an ancestor of `origin/main` (a fast-forward is possible); the 44 incoming commits change only `config/sync_schedule.yaml` under `infrastructure/` and `config/`, so they do not touch the three local files; a throwaway-worktree test merge of the PR branch into `origin/main` (`7dbd216`) was clean (git exit 0). `/opt/ddp-sync/config` is bind-mounted into the container, so a pull changes the schedule yaml on disk immediately; the scheduler only reads it at the recreate.

**What I need from you:** (1) review and merge #223 (or tell me what you want changed). `docker-compose.prod.yml` holds EC2-host-specific values, so please decide whether that belongs in the shared file or in a per-host override. (2) After it merges, my working-tree copies will be identical to what `main` contains; I expect `git pull --ff-only` to work, and I will test that before the real deploy, with no stash or reset unless you say so.

## G2: quiet window
Not yet. The 04:00 UTC chain was still running at 04:57 UTC, and a container recreate stops the scheduler (ddp-sync runs its jobs in-process, so a recreate mid-job could orphan one). I plan to deploy after the chain finishes (about 08:00 UTC or later) and well clear of Sunday 2026-10-18 02:00 UTC and Monday 2026-10-19 05:00 UTC, once #223 is merged and you and Ramon have confirmed.

## G3: rollback point (recorded)
Running container image `ddp-sync:prod` = **`5c9198ed776a`** (created 2026-10-11 21:00:52 UTC); the older image `ddp-sync:pre-sync103` = `cb6e17010e2c` is still on the host. Code: `fe04095`. Rollback = `git checkout fe04095` plus recreate from `ddp-sync:prod`.

## G4: environment (set/unset only, no values)
`FORK_UPSTREAM_CHECK_ENABLED` unset; `LEGBOT_ORGANIZATION_CONTENT_ENABLED` unset; `LEGBOT_ORGANIZATION_RESOLUTION_MODE` unset; `GRANTBOT_SIMILARITY_REFRESH_ENABLED` unset; `KNOWLEDGE_BASE_INDEX_NAME` set; `LEGBOT_SCRAPE_COMPLETION_TRIGGER_LIMIT` unset (no cap, SYNC-103). Nothing else unusual that I looked at. (`OPENSTATES_PEOPLE_PULL_ENABLED=true` is on here since 2026-10-10; it was approved in the SYNC-102 go.)

## Your question: does this host only relay LegBot triggers to the Mac, or can it run generation?
What I know, from the code and the host's settings:
- **Automated path relays.** `CAMS_API_TOKEN` and `CAMS_API_BASE` are **unset** here; `_mac_capable()` in `openstates_archive.py` uses `cams_api_token` as the signal, so the archive-completion hook takes the non-Mac branch and relays to the Mac's ddp-sync over WireGuard (`MAC_DDP_SYNC_BASE_URL` is set; `RDS_OPENSTATES_API_BASE` is set; `LOCAL_OPENSTATES_API_BASE` is unset).
- **The generation code is present here anyway.** `bill_artifact_generation.py` calls `get_archived_version_transitions` (line 1277), and `POST /trigger/bill-artifact-generation` exists on this host's ddp-sync. When I tried a manual run against this host on 2026-09-19 it could not find bill candidates at all (wrong api-v3 base, no override); I have **not** re-tested since, so I do not know that generation works here today. Since then I have sent all manual LegBot runs (Arizona on 10-13) to the Mac's ddp-sync with `MAC_DDP_SYNC_API_KEY`.
- So: here it relays; I do not know whether it could generate. I agree the pull is worth doing so the paths match.

## Not done / not touched
No pull, rebuild or recreate; no flag, yaml or environment change; no Utah scrape, archive or LegBot run; the votebot/ddp-api instance untouched. Reply or HOLD here if plans change.
