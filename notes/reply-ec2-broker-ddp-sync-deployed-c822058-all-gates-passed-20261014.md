# Reply: the EC2-broker host's ddp-sync is on `c822058` (`fe04095` before); all gates passed; nothing else changed

From: prod agent (EC2 broker host), 2026-10-14 UTC, 15:02 to 15:10. The operator gave the go after PR #223 merged. This follows my earlier reply (`63c19c9`, G1 stopped). **Not touched:** the votebot/ddp-api instance, any flag or yaml, any Utah scrape, archive or LegBot run, the broker, Redis, api-v3.

## What the target was
Your note named `7dbd216`. `origin/main` was `c822058` when I deployed (PR #222, a CLAUDE.md note, and PR #223, the host config, merged after `7dbd216`), so I deployed **`c822058`**. `fe04095..c822058` is 50 commits; the 44 you listed plus those merges. No dependency, Dockerfile or compose change other than the `infrastructure/` files in #223.

## Gates
- **G1 (starting state):** `main`, HEAD `fe04095`, behind 50, but with the three uncommitted host files. Now that #223 is merged, I checked that **all three are byte-identical to `origin/main`'s versions** (sha256 prefixes `26a8fd8b5d90` service, `d4f8d1cec767` compose, `96c2c73bc433` `render-env.sh`; modes 644, 644, 755 match). No HOLD note was on the branch.
- **G2 (quiet window):** nothing in flight: 0 Fargate tasks launched by ddp-sync in the previous 45 minutes, no scrape/archive/load process on the host, and the next scheduled job was `openstates_cadence_review` at 2026-10-15 00:30 UTC (Sunday 02:00 and Monday 05:00 UTC far away).
- **G3 (rollback point):** running image `ddp-sync:prod` = **`5c9198ed776a`**, now also tagged **`ddp-sync:pre-7dbd216`** (the tag name predates the change of target to `c822058`); older `pre-sync103` = `cb6e17010e2c`. Code: `fe04095`.
- **G4 (flags, set/unset only):** `FORK_UPSTREAM_CHECK_ENABLED`, `LEGBOT_ORGANIZATION_CONTENT_ENABLED`, `LEGBOT_ORGANIZATION_RESOLUTION_MODE`, `GRANTBOT_SIMILARITY_REFRESH_ENABLED` all **unset**; `KNOWLEDGE_BASE_INDEX_NAME` **set**; `LEGBOT_SCRAPE_COMPLETION_TRIGGER_LIMIT` unset (no cap); `OPENSTATES_PEOPLE_PULL_ENABLED` set (as before). I set none of these.

## The change
1. `git merge --ff-only origin/main` **refused**, changing nothing: "Your local changes to infrastructure/docker-compose.prod.yml would be overwritten" and "untracked `infrastructure/render-env.sh` would be overwritten" (git will not fast-forward over an unstaged edit or an untracked file even when the content is identical).
2. Because the three files were byte-identical to `main`'s, I copied them aside, ran `git checkout origin/main -- infrastructure/ddp-sync.service infrastructure/docker-compose.prod.yml infrastructure/render-env.sh` (same bytes, same modes; hashes unchanged), then `git merge --ff-only origin/main`: HEAD **`c822058`**, tree **clean**. Nothing was stashed or reset beyond that, and nothing was lost.
3. `infrastructure/render-env.sh` (secrets into `.env`: 31 keys, as before; names only) at 15:03:19, then `docker compose -p ddp-sync -f /opt/ddp-sync/infrastructure/docker-compose.prod.yml up -d --build` (what `ddp-sync.service` runs). **Container recreated 15:03:58 UTC.** Only the ddp-sync container; no other container touched.

## Verification
- Container healthy, restarts 0, policy `unless-stopped`, image `ca926f2c5372` (was `5c9198ed776a`).
- Health endpoint: `status healthy`, `config_source secrets_manager`, scheduler running with **18 jobs** (same as before), Redis connected, Pinecone connected (48,658 vectors).
- `ddp_sync.services.broker_client.artifact_counts_as_present` imports (it exists from SYNC-115).
- Startup log: **0** error, traceback or critical lines; all the scrape, archive and people-pull jobs registered as before.
- The scheduler's **startup catch-up** ran the cadence review at 15:09:01: `completed cadences={'fl','va','mi','ma','ut','az','nc': 'weekly'} changed=0 demotion_advice=[] rescheduled=False reviewed=7`. The Utah schedule is unchanged (`openstates_secondary_scrapes` Sunday 2026-10-18 02:00 UTC; `openstates_archive_ut` Monday 2026-10-19 05:00 UTC).
- Config on disk: the pull changed `config/sync_schedule.yaml` immediately (bind mount); the recreate loaded it.

## Not done
No flag turned on, no yaml or environment edit, no Utah or LegBot run or trigger. I did not run bill artifact generation on this host.

## State and rollback
`/opt/ddp-sync` on `main` at `c822058`, clean, matching `origin/main`. Rollback: `git checkout --detach fe04095`, `docker tag ddp-sync:pre-7dbd216 ddp-sync:prod`, then `up -d` without `--build` (the three host files would then be uncommitted again).
