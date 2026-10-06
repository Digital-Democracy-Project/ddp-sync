# Reply: PR #199 is deployed; the organizations job ran clean (`written=0 unchanged=5121`) and all 5,121 organization records are now permanent (2026-10-06)

From the prod agent on the EC2-broker host. Answers `request-deploy-sync91-org-records-never-expire-pr199-20261006.md` (this branch). Times are UTC, 01:47 to 02:04. Done with the operator's go-ahead. (The TTL thread this answers started on ddp-broker-py's `notes/ops-handoff`: `finding-embedding-record-ttl-and-redis-policy-20261005.md` and `finding-update-ut-records-now-permanent-65467-still-expiring-20261006.md`; I am posting here, where the request was, and the operator can point Ramon to this note.)

## 1. Deploy

- Quiet window checked before the pull and again before the recreate: no ECS tasks, nothing in flight, no backfill locks; the next job was the 02:30 WA scrape. Rollback tag **`ddp-sync:pre-pr199`** (`1a44b6ff54e1`, the #193 build).
- `git pull --ff-only`: `/opt/ddp-sync` `a8a96fa` -> **`9449524`** (14 files: #199 and SYNC-99 #194 to #197; `config/sync_schedule.yaml` unchanged, as you said). `git status` before and after is identical: my local uncommitted files (`docker-compose.prod.yml`, `ddp-sync.service`, `render-env.sh`) were not touched by the pull, and the rendered compose still has my host-specific lines.
- Rebuilt as `ddp-sync:prod` = **`98e44c9b32d8`** (about 1.5 minutes), `up -d --no-build ddp-sync` at 01:50:09; healthy within 14 s, 17 jobs, 0 restarts, 0 startup errors. Inside the container: the hook is `enabled: true`, ledger `1000000`, alert 0, `ddp_broker_api_base` = the public hostname (your #193 fix still works), `knowledge_base_index_name` = `ddp-knowledge-base`.
- **SYNC-99 on this host:** I read the diff for anything that could break here. `SLACK_BOT_TOKEN` is set on this host and is the only credential the alerts use; `codebot_identity()` in the running container returns the defaults `{'username': 'CodeBot', 'icon_emoji': ':robot_face:'}` (no `CODEBOT_SLACK_*` variables are set here, so the defaults apply). I have **not seen an alert post** since the deploy, so I cannot say whether the Slack app has the `chat:write.customize` scope; per your module docstring a missing scope cannot make a post fail.

## 2. The organizations job (`POST /ddp-sync/v1/trigger/knowledge-base-entities/organizations?dry_run=false`)

Run id `kb-entities-organizations-run-daa53bc1e05b`, started 01:50:42, finished 02:01:32 (**10 min 50 s**). Result line, exactly:

```
2026-10-06 02:01:32 knowledge_base_entities_run  complete=True entity=organizations failed=0 listed=5127 skipped_no_content=6 unchanged=5121 would_write=0 written=0
```

As you expected: `listed 5127`, `written 0`, `unchanged 5121`, `failed 0`. Counted strictly after the start time: 0 organization documents ingested, **0 OpenAI requests**, 0 problem lines. The Pinecone count was **431,287 vectors before (taken immediately before the run) and unchanged after**.

## 3. The organization keys (`ddp:bill_version:organization:*`, Redis db 3)

- **Before the run (01:50:38): 5,121 keys, 0 permanent, 5,121 expiring** (shortest 89.9 days).
- **After the run (02:03:34): 5,121 keys, 5,121 permanent (TTL -1), 0 expiring.** So the repair worked as designed: nothing re-embedded, and the expiry is gone.
- **All `ddp:bill_version:*` keys now: 66,488; 6,142 permanent; 60,346 still expiring (87.5 to 89.5 days left).** The 60,346 are **all bill records**: the six jurisdictions (FL, MI, VA, WA, AZ, US) whose first real ledger pass has not run yet, exactly what you predicted. The permanent ones are the 5,121 organization records plus UT's 1,021 bill records (permanent since UT's ledger pass at 01:27). I will re-scan after the overnight archives (WA about 02:30, USA 03:00, AZ 05:00) and report which jurisdictions flipped.

Reply on this branch either way.
