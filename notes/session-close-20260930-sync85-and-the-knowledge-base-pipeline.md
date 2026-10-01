# Session close, 2026-09-30 to 2026-10-01 (Mac Studio session): SYNC-85 live, the knowledge-base pipeline merged but OFF

End-of-night summary for whoever picks this up next. Times EDT unless marked. State read at about 00:30 on 2026-10-01.

## Where things stand right now

**LegBot pipeline: healthy and running.** US/119 run `08267202-13e7-4f63-bec3-157708a8feb1` (trigger run `f90fb88c…`)
started 23:17 and had completed 1,093 bills by 00:26 (`limit=10000`, `include_concept_statements=true`,
`retry_failed=false`). It has not logged `session_pipeline_run_end` yet, so there is no final count and I
cannot give a finish time. FL 2026 finished earlier (1,897 bills in 23.2 h, ended 9/30 16:58).
Its overlap lock was live (TTL about 540 s, being renewed) after the Redis restart below.

**SYNC-85 is live and working.** In the US/119 run, 894 of the first 1,093 bills were skipped as
`already_exists_pending`; only 55 dispatched concept statements (bills that really had no set), 142 were
`nothing_to_publish`, and the fallback warning `concept_statement_status_endpoint_missing_falling_back_to_published_only`
never appeared. The broker is at `026e623e` on the EC2 host (route verified through the proxy with the
service token); the Mac's ddp-sync was restarted 9/29 17:12 at `1a7df2d`.

**The earlier request to set `CONCEPT_STATEMENT_REQUIRE_REVIEW=false` is superseded and should be treated as
withdrawn.** It was never applied (the prod agent declined to without a decision), and SYNC-85 solved the
duplicates without touching review. Both review flags are `True` on the prod broker and should stay that
way: turning concept-set review off would make them the only LegBot output that skips human review. (The
prod agent's correction note also found `BILL_ARTIFACT_REQUIRE_REVIEW` is `True` there, contrary to the premise
in the earlier Mac-side reply; that premise was wrong.)

**Redis restarted twice tonight; cause unknown, nothing changed.** `ddp-agents-redis-1` (shared by CAMS and
ddp-sync) restarted around 20:3x and again at 00:21; each reload took about 25 s. During the second, ddp-sync's
`/health` showed `redis: error`, and the log filled with `Redis is loading the dataset in memory`; the pipeline
kept completing bills and recovered by itself. What I saw: 518,649 keys, 7.98 GB used, `maxmemory` unset, nearly
all keys `cams:state:*` (CAMS's own state), only 3,430 with an expiry; the snapshot being loaded reported about
8 GB. I did not investigate further (not ddp-sync's data). Worth someone owning: set expiries on `cams:state:*`,
set a `maxmemory`, and check whether Colima/Docker memory pressure is what is killing it.

## Merged today, and what is actually deployed

Merged is **not** deployed. Nothing in the knowledge-base pipeline is running anywhere.

| Change | PR(s) | Deployed? |
|---|---|---|
| SYNC-85 concept dedup counts pending sets | ddp-broker-py #378, ddp-sync #173 | **Yes**, both (broker 9/29 evening, Mac restart 17:12) |
| SYNC-89 index setting + throughput measurement; `ddp-knowledge-base` created (empty) | ddp-sync #174 | Index exists; code on `main` only |
| SYNC-83 embedding hook, SYNC-87 search refresh hook, SYNC-90 staged backfill, SYNC-91 legislators + organizations | ddp-sync #175, #176, #177, #178 | `main` only. The Mac's production checkout is at `1a7df2d`, **15 commits behind `main`**; all of it is default-off, so deploying is safe but optional, and a restart interrupts the running US/119 job |
| OPEN-311 api-v3 version fields | api-v3 #15 | `main` only; **OPEN-315** is the deploy to both instances |
| OPEN-312 replica monitor | ddp-open-states #259, #260; ddp-agents #302 | In both production checkouts (`c98fcd6`, `0b2accb3`); **unproven** (below) |
| VOTEBOT-8 (`ocd_bill_id` resolve/filter), VOTEBOT-10 (version-aware retrieval) | votebot #8, #9 | `main` only; default-off |

Docs PRs opened tonight, **not merged**: ddp-sync #179 (README, CLAUDE.md), ddp-broker-py #391 (primitives),
ddp-infra #186 (plan measurements, status, a route correction).

## Open items, in the order I would take them

1. **Merge the three docs PRs** (#179, broker #391, infra #186).
2. **OPEN-315** (prod agent): deploy api-v3 `main` to the RDS-backed instance and the Mac's, verify `archived_document_id`
   stability, then one real bill end to end through the hook. Nothing in the pipeline can be enabled before this, and
   VoteBot's version-aware retrieval shows "current version unknown" on every bill until the RDS api-v3 is redeployed.
3. **OPEN-313: the replica monitor has never been seen alive.** It crashed silently under launchd until #260
   (no `HOME`, `docker` not on PATH). `cams status` would not show a `ddp_legbot_replica` heartbeat when I looked,
   because Redis was down or loading. Re-check `cams status` first; then the labelled alert test and the RDS-credential
   decision for the schema check. Until then treat the Mac replica as unmonitored.
4. **BROKER-177 / the existing duplicates:** the prod agent's read-only report request (queries in
   `ddp-broker-py`'s `ops-handoff`, `request-readonly-duplicate-concept-set-report-20260929.md`) has not been answered
   in anything I saw. Decide: leave, reject, or delete the ~3,860 extras (identical text vs different generations decides it).
5. **VOTEBOT-11** (hardening of VOTEBOT-8's exact `votebot-large` string compare, wrong-state bill lookup, session guess)
   must land before SYNC-92. **BROKER-144** (broker PR #389) must deploy before organizations can be embedded.
6. **SYNC-92 / SYNC-88** (cutover, retirement) are untouched; both need Ramon.

## Decisions waiting on Ramon

- `suggest` latency through ddp-api: p95 348 ms against the 150 ms bar. The stop condition was overridden by the
  user, not confirmed by Ramon (prod agent's note, section 7).
- VOTEBOT-10 item 6: when a new bill version is archived but not yet embedded, bill chat answers without text until
  the hook catches up. Fall back to the previous version with a note, or keep it?
- SYNC-91: legislator documents carry the OpenStates record, **not** the Webflow bio the legacy ones used; organizations
  have no schedule; `legislator-bills`/`legislator-votes` are not migrated.
- Whether to add a monthly schedule for organizations once BROKER-144 is live.

## Known issues not fixed

- ddp-sync's httpx INFO logging prints api keys: the prod agent reported the read-only api-v3 key in plaintext on every
  call, and the Mac's log shows `apikey=` in query strings for local-replica calls. Needs scrubbing (not ticketed that I know of).
- The prod host's SSH key is rejected by GitHub (`ddp-open-states` fetch/push fails from there).
- `test_retry_failed_is_passed_through` fails intermittently on `main` (it passed in some full runs tonight).

## What to remember about how tonight went

- "Done" tickets here often mean the code merged, not that it is live. I closed tickets only with the unfinished
  deploy/verification work filed separately (OPEN-313, OPEN-315, BROKER-177) so it is not lost.
- A silent `|| true` hid a completely broken monitor for a day. After any change to a scheduled hook, confirm its
  heartbeat or log line exists; absence of errors proves nothing.
- Check the Redis container before concluding the pipeline stopped.

Nothing destructive was done on the votebot/ddp-api EC2 instance or its diverged branch; no rows were deleted or
published; no review flag was changed.
