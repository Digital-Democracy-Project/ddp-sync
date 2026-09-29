# LegBot concept-statement dedup regenerates duplicates: US/119 (2026-09-29)

From the prod agent on the EC2 host (`/opt/ddp-open-states`, ip-172-31-78-173). Nothing in this
note is a credential. Everything below was read-only observation of the broker DB, the ddp-sync
container logs, and ddp-sync source; I changed nothing except the one FL trigger call described
under "Currently blocked on".

## Where this stands

**Observed facts (broker DB / logs, all times UTC, 2026-09-29):**

- The archive-completion hook triggered LegBot for US/119: the US archive Fargate task finished
  at 03:20:06, the hook began its touched-sessions read at 03:20:16, and the first artifact landed
  at 03:21:58. Dispatch is session-wide and coverage-aware (`limit` 10000,
  `include_concept_statements=true`, org research off).
- **Artifact phase** ran 03:21 to 10:54: 344 bills touched (298 brand new to LegBot, 46 with
  earlier artifacts), 2,443 artifacts, 297 of them `failed`. Nothing has been written for
  US/119 artifacts since 10:54.
- **Concept-statement phase** is still running as of 14:43: 2,916 new US/119 `ConceptStatementSet`
  rows since 03:22 (newest 14:43:35).
  - **2,618 of the new rows are for gov_ids that already had a set** from the 09-19..09-25 run
    (18,367 sets, all model_name mlx bar 6). 2,618 gov_ids now have two sets. Only about 298 of the new
    rows are for bills with no earlier set, which matches the 298 brand-new bills above.
  - Every US/119 set is `pending`: 21,248 of 21,248. None are published.
  - The new rows carry a `bill_version`; all 18,367 old rows have `bill_version` NULL.

**From reading the code (not from running it):** in `pipelines/session_pipeline_runner.py`
(around lines 869-881) the concept-statement dedup calls `get_concept_statement_set`, which only
returns a *published* set (its docstring says a set can also exist in `pending` or `rejected`
status, which it never surfaces). So with nothing published, every bill looks uncovered on every
run and each run writes a fresh `pending` set per bill. That is consistent with the data above,
but I have not confirmed it by inspecting a live request.

**Estimates, not observations:**

- About 15,700 bills remain (18,367 minus the 2,618 already redone), at the observed rate of
  roughly 255 sets an hour: about 60 hours of MLX time producing duplicates.
- MI (15,924 sets), WA (3,235) and VA (445) are also all unpublished, so a rerun there would
  do the same. I did not check their publish status.

**Corrections to things I said earlier today:** I first estimated about 106 bills for the run
and then about 335 concept versions remaining. Both were wrong. The first counted only the broker's
own Bill rows, the second compared row counts against bill versions. I also suspected the NULL
`bill_version` on the old rows as the cause; the code says otherwise, so treat it as incidental.

## Currently blocked on

- **FL 2026 is not running.** At 05:21 UTC I sent `POST http://10.0.0.8:8001/ddp-sync/v1/trigger/bill-artifact-generation`
  with `jurisdiction_iso2=FL`, `session_code=2026`, all 9 artifact types,
  `include_concept_statements=true`, `include_org_research=false`, `limit=100000`,
  `retry_failed=false`, `dry_run=false`, header `X-DDP-Environment: prod`, bearer
  `MAC_DDP_SYNC_API_KEY`. It was accepted (no 401/409) and the curl (PID 189430 on this host) is still open
  about nine hours later. The broker has 0 FL 2026 artifacts and 0 FL 2026 concept sets. Most likely it is
  queued behind the US concept phase (CAMS `_mlx_semaphore`), but I could not confirm that from EC2.
  Separately, FL 2026 had 82 bills in the broker and no ingested bill versions when I looked,
  so the call may find nothing to generate.
- **I can't see or stop the Mac's run from EC2.** It executes on 10.0.0.8.
- **Container logs before 09-26 20:39 are gone.** `ddp-sync-ddp-sync-1` was recreated then
  (json-file driver, no rotation opts, no shipping), so earlier LegBot trigger history is unrecoverable here.
- **Known issue, already reported:** httpx INFO log lines print the read-only api-v3 key in plaintext on every call.
- **Unrelated, FYI:** this host's SSH key is rejected by GitHub (`Permission denied (publickey)`), so
  `ddp-open-states` fetch/push fails from here. I posted this note over the HTTPS remote of ddp-sync instead.

## Next step

1. Stop the US/119 run on the Mac if it is still generating concept sets, and say whether the
   2,618 duplicate `pending` sets should be cleaned up. I have deleted nothing.
2. Decide FL 2026: let it wait in the queue, or re-send with `include_concept_statements=false`.
   I have not cancelled the open curl.
3. Fix the dedup so it counts `pending` sets, or publish/reject the existing ones (PR in ddp-sync).
4. Decide whether to set `LEGBOT_SCRAPE_COMPLETION_TRIGGER_INCLUDE_CONCEPT_STATEMENTS=false` on the
   triggering hosts until the fix lands. It is `true` on this host today.
5. Reply on this same branch either way.
