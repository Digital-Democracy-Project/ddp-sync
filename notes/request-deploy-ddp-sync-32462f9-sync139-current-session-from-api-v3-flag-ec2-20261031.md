# Request (SYNC-139): bring the EC2-broker host's ddp-sync to `main` `32462f9`; ddp-sync now reads "current session" from api-v3's flag (Mac side, 2026-10-31)

Written by the Mac session at Ramon's instruction ("ask the prod agent to deploy the ddp-sync fixes as well"). **Nothing here has been run on production.** Ramon decides the time; please pick a quiet window and say which. Do each part only if every gate passes. If a gate fails, stop and report; do not work around it.

**Scope: this host only.** Do **not** touch the votebot/ddp-api instance. **Do not combine this restart with a broker restart:** ddp-sync writes to the broker, so a broker restart in the middle of a ddp-sync job fails writes. The broker release (BROKER-369, `/broker-deploy`) is a separate request; whichever goes first, wait for the other to be fully back before starting it.

## 1. What the pull changes (`a01094d..32462f9`, checked on the Mac)
`a01094d` is the commit you recorded for this host on 10-29 (your reply `b7f31ac`). Read your own `HEAD` in G1; if it is a different commit, say so before pulling. **One merge, PR #256 (SYNC-139):** 9 files (`ingestion/sources/openstates.py`, `pipelines/bill_sync.py`, `pipelines/knowledge_base_backfill.py`, `services/local_openstates_client.py`, three test files, `primitives.md`, `TROUBLESHOOTING.md`). **No dependency, Dockerfile, compose, env, schedule or job change; no job added or removed.**

What it does: ddp-sync used to work out "the current session" itself from dates, and was wrong between sessions (FL, UT and VA returned 2027, a session that has not started; VA and WA returned nothing). It now asks api-v3 for each session's `is_current` flag (api-v3 OPEN-467, live on the EC2 api-v3 since your report `99e96bc`, 10-31 20:44 UTC) through one function, `OpenStatesSource.get_current_session_identifiers()`, which returns a **list** (VA and UT can have two current sessions at once). **There is no date fallback:** if api-v3 is unreachable, or a jurisdiction's sessions carry no `is_current`, ddp-sync treats it as "unknown": bills of that jurisdiction are skipped for that run and a line is logged.

What it changes for the sessions ddp-sync syncs (the PR's table, today 2026-10-31, from real session lists): **unchanged** for AL, AZ, MA, MI, NC, US; **fixed** for FL (2027 to 2026), UT (2027 to 2026), VA (nothing to 2026), WA (nothing to 2025-2026).

## 2. Gates (read-only)
- **G0. No hold.** No newer `HOLD` or contrary note on this branch, and no OPEN-437 step in flight.
- **G1. Starting state.** In `/opt/ddp-sync`: branch, `git rev-parse --short HEAD`, `git status --short`. Uncommitted changes, another branch, or a commit from which `32462f9` is not a fast-forward: **stop and report.** Do not stash, discard or reset.
- **G2. Quiet window.** Nothing in flight: no scrape, archive, RDS load, LegBot relay or backfill. Stay at least an hour clear of **Sunday 02:00 UTC**, **Monday 05:00 UTC**, **Thursday 05:00 UTC** and the nightly USA runs (about 03:00 to 05:00 UTC). Say which window you used.
- **G3. Rollback point.** Record the commit, and the running image and container you are replacing. Tag the image **`ddp-sync:pre-sync139`**.
- **G4. Which api-v3 this host reads, and does it publish the flag? (the one new gate).** Report **which api-v3 address** this ddp-sync reads for OpenStates (name the setting and the host and port, not any key). Then, with that service's read key (**do not print it**), `GET /jurisdictions/va?include=legislative_sessions` and report: status, and whether **every** session has `is_current` and `is_previous` (expected: yes, 33 sessions for VA). If this host reads an api-v3 other than the EC2 one you updated on 10-31, or the answer has no flags: **STOP and report** (the deploy would make that host skip every jurisdiction's bills).
- **G5. Jurisdiction list (names only).** Report the **values** of `DDP_OPENSTATES_JURISDICTIONS` and `LEGBOT_RDS_REPLICA_JURISDICTION_ALLOWLIST` on this host (two-letter codes only; they are not secrets). A jurisdiction this host syncs but that is **not** in `DDP_OPENSTATES_JURISDICTIONS` is read from the public OpenStates API, which has no flag, so its bills would be skipped with an error logged. If **US or MI** is missing, **STOP and report** before restarting. Do not edit any env value; Ramon decides.
- **G6. Environment.** Do not hand-edit any env, yaml, compose or secret value.
- **G7. Job list before.** List this host's scheduler job ids and count (19 on 10-29) so the after-list can be compared.

## 3. Part 1: the deploy
1. `git -C /opt/ddp-sync pull --ff-only origin main` (expect `32462f9`). Not a fast-forward: **stop and report.**
2. `sudo systemctl restart ddp-sync` (its `ExecStartPre` re-renders `.env`; `up -d --build` recreates only the ddp-sync container; touch no other container).
3. Verify:
   - `systemctl is-active ddp-sync` prints `active`; the container is healthy, restarts 0; `/ddp-sync/v1/health` healthy, scheduler running, redis connected.
   - The job list matches G7 (19). If a job appears or disappears, report which.
   - No errors or tracebacks in the startup log.
   - `git rev-parse --short HEAD` prints `32462f9`.

## 4. Part 2: read what ddp-sync now says is current (read-only, no job triggered)
- From inside the ddp-sync container, call `OpenStatesSource(...).get_current_session_identifiers(<jurisdiction>)` for each jurisdiction in G5's list (adapt the invocation to how the container builds that source; it only reads api-v3). Expected today, from api-v3's flags: **US 119; AL 2026rs; AZ 57th-2nd-regular; FL 2026; MA 194th; MI 2025-2026; NC 2025; UT 2026; VA 2026; WA 2025-2026.** A difference is **not** a failure of the deploy: report it with the jurisdiction. If you cannot construct the call in one line, say so and skip this part; do not run a job instead.
- Since the restart, count these log lines and print any verbatim: `api-v3 did not publish is_current on this jurisdiction's sessions` (error) and `Cannot tell the current session (api-v3 unreachable or no is_current flag)` (warning). Expected: 0. A line names the jurisdiction in its `jurisdiction=` field.

## 5. Not authorized
- Any other host, and the votebot/ddp-api instance.
- Any env, yaml, compose or secret change, including `DDP_OPENSTATES_JURISDICTIONS` and the allowlist.
- Starting any sync, scrape, archive, LegBot or backfill run, or triggering any job to "test" this.
- A broker restart or deploy in the same window.
- A rollback without authorization, unless the container fails to come up healthy: then restore the recorded commit (`git checkout <commit>` plus `sudo systemctl restart ddp-sync`), retag the image if needed, and report. If it starts healthy but skips jurisdictions (the two log lines above, or a missing flag), **do not edit anything**; report.

## 6. Reply on this branch
One note covering:
- gates G0 to G7 with the window you used, and G4's api-v3 address and the VA flag result, and G5's two lists;
- the commit, image and container before and after, and the restart time;
- the job list before and after;
- the Part 2 table (jurisdiction, identifiers) or "skipped, and why", and the counts of the two log lines since the restart.

What closes SYNC-139: this reply (live, jobs unchanged, the flag read on this host, no skips) plus Ramon's acceptance.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
