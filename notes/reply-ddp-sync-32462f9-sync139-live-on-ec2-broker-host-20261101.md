# Reply: ddp-sync `32462f9` (SYNC-139) is LIVE on the EC2-broker host, 2026-11-01 00:06 to 00:08 UTC

Operator said go in this session ("option A, if we're in a quiet window"). Nothing was rolled back. No env, yaml, compose or secret value was edited; no sync, scrape, archive, LegBot or backfill job was started or triggered; no broker restart; the votebot/ddp-api instance was not touched.

## Gates
- **G0:** no new notes on this branch, the broker branch or the open-states branch since your request; no hold.
- **G1:** `/opt/ddp-sync` on `main` at `a01094d`, `git status --short` empty; `32462f9` is a fast-forward (checked: `a01094d` is an ancestor); the diff is the nine files you listed (368 insertions, 131 deletions).
- **G2, window used:** **Sunday 2026-11-01 00:06 to 00:08 UTC**, before the 00:30 cadence review and about two hours before the Sunday 02:00 batch (so more than the hour you asked for). Nothing in flight: no ddp-sync log lines in the last 10 minutes except my own schedule read; no VPN writers on the broker in 10 minutes; the newest scraper stream last logged 10-31 22:41; in-flight Fargate record 0.
- **G3, rollback point:** commit `a01094d`; the running image `12bb8d030751` (container started 2026-10-29 01:03:31 UTC, restarts 0) tagged **`ddp-sync:pre-sync139`**. Old logs gzipped to `/home/bitnami/ops/log-archive/ddp-sync-pre-32462f9-20261101T0005Z.log.gz` before the restart.
- **G4, which api-v3:** this host's ddp-sync has no CAMS token (`cams_api_token` unset), so a replica jurisdiction routes to the setting **`rds_openstates_api_base` = `http://10.0.0.11:8002`**, which is **this host's own WireGuard address (wg0)**, i.e. the EC2 api-v3 you updated on 10-31 (now `aed937a`). (`local_openstates_api_base` is `http://localhost:8002`, which from inside the container reaches nothing; not used here.) With ddp-sync's own key from its settings (not printed): `GET /jurisdictions/va?include=legislative_sessions` **200, 33 sessions, every session has both flags as true/false**, current `2026`. I also read all nine of this host's sync jurisdictions: every one answers 200 with flags on every session (current: FL 2026, WA 2025-2026, US 119, VA 2026, MI 2025-2026, MA 194th, UT 2026, AZ 57th-2nd-regular, NC 2025).
- **G5:** `DDP_OPENSTATES_JURISDICTIONS` = **FL, WA, US, VA, MI, MA, UT, AZ, NC** (US and MI present). `LEGBOT_RDS_REPLICA_JURISDICTION_ALLOWLIST` = **empty** (the known unset-allowlist state; unchanged). **AL is not on the list**, so it is read from the public OpenStates API (see Part 2).
- **G6:** nothing hand-edited. **G7:** 19 jobs before.

## Deploy
- `git pull --ff-only` to **`32462f9`**; `HEAD` prints `32462f9`, clean.
- `sudo systemctl restart ddp-sync` at 00:06:04 UTC (the command returned after 41 s); container **healthy at 00:07:49**, restarts 0, `systemctl is-active` = active. New image `dca536879f5e`, container started **2026-11-01 00:06:45 UTC**. Only the ddp-sync container was recreated; the broker, api-v3, VoteBot and Redis containers kept their uptimes.
- `/ddp-sync/v1/health`: healthy, `config_source` secrets_manager, scheduler running with **19 jobs**, redis connected, pinecone connected (48,981 vectors).
- **Job list after = before** (same 19 ids: grantbot_scrape, jurisdiction_drift_check, openstates_archive_{al,az,fl,ma,mi,nc,us,ut,va,wa}, openstates_cadence_review, openstates_fl_scrape, openstates_people_pull, openstates_people_refresh, openstates_secondary_scrapes, openstates_usa_scrape, openstates_wa_scrape). No job added or removed.
- No tracebacks or errors in the startup log.

## Part 2: what ddp-sync now says is current (read-only; `OpenStatesSource().get_current_session_identifiers(j)` inside the container, no job triggered)
| jurisdiction | identifiers | | jurisdiction | identifiers |
|---|---|---|---|---|
| US | `['119']` | | MA | `['194th']` |
| AZ | `['57th-2nd-regular']` | | MI | `['2025-2026']` |
| FL | `['2026']` | | NC | `['2025']` |
| UT | `['2026']` | | VA | `['2026']` |
| WA | `['2025-2026']` | | **AL** | **`[]`** |
**The one difference from your expected table: AL returns an empty list** (you expected 2026rs). AL is not in `DDP_OPENSTATES_JURISDICTIONS` on this host (G5), so ddp-sync asks the public OpenStates API, which carries no `is_current` flag; I did not edit the list (Ramon's call). AL is not one of this host's sync jurisdictions (it has an archive job only); I did not check whether anything on this host asks ddp-sync for AL's current session.

## Log lines since the restart
`api-v3 did not publish is_current on this jurisdiction's sessions` (error): **0**. `Cannot tell the current session (api-v3 unreachable or no is_current flag)` (warning): **0**. (Counted after my own Part 2 reads, which include the AL one.)

## Rollback
`git checkout a01094d` in `/opt/ddp-sync`, re-tag `ddp-sync:pre-sync139` as the image if needed, `sudo systemctl restart ddp-sync`. Not needed.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
