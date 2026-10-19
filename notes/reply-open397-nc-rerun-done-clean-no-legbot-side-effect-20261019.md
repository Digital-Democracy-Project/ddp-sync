# REPLY: OPEN-397 NC re-run on v36 finished clean; nothing was started as a side effect, so NC's LegBot batch has NOT started (EC2-broker host, 2026-10-19)

Operator (Ramon's side) gave the go to run NC and accepted the NC LegBot batch (LegBot has NC-specific prompts now). Run started 04:32:32 UTC on 2026-10-19 with one call to `POST /ddp-sync/v1/trigger/openstates-scrape/nc` (HTTP 200 `{"status":"started","target":"nc"}`), no retry. Quiet check just before: no job running, no in-flight Fargate record, no Mac writes.

## Results
1. **Run id** `nc-51a2973c1231`. **Task definition** `ddp-scrapers:39` (read from ECS for this task: revision 39 = v36). Collection done 05:28:12, loading into RDS 05:28:12 to 05:32:07, `cloud_scrape: done` at 05:32:07, total 3,574.7 s. No `cloud_scrape: load failed`; **0** `DuplicateItemError` lines in this host's ddp-sync log. (The log has no separate `status: ok` line; the success is `load_ok` true then `cloud_scrape: done`.)
2. **G2 again (RDS, read-only):**
| session | classification | votes | keys | id_hash |
|---|---|---|---|---|
| `2025` | primary | **774** | **774** | `44906cf9927a198f81937675078aeed1` (unchanged) |
| `2025E1` | special | **7** | 7 | `6507209741551ae10d06580c6c493ba3` |
`2025E1` votes whose key lacks the `2025E1-` prefix: **0**. Bills now in RDS: `2025` 2,338; `2025E1` 5; every older NC session 0.
3. **Side effects:** none seen, checked about 1 to 2 minutes after the load. No LegBot dispatch, no archive, no embedding in ddp-sync's log; no Fargate task running; no in-flight record; no Mac writes to the broker. The scrape path's scrape-triggered archive hook did not fire on this manual-trigger run (I did not look into why). **So NC's first LegBot batch has not started.** With the current settings the next chance is NC's weekly archive, Thursday 2026-10-22 05:00 UTC (the archive's LegBot hook), unless someone starts the NC archive earlier. I started none.
4. **Not checked:** the broker still has 0 NC bills (the broker gets bills through LegBot's ensure step, not the scrape); I did not read why the post-scrape archive hook did not fire; no S3 objects were read.

## Also, from the Monday 05:00 UTC archives (FL, UT) and Massachusetts
Both archives ran (UT 91 s, FL 121 s, each logged `archiver_triggered_legbot_no_sessions_touched`). The hidden changelog rows were not regenerated: FL 11 of 11 still `pending`/`rejected`, UT 11 of 11 still `complete`/`rejected`, MA 290 still `complete`/`rejected`; all rejected changelogs (every state) 1,015, same as before. No MA approvals.

## Also
Broker release `1b9316ca` Stage 6 (every state) is live on the EC2-broker host and passed (broker branch Checkpoint G, `8035b5ba`). OPEN-399 (Virginia) Part 1 is accepted and will run after this load settles; Parts 2 and 3 stay held.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
