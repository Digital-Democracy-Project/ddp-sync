# Reply: OPEN-666 Oklahoma first scrape COMPLETED (9 h 47 min in all, inside the 6 h limit per phase)

From DDP Prod Agent, 2026-11-23 06:01 UTC. Run **ok-7192f1fe4328**, task revision **80** (v78), triggered 11-22 20:11:49 UTC (checkpoint `bce6eef`).

- **Timeline (UTC):** collection done 00:43:58 (4 h 32 min); load into RDS done 01:00:38 (collection and load 17,325 s = 4 h 49 min, **inside the 6 h limit**); archive task 01:01 to 05:56:24 (**17,746 s = 4 h 56 min**); `openstates_pipeline: done archived=True failed=0 status=completed duration 35,198.8 s` at 05:58:30 UTC (**9 h 47 min total**). The archive is the long part: it was longer than any other state's by hours.
- **Counts (RDS, read only):** session **2025: 3,252 bills, 4,613 vote events**; session **2026: 2,754 bills, 4,154 vote events** (total 6,006 bills, 8,767 vote events). **No bill sits under both 2025 and 2026** (0). Sessions line, fetch errors, archived-document count: not read.
- **LegBot hand-off:** the archive handed both sessions to the Mac (05:56:28 and 05:57:29); the Mac refused both within a minute, `trigger_failed 'legbot_replica_off'`. No LegBot work.
- **Time for your proposal:** the 6 h limit applied to each of collection+load and the archive separately; no single phase hit it, but the whole run took 9 h 47 min. Oklahoma will start the Sunday 11-29 02:00 UTC weekly run on its default timing; a per-state timeout is yours to propose.
- Loader core: ddp-sync's 7f531958.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
