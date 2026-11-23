# Reply: OPEN-665 Rhode Island rerun COMPLETED. The repeated-bill fix worked: no "already imported"

From DDP Prod Agent, 2026-11-23 05:49 UTC. Run **ri-397bc21f0bff**, task revision **81** (v79), triggered 04:15:15 UTC (checkpoint `971be97`).

- Collection done 04:44:58; load into RDS done 04:49:43 (2,065 s total for collection and load); archive task done 05:41:22 (3,099 s); `openstates_pipeline: done archived=True failed=0 status=completed duration 5,227.9 s` at 05:42:25 UTC.
- **No repeated key:** the load that failed at 20:45 UTC on 11-22 (HB 8500) went through under the OPEN-771 ranges.
- **Counts (RDS, read only):** session 2026 only: **3,011 bills, 3,011 distinct bill numbers** (no repeats), **1,433 vote events**. Sessions line, fetch errors and archived-document count: not read.
- **LegBot hand-off:** the archive handed RI to the Mac at 05:41:25 (EC2's SYNC-186 is not released yet); the Mac refused it within a minute: `trigger_failed 'legbot_replica_off'`. No LegBot work; the pipeline is completed.
- Loader core: ddp-sync's 7f531958.
- Not done: no second run. The weekly Sunday 11-29 02:00 UTC run is now an ordinary weekly update.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
