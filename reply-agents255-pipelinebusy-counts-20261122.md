# Reply: AGENTS-255 (read only): EC2 PipelineBusy counts and LegBot hand-off waits

From DDP Prod Agent, 2026-11-22 16:52 UTC. Source: EC2 ddp-sync container logs, 19 saved archives plus the live log, continuous from 11-02 to now. Nothing was changed.

## 1. PipelineBusy (11-08 to now)

Exactly **1**.

| When (UTC) | State | Refused caller | Lock holder |
|---|---|---|---|
| 11-22 03:00:00 | usa | the nightly schedule (scheduler._pipeline_wrapper) | the US full re-import (POST openstates-scrape/usa?full=true, sessions 118/119 lower+upper, 202 at 11-21 22:29:07 UTC) |

The holder's three scrape runs finished 04:38, 09:37 and 11:56 UTC; the lock has **not** been released since. It passes to the queued US 119 LegBot hand-off (accepted 12:14:15 UTC, see below), which still holds it now.

No PipelineBusy for any trigger route, re-import or retry. The only other refusals in the window were two HTTP 409s on POST openstates-archive/wa (11-20 04:26:53 and 05:31:47 UTC) that are not PipelineBusy. Before 11-22 the logs hold zero PipelineBusy lines.

## 2. archiver_triggered_legbot_wireguard_accepted (15 since 11-16; none earlier in the saved logs)

Wait = accepted to the next result or trigger_failed line for the same state and session. A 1.0 means the Mac answered inside the first one-minute poll.

| Accepted (UTC) | State/session | Wait (min) | Outcome |
|---|---|---|---|
| 11-16 19:41 | al 2026rs | 1.0 | success |
| 11-17 03:39 | nv 2025Special36 | 1.0 | success |
| 11-17 03:59 | ga 2026_ss | 1.0 | success |
| 11-17 04:02 | or 2026R1 | 1.0 | success |
| 11-17 11:11 | pa 2025-2026 | 72.1 | success |
| 11-18 03:22 | us 119 | 74.1 | trigger_failed (Mac: "no heartbeat: ddp-sync restarted during the run") |
| 11-18 13:15 | nc 2025 | 22.0 | success (2,342 bills) |
| 11-18 15:33 | nm 2026 | 1.0 | success |
| 11-19 03:22 | us 119 | 166.2 | success (20,004 bills) |
| 11-19 11:11 | pa 2025-2026 | 43.0 | success |
| 11-20 05:53 | us 119 | no end line | the EC2 ddp-sync restarted at 05:53:45 (SYNC-181 rebuild), so the wait was cut short; not a limit |
| 11-20 18:39 | us 119 | 1.0 | quiet refusal: already_running (another Mac run held the lock) |
| 11-21 03:23 | us 119 | 1.0 | quiet refusal: already_running (same Mac run id) |
| 11-21 12:28 | nv 84 | 1.0 | success (190 bills) |
| 11-22 12:14 | us 119 | still open | waiting now; EC2's own limit should end it near 18:15 UTC |

Counts: 10 success, 2 quiet refusals, 1 trigger_failed, 1 cut by a restart, 1 still open. **None ended on the 6 h limit so far.**

## 3. Nightlies with no run on a due day

Cadence: pa 04:00 UTC from before 11-07; wa 02:30 and usa 03:00 UTC from 11-11 (first saved start lines).

- **wa, pa: none missed** 11-11 to 11-22.
- **usa: 1 missed**, 11-22 03:00 (the PipelineBusy above).
- 11-08 to 11-10: only pa was scheduled; it ran each day (apscheduler "Running job" at 04:00).

## Caveats

The pipeline holder on 11-22 is read from the trigger's 202 and the starting line at 22:29:07 on 11-21; the log does not print the lock key's owner. Quiet-refusal and success classification come from the result line text. Seconds-long gaps at each container recreate are possible.
