# Checkpoint (2026-11-23 18:18 UTC): ddp-sync 85c8888 (SYNC-187): G1 passes, G2 in-flight list BEFORE the restart

Ramon, in my session (pop-up, 11-23): **"release as soon as the Montana archive task finishes."** Montana's and Arizona's archives are done in Fargate (CloudWatch: `mt-archive-a11a06b29501 status ok, archived=294`, `az-archive-92350bb4bc22 status ok, archived=251`); no archive task is running now. I treat the repairs you list as covered by what Ramon already approved: manual archives (overnight pre-approval), the queued runs he GO'd at 17:08, Florida 2026 (pre-approved). Notes re-fetched at 18:17; only your `692a2ba` is new.

## G1
`/opt/ddp-sync` at `b569cf2`, `git status` clean; `git log --first-parent HEAD..origin/main` lists only **`85c8888` (#305)**.

## G2: every state in flight (ECS tasks, held locks, queued), and its step
Held `ddp:pipeline_lock:<st>`: az, fl, hi, ia, id, la, ma, md, me, mi, mo, ms, mt, nm, ok, ri, ut.
| State | Step |
|---|---|
| **fl** | **collecting 2025** (`ddp-scrapers:83`, task since 14:43); 2026 not started (multi-session, would not be launched) |
| **la** | collecting 2026 (rev 84, since 16:49) |
| **ut** | collecting 2026 (rev 83, since 14:18) |
| **ma** | collecting (weekly, rev 83, since 12:01) |
| **md, hi, me, ms** | collecting (md rev 84; hi, me, ms rev 85) |
| **mo, nm, id, ri, ok** | **queued, no task** (ok = `2025,2026`, full) |
| **ia** | session 2025 **collection failed** at 17:55:31 (`ReadTimeoutError www.legis.iowa.gov, read timeout 60`, nothing retried); the run then moved to 2026 and is **queued, no task** (2026s1 not started) |
| **az, mt** | loaded (17:50:17, 17:53:24); Fargate archives finished ok; ddp-sync has not logged their archive "done", pipeline lock still held (wrap-up, LegBot-on az hand-off not seen) |
| **mi** | LegBot hand-off finished (success 17:15), search refresh done 17:16; lock still held |
| wy, ne | not in flight |

## What I will do after the restart (only this, on Ramon's earlier approvals)
Re-trigger: mo, hi? no (hi, me, ms, md are collecting and **resume**); queued **mo, nm (full 2026), id (full 2026), ri (full 2026), ok (full 2025,2026)**; **ia 2026 and 2026s1** (`?sessions=2026,2026s1`, Iowa 2025 failed on its own: not retried without a go); **Florida 2026** (`full=true&sessions=2026`) after its 2025 loads; manual **archives** for each resumed run when it loads; Nebraska 109 (pre-approved) on rev 85. Locks still held with no task: I report key and TTL and delete nothing.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
