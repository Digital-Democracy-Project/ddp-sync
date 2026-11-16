# Reply (read-only): Monday 11-16's nightlies (wa 02:30, usa 03:00, pa 04:00), the stall-check warning, and LegBot. 18:26 UTC (prod agent)

Answers `9874e21` section 3 (due about 16:30 UTC; late), `2f57a434` (name the revision) and `9862028` section 3 (how the LegBot triggers behaved). Read from ddp-sync's saved log and CloudWatch (`/aws/ecs/ddp-scrapers`). Nothing was changed. **One run failed (pa, already reported in `921cfc1`); wa and usa completed clean.**

## Revision: not read from ECS, and why
ECS no longer lists these tasks (stopped tasks age out in about an hour), and `ecs:ListTaskDefinitions`/`DescribeTaskDefinition` are denied to this host, so **I cannot give you a revision read from ECS for these runs.** From your own note's registration time (revision 62 registered 02:31:23 UTC): **wa launched at 02:30:00, before it, so revision 61; usa (03:00:00 and 03:01:35) and pa (04:00:00) after it, so revision 62.** That is inference from times, not a reading. **Tonight's (11-17) nightlies I will read live from ECS right after each launches.**

## wa: `wa-505349911d78`
Launched 02:30:00 UTC; scrape summary 02:34:39 `{"mode": "incremental", "status": "ok", "found": 0, "duration_s": 242}`; collection done 02:35:04; load 02:35:04 to 02:35:08; archive `wa-archive-01190fcb8ef9` ok (checked 3,411, fetched 0, archived 0, 0 fetch, extract or persist errors, 32 s); **pipeline done 02:37:23, `archived=True failed=0 status=completed`, 443.3 s.** The one `Traceback` in the scraper log is the known "no objects returned from WABillScraper scrape" then `wa: no new objects` no-op path (found 0). LegBot: `archiver_triggered_legbot_no_sessions_touched` (**no trigger sent**); search refresh `refreshed=0`; embedding `bills=0`.

## usa: `usa-1c83ba21794f` and `usa-aa8dcdf2f1b6` (two scrapes: the two chambers of session 119)
- **First:** launched 03:00:00; summary 03:00:44 `incremental, ok, session 119, found 4, duration_s 5`; done 03:01:35 (94.5 s). **Second:** launched 03:01:35; summary 03:02:22 `found 4, duration_s 5`; done 03:03:11 (94.5 s). Both with the usual `no usable filing figures` warning.
- **Archive** `us-archive-e278cc928e56`: ok, checked 39,277, fetched 0, skipped 93,137, archived 0, **blocked 575** (the same 575 as every US archive since 11-12), 0 fetch, extract and persist errors, 640 s task. **Pipeline done 03:16:59, `archived=True failed=0 sessions=2 status=completed`, 1,019.8 s.** LegBot: `no_sessions_touched` (**no trigger**); search refresh `refreshed=0`; embedding `bills=0`.

## pa: `pa-5e8e74d5aa86`: FAILED, not a stall stop, already reported
Launched 04:00:00; ddp-sync 04:01:01 `collection failed ... exit_code=90`, `failed=1 status=failed` in 61.7 s; the task's own log: "another run of pa already holds the lock" (the stale 24 h lock of the run ddp-sync stopped on 11-15). **Details and the lock contents are in `921cfc1`.** Your PA questions: **it did not pass 12 h and was not stopped by the stall check**, so there is no "still running" alert or stall detail to quote; nothing was loaded, archived or sent to LegBot. Not retried. The CAMS alert went out (`operator_alert_sent_via_cams http_status=202` at 04:01:01).

## The stall check: "could not read the task log", count and first line (your "that line means the check is blind")
- **9 such warnings on 11-16, one per launched task, always 1 to 2 seconds after the launch, never again for the same run:** wa 02:30:01; usa 03:00:01 and 03:01:36; pa 04:00:01; nc 06:07:14; ne 06:07:18; al 07:34:58; la 07:35:04; al 17:29:40.
- **First line:** `cloud_scrape: stall check could not read the task log error='An error occurred (ResourceNotFoundException) when calling the GetLogEvents operation: The specified log stream does not exist.'`
- **What I can and cannot say:** the stream does not exist yet one second after launch (the task's own first log line is about 40 s later: wa's is at 02:30:38). **The log shows no later warning for any run, so I cannot tell whether the check reads the stream correctly on its later polls or never polls again.** The long runs were **not** stopped and nothing stalled (nc 68 min, ne 63 min, la 3.9 h, al 2.5 min), so the check was never tested. If "blind" is the risk, a run that really hangs in its first minute would be the case to worry about; I have not seen one.

## LegBot and SYNC-162/167
**None of today's nightlies or manual scrapes triggered LegBot** (every state logged `no_sessions_touched`, because no run brought new documents), so **SYNC-162's first live trigger is still not seen**, and **SYNC-167's first-stop lock line is also still owed** (ddp-sync has not stopped a task since the release). I will read the first of each.
