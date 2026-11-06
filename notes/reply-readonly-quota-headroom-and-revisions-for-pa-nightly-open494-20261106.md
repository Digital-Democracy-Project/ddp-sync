# Reply (read-only): room for a fourth nightly scrape, from the EC2 host (OPEN-494, OPEN-478). 2026-11-06, read at about 19:55 UTC

Reply to f13b45b. Nothing was changed: no pull, restart, scrape, schedule or config edit. Numbers below are what I read, with the source. Where this host's read-only AWS role cannot read something I say so.

## 1. Cloud tasks
- **Quota:** Fargate On-Demand vCPU quota in us-east-1 is **4,000** (Service Quotas, quota L-3032A538). The listing call is denied to this role; I read that one quota by its code.
- **Real use, last 7 days (CloudWatch AWS/Usage, Fargate vCPU, hourly max):** week peak **32 vCPU**; in the 02:00-11:00 UTC window peak **7 vCPU**, median **1 vCPU** (70 hourly readings). So a fourth long task is far below the quota.
- **Concurrent tasks (from the /aws/ecs/ddp-scrapers log streams, first-to-last event of each of 129 streams):** peak overlap in the 7 days **24** (10-31 18:41 UTC, a one-off burst of 65 starts that day). Since 11-02, the normal nights: peak overlap **4** (11-04 05:00 UTC, archive tasks). Method caveat: a stream's first-to-last event is a good stand-in for task life, not an exact one. I could not list running tasks (list-tasks answers none now) or read the task size (DescribeTaskDefinition is denied), so I did not convert vCPU into a task count.

## 2. Database (RDS ddp-openstates, last 7 days, CloudWatch, window 02:00-11:00 UTC)
- **Class:** db.t4g.large, PostgreSQL 16.15, gp3 50 GB, 3,000 provisioned IOPS.
- **CPU:** average peak 7.7%, typical hourly average 5.6%; hourly **maximum** peak 61.6% (typical hourly max 8.2%). That 61.6% spike is in the window; I did not trace which job caused it.
- **Connections:** peak 14, typical 9 (max hourly).
- **Write load:** write IOPS peak 132.6 in the window (291 elsewhere in the week), typical 10.6; write throughput peak about 13 MB/s, typical about 1.1 MB/s. Disk queue depth peak 1.3. Freeable memory never below about 4.57 GB.
- My read: nothing near a limit, one CPU spike to watch. A fourth import adds writes, so judge it on the spike, not the averages. I did not test or simulate it.

## 3. Image / revision
- **Not readable here:** ecs:DescribeTaskDefinition, ListTaskDefinitions and ecr:DescribeImages are denied to this role, so I cannot read the latest ACTIVE revision or v44's registration.
- Last revision I saw actually used: **v43 / revision 46** (Nebraska re-archive, finished 11-06 03:17 UTC). The newest log stream in /aws/ecs/ddp-scrapers has its last event at 11-06 05:02 UTC, after that run; I cannot tell its revision from the stream. I have no evidence of a revision newer than 46, and no evidence against it.

## 4. Checkouts and times (this EC2 host only; I cannot see the Mac)
- **ddp-sync** `28e8e0b` (merge of #266, committed 11-03 19:45 host time). `28e8e0b` is HEAD, so #267 is **not** pulled.
- **ddp-open-states** `05515505` (merge of #325). #343 is **not** pulled.
- **ddp-sync restart:** container `ddp-sync-ddp-sync-1` started **11-04 01:21:08 UTC**, after `28e8e0b` was checked out, so it has restarted since `28e8e0b`. It is Up and healthy.
- **Next ddp-sync job on this host:** Washington 02:30 UTC on 11-07 (daily schedule in config/sync_schedule.yaml, which I read; I did not read the scheduler's own next-run line for it). Jobs whose next-run line I did read in the log: jurisdiction drift check 11-07 06:20 UTC; Massachusetts archive 11-13 05:00 UTC.
- Mac: not visible from here.

## Not done
Nothing was pulled, restarted, registered or run. When you send the OPEN-494 pull and restart request I will answer it on its own go.
