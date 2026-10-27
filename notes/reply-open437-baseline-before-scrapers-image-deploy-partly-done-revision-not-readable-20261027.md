# Reply: OPEN-437 baseline before the scrapers image deploy. Read-only; items 2, 3, 4 (own queries), 6 (partly) done; item 1 NOT readable from this host; item 5 waits for the 10-28 jobs

Prod agent, Tue 2026-10-27 22:15 UTC. The operator said "Option A, run the baseline now" in my session. Read-only: no deploy, no scrape, no config change. Outputs saved in `/home/bitnami/ops/open437-baseline-20261027T2209Z/` (counts.txt, fl_and_s3.txt, s3_counts_inflight.txt, schedule.txt and the two scripts).

## 1. Live revision: cannot read it from this host
This role is denied `ecs:ListClusters`, `ecs:DescribeTasks`, `ecs:ListTaskDefinitions/DescribeTaskDefinition`, `ecr:DescribeImages` and `ecr:DescribeRepositories`, and ddp-sync's logs never record the revision (it launches the bare family `ddp-scrapers`, so the latest ACTIVE revision is used). The last revision I saw a task run on is **`ddp-scrapers:39`** (NC, 10-19); I have no newer observation. To get it, run `aws ecs describe-task-definition --task-definition ddp-scrapers` from a role that can, or grant these read actions to this host's role.

## 2. Per-state counts in RDS (db time 22:10:52 UTC)
| State | bills | vote events | person votes |
|---|---|---|---|
| Virginia | 4396 | 11553 | 452159 |
| Arizona | 2190 | 3443 | 75720 |
| Washington | 3411 | 2302 | 171556 |
| Florida | 7685 | 10228 | 324838 |
| Michigan | 4159 | 1279 | 96376 |
| USA | 39084 | 1604 | 561033 |
| Utah | 1021 | 1917 | 93865 |
| Massachusetts | 11840 | 529 | 46448 |
| North Carolina | 2343 | 781 | 72680 |
Virginia vote events whose motion_classification contains `passage`: **4165** (2026: 4136; 2026S1: 29).

## 3. Newest working-tier run per state (by newest object; objects, bytes)
va va-14bad9d7e58c 10-25 02:20 (13, 27933); az az-e9850a3f0902 10-25 03:22 (900, 2224002); wa wa-05375263aed4 10-27 03:28 (1, 60: manifest only); fl fl-68e84cc45b3d 10-26 01:29 (1, 60: manifest only); mi mi-b98df4482eaf 10-25 02:01 (13, 30396); usa usa-57407d77bbec 10-27 03:16 (21, 116879); ut ut-7902ffd91496 10-25 02:37 (5, 10659); ma ma-0c34a47aae70 10-25 12:21 (12261, 43369992); nc nc-c1e6ba36f200 10-25 02:55 (3129, 19155397). Prefix counts: va 10, az 10, wa 54, fl 39, mi 9, usa 115, ut 15, ma 9, nc 4. (WA and FL newest runs are 1-object manifests, i.e. empty or no-change runs; "newest" is by last write, not by run start.)

## 4. Checks (my own queries; I do not have one canonical check script, so these are my versions, read-only)
* Duplicate vote events (same bill + motion text + date, count > 1), groups per state: Arizona 249, Massachusetts 32, Virginia 290, Washington 1 (others none).
* Virginia vote events with motion text starting `H VOTE:`: **2188**.
* Florida committee votes (organization named House, classification committee) with no person votes: **0**.
* Bills/votes drop: no earlier same-moment baseline to compare, so this is the baseline itself. Known earlier figures still match (WA bills 3411, NC 2025 bills 2338 in the broker).

## 5. Broker confirmations: pending
The first real ddp-sync write after 269cdb29 and the 03:15 UTC session cleanup need the 10-28 02:30/03:00/03:15 UTC jobs; the broker was also moved to 63d1a132 tonight (19:56 UTC). I will report both after 03:15 UTC.

## 6. In flight and next launches
* In flight: **none**. ddp-sync's in-flight Fargate record (redis db3 `ddp:inflight_fargate_jobs`) is empty. I cannot list ECS tasks directly (denied).
* Next scheduled launches until Thu 00:00 UTC: the weekly scrape runs are Sundays in the config (FL 02:00, WA 02:30, USA 03:00, VA/MI/MA/UT/AZ/NC 02:00), so none is due before Thursday. The one daily item that can start scrapes is the **dynamic-cadence review at 00:30 UTC**; the only one before Thursday 00:00 is **Wed 10-28 00:30**. Any state it has escalated to nightly would launch after it. I did not read which states are escalated now (the config only gives the rule; the escalation state is not in the file); tell me if you want that checked. I did not read the live job list of this host's ddp-sync scheduler, so this is from the config file, not from the running scheduler.
* Not a scrape: people_refresh Tue az, Wed wa and va.

Not done: no canary launch, no rescrape, no snapshot, nothing written anywhere.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
