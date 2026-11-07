# Reply (read-only): watermarks for pa/ne/la, last Sunday's batch, and the v47 cut-over line (OPEN-500, OPEN-494). 2026-11-07, about 04:45 UTC

Reply to 2f87ce4 (items 1-3) and ecef62e (items 1 and 3). Nothing was changed: no retry, relaunch, pause, pull, restart or registration.

## 2f87ce4 item 1: not touching Pennsylvania
Understood: the 04:00 UTC job stays as it is. I will not retry, relaunch or pause anything for it. (No alert preference from me; Ramon decides whether the failure alerts are a nuisance.)

## 2f87ce4 item 2: the last-run watermarks (S3 scraper memory, `s3://ddp-openstates-scraper-memory/prod/<st>/<st>/<st>.ts`, read-only)
| state | `.ts` value | `.count` | object last modified (UTC) |
|---|---|---|---|
| pa | **2026-11-03T05:12:48** | 11132:full | 2026-11-03 05:19:35 |
| ne | **2026-11-02T23:26:28** | 3621:full | 2026-11-02 23:28:40 |
| la | **2026-11-03T02:13:32** | 5792:full | 2026-11-03 02:17:11 |
- All three exist and are from their first full scrapes, as you expected. **The failed 04:00 run did not move Pennsylvania's** (last modified 11-03 05:19:35, nothing since). The 11-07 failure log shows `bills: {'start': '2026-11-03T04:12:48'}`, which is exactly the `.ts` minus one hour, so it matches your description of `run-scrape.sh`. Because all three have a marker, I expect Sunday's Nebraska and Louisiana to be incremental runs too (that is what I read from the markers and your description; I have not seen the scrape harness run them).

## 2f87ce4 item 3: last Sunday's batch (11-01 02:00 UTC), from the task logs in `/aws/ecs/ddp-scrapers`
I read each stream's own final status line and counted `unexpected keyword argument` lines. **No `unexpected keyword argument` line in any stream from 11-01 01:50 to 16:00 UTC (20 streams).**
| jurisdiction | stream start | length | final status line |
|---|---|---|---|
| mi | 02:00 | 1 min | `incremental`, `ok` |
| fl | 02:00 | 3 min | `full`, `ok` |
| va | 02:00 | 20 min | `incremental`, `ok` |
| ut | 02:00 | 34 min | `incremental`, `ok` |
| nc | 02:00 | 54 min | `incremental`, `ok` |
| wa (02:30), usa (03:00 and 04:19) | | | `incremental`, `ok` each |
| az | 02:00 | 82 min | **no final status line found** |
| ma | 02:00 | 720 min (12 h) | **no final status line found** |
- az and ma: I found no `ok` or `failed` line in their logs and no `scrape failed, exit` line. For ma, 12 h is the cap OPEN-476 recorded for that run (it was cut off and nothing loaded; MA was raised to 24 h afterwards). For az I do not know why there is no status line; I did not look further. The marker times in S3 show az written 11-01 03:23 UTC, ma 11-02 13:27 UTC.
- The other streams in the window (marked unknown, 0 to 11 min) are not scrapes I could name (probably archive/load tasks); none contained the error.
- ne and la were not in that batch (their first full scrapes were 11-02 and 11-03), so they have no earlier Sunday result.

## ecef62e item 1: the cut-over line (v47 / revision 49, registered about 04:24 UTC)
- At 04:27:20 UTC one task was running: `22920027f06a4e29a93c7dd770e9a4fa`, created **04:22:02 UTC**, **task definition `ddp-scrapers:48`, image `v46`**. By the ddp-sync log it is the **USA scrape's second Fargate collection** (`jurisdiction=usa run_id=usa-eb899b515279`, triggered 04:19:21 after the USA archive finished 04:19:20); I matched it by timing, not by an id in the task itself. So it launched before revision 49 existed and runs 48 / v46.
- I will read the revision with `describe-tasks` from the next launches: the Sunday 11-08 02:00 UTC batch (Nebraska, Louisiana) and Pennsylvania's 11-08 04:00 UTC launch, and tomorrow's Washington 02:30 and USA 03:00. I **cannot** confirm revision 49 is the latest ACTIVE (DescribeTaskDefinition is denied), so I only report what each launched task ran.

## ecef62e items 2 and 3
First-run checks (a) to (g) will be in my end reports for Nebraska, Louisiana and Pennsylvania, including the "no `no objects returned from`, no 'no changes since cutoff (no-op)'" check; if I see a no-op summary I stop and tell you rather than accept it. Item 3 is answered above (watermarks, whether the failed run moved Pennsylvania's, last Sunday's results).

## Not done
No retry, relaunch, schedule or config change, pull, restart, registration, recompute or LegBot run.
