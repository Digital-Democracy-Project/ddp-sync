# Reply: Pennsylvania's first nightly run (04:00 UTC 11-07) FAILED after 93 seconds; stopped, not retried (OPEN-494, OPEN-478). 2026-11-07, 04:05 UTC

Reply to 3a79efc, item 1 (launch report), and it is also the end report: the run is over. I took no action beyond reading.

## Launch
- ddp-sync job `openstates_primary_scrape_pa` ran at **04:00:00 UTC** as scheduled (`openstates_secondary_scrapes: starting jurisdictions=['pa']`; people count read 257 from RDS). Fargate collection triggered 04:00:01, `run_id=pa-3704a9e5fd83`.
- Task `a903ce04837f4903aa61bb9a268f1be2`, created 04:00:02 UTC, stopped 04:01:04: **task definition `ddp-scrapers:48`, image `ddp-scrapers:v46`**, digest `sha256:faa680f802aff3256676b41a23d9f8cfff41bd319ee48aed2ad092a494530395` (read with `describe-tasks`). So Pennsylvania ran the new revision, as Ramon expected.

## What failed (read from the task's CloudWatch log; nothing inferred)
- Exit code **1**; ddp-sync logged `cloud_scrape: collection failed detail='Essential container in task exited' exit_code=1` at 04:01:32 and `completed ... failed=1`; duration 92.8 s (the container itself ran about 63 s, only about 2 s of it scraping).
- The scraper started in incremental mode with `bills: {'start': '2026-11-03T04:12:48'}`, then died at 04:00:40 with:
  `TypeError: PABillScraper.scrape() got an unexpected keyword argument 'start'` (from `openstates/scrape/base.py` line 346, `self.scrape(**kwargs)`). The container printed `ERROR: pa scrape failed, exit 1` and a result line `"mode": "incremental", "status": "failed", "duration_s": 2`.
- What I can say: the nightly run passes a `start` date to the Pennsylvania bill scraper in this image and that scraper's `scrape()` does not accept it. **I have not looked at why** (the scraper code, whether v43/v45 behaved the same, or what the 11-03 first full scrape did differently), so I am not naming a root cause beyond the traceback.

## What did not happen
Nothing was loaded: no bills, vote events or person votes were saved by this run, so there is nothing to compare against 5,270 / 5,858 / 425,560 and no 429, 403 or block (the scraper never reached the site beyond a first request that raised an insecure-request warning). No RDS load ran, so no RDS CPU or connections to report from this run. The other scraper tasks launched tonight on the same image were fine: both USA tasks (03:00 and 03:05, rev 48 / v46) exited 0; Washington (02:30, rev 47 / v45) was fine earlier.

## What I did and did not do
Stopped, as your plan says: **no retry**, no relaunch, no config or schedule change, no pull, no restart, no registration. No ECS task is running now. The watch after 3 hours is moot.

## Heads-up on what happens next (my reading, not verified)
The job is scheduled daily at 04:00 UTC, so **the next run is 2026-11-08 04:00 UTC** and, unless the cause is fixed or the cadence changes first, I expect it to hit the same error. I will not change the schedule or the image myself; tell me if you want it paused (a config edit and a ddp-sync restart, on the user's go) or fixed by a new image.
