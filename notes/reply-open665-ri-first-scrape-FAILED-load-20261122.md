# Reply: OPEN-665 Rhode Island first scrape FAILED at the RDS load (a repeated BILL, HB 8500). Nothing written. Stopped, not retried.

From DDP Prod Agent, 2026-11-22 20:49 UTC. Run id **ri-8151f1638a5a**, task revision 80, triggered 20:11:47 UTC (202, people read 116, no 409).

## What happened (observed)
- Fargate collection finished 20:41:45 UTC (about 30 min).
- Load failed 20:45:29 UTC: `cloud_loader.py exited 1`; pipeline `status=failed failed=1 duration 2019.5 s archived=None`.
- The error text (tail kept in the log): a bill object (title ending "...OFFICE OF HEALTH AND HUMAN SERVICES", classification bill, session `a429e188-a0e2-41c5-ae5b-cc13ecd4b0d6`) is "**already imported as HB 8500 in Rhode Island 2026 Regular Session**". Both objects list the source `https://status.rilegislature.gov/`. This is a repeated **bill** key, not a repeated vote key as your note expected. The head of the message was cut in the log; I have not verified the cause.

## State after (read only on RDS)
- RI: **0 bills and 0 vote events** in any session. Nothing partial was left.
- I did not read the sessions line, vote counts or fetch_errors: the load did not complete.

## Schedule
RI is at `scraping` on EC2, so the **weekly Sunday 02:00 UTC scrape on 11-29 will try RI again** unless you change that (your PR to probing, then my rollout).

## Not done
No retry, no flag, no second run. A rerun needs a new GO from Ramon in my session.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
