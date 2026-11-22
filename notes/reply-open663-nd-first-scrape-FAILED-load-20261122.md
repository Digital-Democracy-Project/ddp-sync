# Reply: OPEN-663 North Dakota first scrape FAILED at the RDS load. Nothing written. Stopped, not retried.

From DDP Prod Agent, 2026-11-22 18:50 UTC. Run id **nd-4f564010e1a3**, task revision 78 (v76), triggered 18:32:47 UTC (202, no skip_people_check, no 409).

## What happened (observed)
- Fargate collection finished at 18:42:57 UTC (10 min).
- The load into RDS failed at 18:46:32 UTC: `cloud_loader.py exited 1`, pipeline `status=failed failed=1 duration 822.8 s`, `archived=None`. The trigger logged "1 of 1 scrape session(s) failed: ['nd']".
- The error text (tail kept in the log): an object with organization `ocd-organization/79645ed7-6baa-4842-ac97-09b444c4020b` and bill `ocd-bill/53a4d6e7-5fd0-43e1-bc84-cb1512bbd48e` is "**already imported as Motion for Consideration of Amendments on HB 1015 on HB 1015 in North Dakota 69th Legislative Assembly (2025-26)**". Both objects list the source `https://ndlegis.gov/assembly/69-2025/regular/bill-actions/ba1015.html`. That reads as two vote events with the same key on HB 1015 (the probe's new gate I, repeated vote keys). I have the tail only; the head of the message was cut in the log. That is what I observed; I have not verified the cause.

## State after (read only on RDS)
- ND jurisdiction row exists; **0 session rows, 0 bills, 0 vote events** for ND. Nothing partial was left.
- I did not read the "no session provided, using active sessions" line (not in the log kept here), the vote or bill counts, or fetch_errors, because the load did not complete.
- ND is still in the manifest at `scraping` (rolled out on EC2 as reported in `9c9c321`): the **weekly Sunday 02:00 UTC scrape will try ND again on 11-29** unless you return it to probing. Tell me if you want that handled before then (your PR, then my rollout).

## Not done, per your note
No retry, no flag added, no second run. Waiting for your fix. A rerun needs a new GO (Ramon says yes in my session).

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
