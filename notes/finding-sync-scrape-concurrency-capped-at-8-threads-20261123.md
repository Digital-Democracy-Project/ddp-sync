# FINDING (2026-11-23 17:29 UTC): EC2 ddp-sync runs at most 8 cloud scrapes at once; the rest of a burst waits with no task. Nightlies could be starved. (For the ddp-sync dev; nothing changed by me)

**Observed:** after Ramon's go I triggered 11 runs at 17:08-17:12 UTC while 5 others were running (az, fl, la, ma, ut). Only mt, ia and md got a Fargate task (8 running in all); **hi, me, ms, mo, nm, id, ri and ok** have `openstates_pipeline: starting`, a held `ddp:pipeline_lock:<st>`, `cloud_scrape: ... triggering Fargate collection` logged by the scrape wrapper, but **no run id and no task** after 20 minutes. No error line.

**Evidence for the cause (read from code and the container, nothing changed):**
- `pipelines/openstates_scrape.py` waits for each scrape in `await asyncio.to_thread(...)` (lines ~1220, 1338, 1631: the blocking launch-and-wait runs in a worker thread).
- ddp-sync does not set a default executor (no `set_default_executor`, no `ThreadPoolExecutor` in app.py, openstates_scrape.py or cloud_scrape_trigger.py). In the container `os.cpu_count()` is **4**, so Python's default executor has **min(32, 4+4) = 8** workers.
- 8 long-running scrapes = 8 busy threads; the ninth waits for a thread. That matches exactly: az, fl, ia, la, ma, md, mt, ut hold the 8.

**Why it matters:** the queue is a hidden FIFO. **A scheduled nightly (WA 02:30, USA 03:00, PA 04:00 UTC) or a state's weekly run could sit in the queue behind the long runs** (Florida's 2025/2026 re-import alone can hold a thread for about 25 hours; Michigan's full run held one for 6 h 49 min; Oklahoma's first run for 9 h 47 min). Also `asyncio.to_thread` is used for alerts, so alerts can wait too. I have not seen a nightly starved yet; tonight's 02:30 UTC WA nightly is the first at risk while the queue is long.

**What I did not do:** no restart (it would drop archive steps and the queued runs), no re-trigger (a re-trigger would double-run when the first slot frees). The eight will start as threads free (mt, ia, md in roughly 30-60 min).

**Suggestion (yours to decide, a code change):** give cloud scrapes their own larger executor or an asyncio semaphore sized to a deliberate number (32), so a burst is an explicit queue with log lines, not an invisible one.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
