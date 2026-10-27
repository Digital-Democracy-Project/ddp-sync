# Interim: OPEN-437 canary. The Arizona task's CloudWatch log shows no problem so far; still running

Prod agent, Tue 2026-10-27 23:30 UTC (task started 22:28:42; revision `ddp-scrapers:41` / `v38` per the Mac's note 5fd0163). Read-only: I only read the task's log stream (`scraper/scraper/d5fd97ea9b824175835175ab750a550e`). The in-flight record still shows the run (count 1). Nothing else is running.

## What the log shows at 23:29 UTC (about 61 minutes in)
* **0** lines with ERROR, Traceback or Exception.
* **0** timeouts, connection errors or "Max retries".
* **0** hits for blocked, captcha or WAF.
* **1** WARNING in total: `no session provided, using active sessions: {'57th-2nd-regular'}` (normal for an incremental run).
* Progress: **1,681** requests to `apps.azleg.gov/api/Bill/` so far, steady and still moving; the last event was seconds before the check. The previous incremental run (`az-e9850a3f0902`, 10-25) took 4,904 s (about 82 min) and `az-702ab787b606` took 4,906 s, both `found: 899`, so I expect this task to end near 23:50 UTC.
* Harmless noise: a Python 3.10 FutureWarning from a Google library at start-up, an "Unverified HTTPS request" warning on each `apps.azleg.gov` call, and many `Unclassified action: None` INFO lines (1,297 so far). I have no earlier Arizona-only count of that last line to compare with (the log group is shared by all states, so a time-window count includes other states).
* A search for 403/429/Forbidden matched 11 lines; they are ordinary successful GETs whose ids contain those digits, not errors.

## What this does not tell
It says nothing yet about the behaviour changes (#71, #78, #79: the `not voting` label, the excused count, undated Engrossed version names); that comes from the counts after the run. The stream may also show errors only at its end. I will read it again when the task stops and post the full report (task result, scrape summary line, archive step, Arizona counts against the baseline of bills 2190, vote events 3443, person votes 75720, duplicate groups 249, `not voting` vs `other`).

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
