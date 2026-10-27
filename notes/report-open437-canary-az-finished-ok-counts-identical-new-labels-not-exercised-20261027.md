# Report: OPEN-437 canary. The Arizona scrape finished OK; counts identical to the baseline; the new vote labels were NOT exercised (no vote events in this run)

Prod agent, Tue 2026-10-27 about 23:58 UTC. Read-only after the launch. Task `d5fd97ea9b824175835175ab750a550e`, run `az-0f32cd69beb3`, launched 22:27:59, on `ddp-scrapers:41` / `v38` per the Mac's note 5fd0163. No stop rule fired. I did not retry, roll back or pause anything.

## Result
* ddp-sync: `collection done, loading into RDS` 23:51:45; **`cloud_scrape: done duration_seconds=5089.0`** at 23:52:50 (so the RDS load took about 65 s).
* Scraper final line: `{"source": "az", "run_id": "az-0f32cd69beb3", "mode": "incremental", "status": "ok", "found": 899, "duration_s": 4902}`. The two earlier incremental runs (az-e9850a3f0902, az-702ab787b606) were 4904 s and 4906 s, also `found: 899`: same duration, same count.
* CloudWatch stream, whole run: **0** ERROR/Traceback/Exception, **0** timeouts or connection errors, **0** blocked/captcha/WAF, **1** WARNING (the normal "no session provided, using active sessions: 57th-2nd-regular"), 1,827 `Unclassified action: None` INFO lines. Task exit code: I cannot read it from here (ECS describe is denied); the Mac's watcher has it.
* Working-tier run `az-0f32cd69beb3`: **900 objects, 2,224,002 bytes** (895 bill, 1 jurisdiction, 3 organization): the same object count and the same byte total as the 10-25 run `az-e9850a3f0902`.

## Arizona counts after the run (RDS, 23:53:52 UTC) against the baseline
* bills **2190** (baseline 2190); vote events **3443** (3443); person votes **75720** (75720); duplicate groups **249** (249). No drop. No bill was touched in the last 3 hours, and the latest vote event was written on 2026-06-22: this load changed nothing.
* Person votes by option: yes 50,292; no 19,114; **other 6,314; `not voting` 0** (the new label is not in the database).

## What the canary did NOT test (please read)
* **The scrape produced no vote events at all.** The run has 895 bill objects and zero `vote_event` objects (the previous run also had zero), because an incremental run only takes votes newer than its start mark (`start: 2026-10-25T02:22:22`) and nothing new was found. So #71 (the `not voting` label) and the excused count were **not exercised**, and `not voting` vs `other` cannot say anything yet: 0 vs 6,314 is expected from the code never running on a vote. A real test of them needs a full or dated rescrape of Arizona votes, which was not authorised here.
* **#79 (undated Engrossed version names):** the version lists (note and date) of all 895 bills are identical between this run and the 10-25 run, and the count of versions with an empty date is **896 in both**. If #79 was meant to change those, it did not show here. I do not know what exactly it changes, so I can only say the output is unchanged.
* So the canary shows the new image **starts, scrapes, loads and finishes cleanly with the same counts and timing**; it does not show the three fixes working.

## Not yet checked
The Arizona bill-document archive is weekly (11-03 05:00), so nothing followed the scrape; no LegBot or broker write came from it (no non-local POSTs in the broker log). The 00:30 UTC cadence review has not happened yet; I will say after it whether it set a cadence key for `az`.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
