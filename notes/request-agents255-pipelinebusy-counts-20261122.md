# REQUEST (read only, no GO needed): AGENTS-255, how often EC2's state pipeline lock waited on the Mac's LegBot

**From:** the ddp-agents session working AGENTS-255 (Ramon asked for it on 2026-11-22: measure first, change nothing until he approves a fix).
**What I need:** counts from EC2's ddp-sync log only. Nothing to run, restart or change.

For 2026-11-08 to now (UTC), from EC2's ddp-sync log:

1. **`PipelineBusy`:** every line, with the state, the time, and who was refused (the nightly schedule, `/trigger/openstates-scrape`, `/trigger/openstates-archive`, a re-import, a retry). For each one, if you can tell: which run held the lock, and when that run released it.
2. **The LegBot hand-off waits:** every `archiver_triggered_legbot_wireguard_accepted` line, with the end of its wait. The end is the next `archiver_triggered_legbot_wireguard_result` or `archiver_triggered_legbot_wireguard_trigger_failed` line for that state and session. Give the minutes between them, and whether it ended in success, a quiet refusal or the 6 h limit.
3. **Nightlies that did not run:** any nightly (wa, usa, pa, or a state the cadence review raised) whose `openstates_scrape_<state>` record shows no run on a day it was due.

**What the Mac side already shows** (its own log, 11-15 to 11-22 EST), for context:
- The US 119 hand-off of 11-22 12:14 UTC (`legbot-session-4ef22e5c2e33`) has been polled by EC2 for 4.2 h. Its Mac run has not started one bill: it waits behind Virginia's run for the Mac's two shared LegBot slots.
- EC2's own limit should end that wait at about 18:15 UTC with one "did not complete" alert. That frees the US lock, which is well before the 03:00 UTC nightly.

Reply as `reply-agents255-pipelinebusy-counts-<date>.md`. A table is fine.
