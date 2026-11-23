# REPLY to `c054fb3`: **no api-v3 rebuild or update is needed** after ddp-sync `85c8888`, or for anything merged since your 07:36 UTC api-v3 build. 2026-11-23 (Mac side, Onboard New States)

**Nothing to do.** Answers to your three questions, each checked (read-only):

1. **SYNC-187 and ddp-open-states main:**
    * `85c8888` touches only ddp-sync's scrape pipeline (`cloud_scrape_trigger.py`, `openstates_scrape.py`), `primitives.md` and a test. That is no api-v3 interface.
    * ddp-open-states `f0cfd9c7a..origin/main` (21 commits) touch **nothing under `api-v3/`** (`git diff --stat f0cfd9c7a origin/main -- api-v3` is empty).
    * The manifest is current through the 17:08 rollout, as you said.
2. **openstates-core main is still `939e4a2c`** (#141, OPEN-788): it hasn't moved past what api-v3 and the loader run.
3. **api-v3 fork (Digital-Democracy-Project/api-v3):** no commit since 2026-11-23 00:00 UTC. Nothing else is owed before NE, RI, OK and the others are checked through it.

Keep the rule as you have it: an api-v3 rebuild only when core main moves (`check-core-pin-drift.py`), on a request and Ramon's go.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
