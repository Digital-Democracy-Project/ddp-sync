# Request (read-only): report Sunday 11-15's scrape batch on revision 61, state by state (OPEN-515, and the Massachusetts and Alabama checks)

**Ramon, 2026-11-14 ~22:45 UTC: "post 1 and 5 now".** Read-only: nothing to start, change or restart.

## What I need from you

1. **For every scrape run that starts from Sunday 11-15 02:00 UTC through Monday 11-16 06:00 UTC** (the Sunday weekly batch, including al, la, ma, ne, and the nightlies, which were wa 02:30, usa 03:00 and pa 04:00 per your 11-13 schedule read), report one line per run:
   - state, run id, start and end (UTC), finished / failed / timed out;
   - **the task-definition revision it ran on** (expected **61**, image v58, Python 3.13, which carries openstates-core `016fc6b3` and openstates-scrapers `361737d8c`);
   - bills and vote events the import reports as saved or updated (the import's own summary line);
   - the archive step: finished, documents saved, retries;
   - the LegBot step: the trigger's answer from the Mac (2xx, 409 or 503) and, if you can see it, whether the LegBot run finished;
   - any traceback or ERROR line (first line and count; no secrets).
2. **Massachusetts can run up to 24 hours.** Post the other states as soon as they are done (one `reply-` note), then a second note when Massachusetts finishes. Do not wait for Massachusetts to report the rest.
3. **One check that matters for revision 61:** state whether any run's import saved **vote events** (the 11-14 tests on revision 61 did not exercise the vote import). Name the state and the count.
4. If any run fails on revision 61 in a way that looks like the image (an import error, a missing module, a Python error at start-up), say so at the top of the note. **Do not roll back yourself:** rollback (deregistering revision 61) is Ramon's step from the Mac.

## Not authorized

Starting, retrying or cancelling any scrape, archive, people load or LegBot run; any restart or pull of ddp-sync; any change to the schedule, env, manifest, broker, api-v3 or task definitions.

## What I check on the Mac side afterwards

On the Mac replica, once you report a state finished:
- **Massachusetts:** 0 House votes whose yes/no counts disagree with their voters (16 before; OPEN-442); the legislator-matching gate (OPEN-474); the count of roll calls with a tally and no voters (OPEN-176).
- **Alabama:** votes present (OPEN-524); sponsor matching (OPEN-526); motion types (OPEN-527); latest-action order (OPEN-528); then the bill-stage check (BROKER-473).
- **Every state:** Stage 3 quality checks from the Mac's Python 3.13 tools.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
