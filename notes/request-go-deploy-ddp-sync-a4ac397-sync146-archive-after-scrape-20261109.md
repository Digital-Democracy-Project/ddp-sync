# Request (production change, GO given): deploy ddp-sync `a4ac397` on the EC2 host (SYNC-146 part 2), then remove tonight's host-local `- pa` line (SYNC-151; OPEN-478). 2026-11-09 about 02:40 UTC (Mac side)

**Ramon's go** (2026-11-09, pop-up): "I'll merge; deploy tonight". He merged ddp-sync **#268** at 02:34:49 UTC (merge commit `a4ac397cb5c644ee5817fae234e17adb7a811e80`).

## What changes
`maybe_trigger_archive_after_scrape` decides, in `authoritative` mode, from the shared state list's `archive.enabled`. The new feature is `archive_after_scrape` through `enrolled()`. It no longer reads only the yaml `openstates_archive.jurisdictions`.
* With the real `jurisdictions.yaml` this adds `pa`, `ne` and `la` to the after-scrape archive and drops nothing.
* The weekly archive cron (`archive.scheduled`) and the drift comparison are unchanged.
* **`11cd64e` to `a4ac397` is exactly this one PR:** 4 files (`openstates_archive.py`, `jurisdiction_enrollment.py`, a test file, `primitives.md`).
* **No change under `config/`.** A fast-forward pull keeps your host-local edit to `config/sync_schedule.yaml` if you made it under `4819e8b`.
* Full suite: 2,223 passed against 2,216 on `11cd64e`, the same single pre-existing failure.

## Gates
1. **Code:** `/opt/ddp-sync` is at `11cd64e`. Its `git status` shows only `config/sync_schedule.yaml` modified (the `- pa` line, if you made it) or nothing. Anything else: stop.
2. **Window:**
   * Nothing in flight: no ddp-sync job running and none in the last 10 minutes.
   * Stay clear of 02:00, 02:30 and 03:00 UTC (FL, WA, USA). **Best window: after USA's 03:00 job is done, before 03:55.** Pennsylvania's nightly starts 04:00.
   * If you cannot find a clear window before 03:55, **stop and tell me.** Do not restart during the Pennsylvania scrape. Tonight is then covered by the hand edit, and this waits for the day.
3. `JURISDICTION_MANIFEST_MODE` in the running container is `authoritative` (as you reported on 11-04).

## Steps (in this order)
1. `git -C /opt/ddp-sync merge --ff-only a4ac397` (or pull to `origin/main` = `a4ac397`). If git refuses because of the local edit, **stop**; do not stash or reset.
2. **Restart ddp-sync**, the same way as on 11-06 (`66f71c0`). Report the container start time and health, and that the scheduler registers the same jobs as before: no new or missing job.
3. **Verify from the running app (read-only):**
   * `enrolled("archive_after_scrape", <yaml openstates_archive.jurisdictions>, <root>)` returns the yaml list plus `la`, `ne` and `pa`;
   * `enrolled("archive", ...)` is unchanged: no `pa`, `ne` or `la`;
   * `/opt/ddp-open-states` (the manifest) is at `5d8cef1` or later, with Pennsylvania `archive.enabled: true`.
4. **Only after step 3 passes: remove the host-local `- pa` line.** Restore `config/sync_schedule.yaml` to the committed version: `git checkout -- config/sync_schedule.yaml` after backing it up. `git status` must then be clean. **No restart:** the hook reads the file at call time. Check again that `enrolled("archive_after_scrape", ...)` still includes `pa`, now from the manifest alone.
5. Run the drift check by hand and paste it. Expect OK: the `pa` drift your `4819e8b` edit would have caused is gone with the line.

## Rollback
Fast-forward back is not possible, so check out `11cd64e`, restart, and put the `- pa` line back from your backup (that is tonight's stopgap state).

## Not authorized
* Any other commit or config change.
* Changing the LegBot dispatch settings (still nine types plus concept statements, no limit, as Ramon accepted).
* Any archive or LegBot trigger by hand, any scrape.
* Anything on the Mac.

## Watch
Everything in `4819e8b`'s "Watch" section stands for tonight's Pennsylvania run: the archive after the scrape, the trigger to the Mac, and the broker write-backs (`fd973b97` Part B). The difference is that the decision now comes from the shared list.

## Reply
`reply-deploy-ddp-sync-a4ac397-<date>.md`. If you had not yet made the `4819e8b` edit when you read this, skip it; this deploy replaces it.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
