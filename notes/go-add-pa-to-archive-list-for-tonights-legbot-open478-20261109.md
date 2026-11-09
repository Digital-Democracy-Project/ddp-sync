# GO: add `pa` to the EC2 host's `openstates_archive.jurisdictions`, so tonight's Pennsylvania scrape triggers its archive and then LegBot on the Mac (OPEN-478; reply to `ece1703`). 2026-11-09 about 02:40 UTC (Mac side)

**Ramon's words** (2026-11-09, /ask-me pop-up, choosing between a manual curl, this, and "not tonight"): **"Add PA to the archive list"**.

He chose this after reading what your `ece1703` item 3 implies: **the Mac will be asked for all nine artifact types plus concept statements for every touched Pennsylvania session, no limit** (2025-2026 has 5,277 bills). That includes HB 1331, which stalled the generator twice (AGENTS-163). It may also show the drift check flagging Pennsylvania until its archive schedule is turned on. He accepted both.

## What to do (host only, before Pennsylvania's scrape ends; it starts 04:00 UTC and ends about 11:00 UTC; do it **now**)
1. **Back up** `/opt/ddp-sync/config/sync_schedule.yaml` (mode as is).
2. Add `- pa` to `openstates_archive.jurisdictions` (line 909 onward), **and nothing else.** Leave the weekly `schedule` map alone, and do not restart: the hook opens the file at call time.
3. **Prove it:**
   * `diff` against the backup shows exactly one added line;
   * `_load_archive_config_for_scrape_trigger()["jurisdictions"]` in the running container now includes `pa`;
   * `python -c "import yaml; yaml.safe_load(open(...))"` parses.
4. **Expect the drift check to object.** It runs daily at 06:20 UTC; your temporary-copy test showed `DRIFT archive.scheduled ... pa`. Note the alert as expected and caused by this change, not a fault. Do not "fix" it by changing the manifest.

## Watch (read-only), and report
* **After the scrape:** `scrape_triggered_archive_result` for `pa` (Fargate archive on v50 / revision 52, task id, documents saved).
* **Then:** `archiver_triggered_legbot_*` for `pa`: the session resolved (`2025-2026`) and the POST to the Mac. The `wireguard_trigger_failed` with empty error after 3,600 s is the known read timeout; the Mac carries on.
* **Then, the broker side:** the `fd973b97` Part B after-read (ddp-broker-py) once write-backs start arriving. The baseline there is from 20:28 UTC.
* **Stop and report if:** the archive fails; the trigger is not sent; or any `400` or `5xx` appears on `POST /api/bill-artifacts/` for Pennsylvania rows. Do not retry or replay.

## Rollback
Restore the backup; that takes effect at once. Nothing to restart.

## Not authorized
* Any other line in the file, or any other state.
* A restart, or a manual archive or LegBot trigger.
* Changing the manifest or the trigger settings (limit, artifact types).
* Anything on the Mac.

## Record
This is a **host-local edit**, not in ddp-sync `main`. Mention it in your reply, so a later `git pull` on the host does not surprise anyone. I will note it on OPEN-478.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
