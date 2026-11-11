# Reply (read-only): Pennsylvania's nightly read end to end, and Washington/USA, 2026-11-11 14:35 UTC (prod agent)

Closes the "owed after tonight's Pennsylvania nightly" item of `6d767ca`. Nothing changed.
- **Pennsylvania, one job:** `openstates_pipeline: starting jurisdiction=pa` 04:00:00; `cloud_scrape` run `pa-a1426d9de7d6` triggered 04:00:00, collection done 10:58:37, **done 11:12:48** (25,966.9 s); `openstates_archive: fargate task done` 12:58:35 (6,347.1 s); `archiver_triggered_legbot_wireguard_trigger_failed` (empty error) 13:58:46 = the known 3,600 s timeout; **`archive_after_scrape_succeeded attempt=1`**; **`openstates_pipeline: done archived=True duration_seconds=35926.3 failed=0 jurisdiction=pa sessions=1 status=completed`**. **No `openstates_archive_*` job ran on its own** (0 such lines).
- **LegBot did start:** the broker's write-backs began 27 s after the archive ended (12:59:02) and continue (201 x 1,286, 200 x 110, concept sets 201 x 76; no 400/5xx).
- **Warning to know:** `openstates_scrape: no usable filing figures for a successful run` for `pa` (marker `/opt/ddp-open-states/logs/last-run/pa.imported` missing/unreadable), as before and also for USA tonight; the run itself succeeded.
- **Washington (02:30 UTC):** one job, `status=completed archived=True duration_seconds=532.7 failed=0`, archive on attempt 1. **USA (03:00 UTC):** two scrape runs (`usa-7415af10bb1e` 336.8 s, `usa-a2a3bc5f80f1` 161.2 s) then the archive task done 03:20:53 (753.0 s).
- **Next:** Alabama's first scheduled run is Sunday 11-15 02:00 UTC.
