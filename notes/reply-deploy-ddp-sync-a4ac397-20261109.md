# Reply: ddp-sync a4ac397 deployed on EC2 (SYNC-146 part 2), 2026-11-09 03:50-03:53 UTC (prod agent)

Answers `814e740`. **User's words in this session:** pop-up "Yes, update ddp-sync now". The `4819e8b` hand edit was never made (the user said "Not yet" to it at about 02:55), so there was no `- pa` line to remove; step 4 is not applicable and `config/sync_schedule.yaml` was never touched.

**Gates:** `/opt/ddp-sync` at `11cd64e`, `git status` clean; origin/main exactly `a4ac397`; `JURISDICTION_MANIFEST_MODE=authoritative`; no ddp-sync job in the last 12 min, none due before the restart (clear of 02:00/02:30/03:00; done before 03:55).

**Steps:** log gzipped to `~/ops/log-archive/ddp-sync-pre-sync146p2-*.gz`; rollback tag `ddp-sync:pre-sync146p2`; `git merge --ff-only a4ac397` (4 files: primitives.md, openstates_archive.py, jurisdiction_enrollment.py, test_jurisdiction_enrollment.py); `sudo systemctl restart ddp-sync` at 03:50:01; container started 03:50:52Z, **healthy 03:51:57**, image 47cf074da2d2.

**Scheduler:** 20 jobs registered, same count as on 11-06; FL 02:00 (Sunday), WA 02:30, USA 03:00, primary PA 04:00, secondary scrapes list, no new or missing job seen.

**Verify (from the running app, read-only):** yaml list `fl, ut, az, wa, va, mi, ma, al, us, nc`.
- `enrolled("archive_after_scrape", yaml, root)` = al, az, fl, **la**, ma, mi, nc, **ne**, **pa**, us, ut, va, wa (yaml list plus la, ne, pa).
- `enrolled("archive", ...)` = al, az, fl, ma, mi, nc, us, ut, va, wa: unchanged, no pa/ne/la.
- It logged `jurisdiction_manifest_disagrees feature=archive_after_scrape only_in_manifest=['la','ne','pa'] only_in_yaml=[]` (a warning, expected, mode authoritative).
- **Manifest revision:** `/opt/ddp-open-states` is at `2af610d1`, NOT `5d8cef1`. `5d8cef1` is #352, docs only, and I did not pull it (not authorized). PA, NE and LA each have `archive: enabled true, scheduled false, timeout_s 14400` in this checkout.

**Drift check by hand:** `OK: every list compared agrees with jurisdictions.yaml (13 jurisdictions).` (openstates-core not compared.)

**Consequence tonight:** Pennsylvania's scrape (04:00 UTC) should now trigger the archive and then LegBot (nine types, concept statements, no limit). NE and LA also trigger after their next scrapes. I will read the archive and trigger lines after the scrape. No archive, LegBot or scrape was started by hand.

**Rollback:** `git checkout --detach 11cd64e`, restart ddp-sync (or retag `pre-sync146p2`).
