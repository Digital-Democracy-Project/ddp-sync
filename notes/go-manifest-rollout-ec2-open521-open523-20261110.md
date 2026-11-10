# GO: Part C of `6d767ca`, the manifest rollout on the EC2-broker host (OPEN-521, OPEN-523). 2026-11-10 about 18:35 UTC (Mac side)

**Ramon's words, 2026-11-10** (pop-up answer, after your `0e0038f`): "let's do the new image now and then roll it out."

**Done first, as he asked:** scrapers image **v55 = task-definition revision 58**, registered from `deploy-scrapers-image.sh` at about 18:30 UTC. I checked that v55 carries OPEN-528 (Alabama actions oldest first) on top of everything in v54.

## What I need from you
1. **Gates:**
   * The running ddp-sync is the Part B image (`5919c8c0547d`, `cb6f947`). Your `0e0038f` says it is.
   * Nothing in flight.
   * Clear of 02:00, 02:30, 03:00 and 04:00 UTC.
2. **Dry run:** `bash deploy/rollout-manifest.sh --host ec2 --ddp-sync-root /opt/ddp-sync --state al --dry-run` in `/opt/ddp-open-states`. Report the lines.
   **Expected:**
   * incoming `main` at `a2ade4c` or later;
   * validate `OK: 13`;
   * drift check `OK ... (13 jurisdictions)`;
   * Alabama `status: scraping`, `cloud_path: true`;
   * no `tier` anywhere.

   If anything differs, **stop and report**.
3. **The real run:** the same command without `--dry-run`. Add `--recreate` only if it exits 3, and read the compose comment first.
4. **Check (read-only):**
   * the host checkout's HEAD;
   * api-v3's `/ddp/enrollment` shows Alabama `status: scraping` and no `tier`;
   * ddp-sync's `GET /schedule` now lists **`openstates_scrape_al`**, next run **Sunday 11-15 02:00 UTC**, alongside the other 12 jobs (13 in all).
5. **Only if `openstates_scrape_al` is not listed** (ddp-sync re-reads the list by modification time, but job registration may need a start): restart **only the ddp-sync container**, by the host's usual unit, still clear of 02:00 to 04:00 UTC. Then check again: 13 `openstates_scrape_*` jobs, Alabama's on Sunday 02:00. The restart also clears the pipeline locks, by design.
6. **Reply:** `reply-manifest-rollout-ec2-20261110.md` with the dry-run and run lines, the checks, and whether a restart was needed.

## Not authorized
* Starting a scrape, archive or LegBot run (Alabama's first run is the scheduled Sunday one).
* Any yaml or environment edit.
* Any container other than api-v3 (only through the script's `--recreate`) and ddp-sync (only for step 5).
* The Mac host.
* A task-definition change.

## Still owed from `6d767ca`
Tonight's Pennsylvania nightly read end to end (scrape, archive, LegBot in one job; status `completed`; no `openstates_archive_*` job).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
