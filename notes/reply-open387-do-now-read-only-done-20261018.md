# Reply to OPEN-387 "Do now" (read-only) DONE. Nothing rejected, nothing regenerated. (prod agent, EC2 host, 2026-10-18 20:38 UTC)

Ramon confirmed in this session that I should do the read-only checks. No review status was changed, no trigger sent, nothing started.

## 1. Production counts per state, `bill_changelog`, against your snapshot
| state | production now | snapshot | |
|---|---|---|---|
| AZ | 1,972 (1,907 complete+approved, 65 failed+approved) | 1,972 | same |
| US | 3,245 (3,239 complete+approved, 6 failed+pending_review) | 3,245 | same |
| UT | 2,701 (2,020 approved, 680 complete+rejected, 1 failed+rejected) | 2,701 (2,020 / 681 rejected) | same |
| FL | 1,229 (1,207 approved, 22 pending+rejected) | 1,229 | same |
| MI | **1,355** (1,352 complete+approved, 1 failed+approved, 2 failed+pending_review) | 1,283 | **+72** |
| WA | 1,867 (all complete+approved) | 1,867 | same |
| VA | **511** (all complete+approved) | 213 | **+298** |
| MA | 290 (all complete+rejected) | 290 | same, **still hidden** |
| NC | 0 | 0 | same |
MI and VA grew because LegBot kept generating after the snapshot: 73 MI rows and 298 VA rows have `generated_at` after 2026-10-17 21:00 UTC (the newest MI row 2026-10-18 07:03, the newest VA row 10:31 UTC, both after this morning's scrapes and archives). The ids in your lists do not depend on those new rows.

## 2. Id check against production: **every id matches**
I checked **all 3,434 rows of every CSV** (3,294 distinct artifact ids), not only a sample: **all 3,294 ids exist on production** and each maps to the same (state, session, gov_id, new version note) as your columns, and **no row's (status, review_status) differs from your snapshot**. The sample you asked for: AZ `REWRITE_NOW` 20 of 20 match (1,497 listed), US 20 of 20 (226 listed), MI 8 of 8 (the whole list). The `.txt` lists contain only ids that are in those CSVs. (The lists are not a count of all changelogs; they are your flagged rows.)

## 3. Can I reach the Mac's ddp-sync from this host?
**Yes.** `GET http://10.0.0.8:8001/ddp-sync/v1/health` with `X-DDP-Environment: prod` and this host's Mac key (`MAC_DDP_SYNC_API_KEY` in the ddp-sync container's environment, not printed; the same address and credential the archive hook uses) returns **200** (keys `status, service, version, config_source, scheduler, redis, pinecone, flows`). I did not call any trigger. So the `bill-artifact-generation` calls could be made from this host, by me, once there is a go. The Mac's ddp-sync was reachable now; its 409 `already_running` replies seen earlier mean a run was in flight at those times.

## 4. MI and WA, diffs recomputed after their summary: I cannot list them
`ddp_bill_version_document` has no column that records a recompute: the recompute does not change `updated_at` (WA's newest `updated_at` is 2026-09-11 and AZ's 2026-09-10, long before their recomputes; MI's newest is 2026-10-18 02:13 UTC from today's archive). The recompute output I saved (`/home/bitnami/ops/open352-part3-20261016/`) holds only the count lines: **MI 32 corrected + 2 nulled, WA 40 corrected + 0 nulled** at 2026-10-16 02:19 and 02:12 UTC. To get the versions, someone has to rerun the old-code computation against a copy of the database from before 2026-10-16 02:00 UTC (a point-in-time restore is possible on RDS, 7-day window, but is not something this host's role can do) or use the Mac's earlier replica. I wrote no list.

## Two things you asked me to tell the operator (observations, not changes)
* **MA is still hidden.** All 290 Massachusetts rows are `complete` + `rejected` as of 20:30 UTC. This host's archive hook has tried to start a MA LegBot run twice since the 290 were rejected: 2026-10-17 04:34 UTC (the Mac answered **409 conflict**) and 2026-10-18 11:29:46 UTC (**failed with an empty error**, probably a client timeout). Both attempts happen in the archive step after the weekly secondary scrape. The rows were not regenerated (your note says ddp-sync skips cross-media pairs, SYNC-110, and today's attempts did not regenerate them), but a MA trigger is attempted every Sunday after the scrape.
* **FL and UT: possible automatic regeneration tomorrow.** The weekly archives for FL and UT run **Monday 2026-10-19 05:00 UTC** on this host, and the archive hook relays a LegBot run for a session the archive touched. Your note says the 11 rejected FL rows and the 11 rejected UT rows that have a diff today regenerate on the next trigger and go live at once. I cannot tell from here whether Monday's runs will touch those sessions; I did not change anything.

## Not done
No go for any change was given. The lists were not used for anything other than reading. AL: not looked at.
