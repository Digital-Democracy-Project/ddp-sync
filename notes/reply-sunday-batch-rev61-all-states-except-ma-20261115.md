# Reply (read-only): Sunday 11-15 batch on revision 61, every state except Massachusetts, 2026-11-15 16:37 UTC (prod agent)

Answers `7926be8`. Massachusetts gets its own note when its pipeline finishes (its Fargate collection ended 16:34:29; ddp-sync is now loading it).

## Top: nothing looks like the image; one run FAILED on ddp-sync's time limit
- **No import error, missing module or start-up Python error in any task log.** The only 2 `Traceback` lines in `/aws/ecs/ddp-scrapers` from 02:00 to 16:30 are the scraper's own "no objects since the cutoff" path in **fl** and **wa** (`ScrapeError: no objects returned from WABillScraper scrape`, then `wa: no new objects since cutoff (no-op)`), and both runs end `status ok, found 0`.
- **Pennsylvania FAILED: `pa-bfb6773bf3ba`, 04:00:00 to 16:00:18 UTC.** ddp-sync: `cloud_scrape: collection failed detail='gave up waiting after 43200s ... task stop requested'`, then `PipelineFailed: pa: 1 of 1 scrape session(s) failed`, `status=failed`, `archived=None`. ECS: task `bcb4602e932043c38e8e45ce32cf1775`, **task definition `ddp-scrapers:61`**, stopped 16:01:25 (`UserInitiated`, "ddp-sync: max_wait_seconds exceeded"). **It was not stuck:** its last lines (16:00:41-16:00:48) are normal `scrapelib: GET` calls and `save bill HR 539` / `save vote_event` on session 2025-2026, moving on to HR 540. So the run needed more than 12 h and was cut off; **nothing was loaded into RDS and no archive or LegBot step ran for PA.** The CAMS alert went out (`operator_alert_sent_via_cams http_status=202`, 16:00:18). I did not retry it (not authorized) and did not look at why it is slower than the earlier nightlies.

## Revision
**Confirmed 61 from ECS for PA and MA** (the only two tasks ECS still lists; stopped tasks age out in about an hour). For the other runs I cannot read the revision (ECS no longer lists them; `ecs:ListTaskDefinitions` is denied to this host's role). What I can show: the FL and WA tracebacks carry Python 3.13's `~~~^^` markers and `/opt/venv/bin/os-update`. That is consistent with 61, not proof.

## One line per run (UTC; 11-15)
Scrape = Fargate collection summary (`found` = objects, `s` = its duration). Archive = the archive task's own summary. LegBot = ddp-sync's step.
- **fl** `fl-47a114afd127` 02:00:00-02:21:36 **completed**. Scrape ok, found 0 (5 s). Archive `fl-archive-625bda3ce7d2` ok: checked 7,702, fetched 0. LegBot: `no_sessions_touched`.
- **mi** `mi-0b236dcb8b1e` 02:00:00-03:24:03 **completed**. Scrape ok, found 67 (383 s). Archive `mi-archive-ba5fa0d1262d` ok: checked 4,234, **archived 122, fetch_errors 16**. LegBot: `archiver_triggered_legbot_wireguard_trigger_failed error=` (empty) at 03:23:11 (see LegBot below). Search refresh 63 bills with text; KB embedding 61 bills, 160 chunks, failed 0.
- **va** `va-698a32edc782` 02:00:00-03:24:17 **completed**. Scrape ok, found 10 (1,201 s). Archive `va-archive-4a2bdb2b9b40` ok: archived 12, **fetch_errors 2**. LegBot: trigger_failed, empty error, 03:23:40. Refresh 6 bills; embedding 6 bills, 56 chunks, failed 0.
- **wa** `wa-ae31adeecfe1` 02:30:00-02:37:48 **completed**. Scrape ok, found 0 (245 s, the no-op traceback above). Archive `wa-archive-37f203727643` ok: checked 3,411, fetched 0. LegBot: no sessions touched.
- **ut** `ut-dc4bf485d673` 02:00:00-02:37:23 **completed**. Scrape ok, found 4 (2,060 s). Archive `ut-archive-76b0289caeb6` ok: checked 1,021, fetched 0. LegBot: no sessions touched.
- **nc** `nc-afdaa00bc4de` 02:00:00-03:00:31 **completed**. Scrape ok, **found 3,128** (3,153 s), **781 vote events**. Archive `nc-archive-db0a51a42402` ok: checked 2,343, fetched 0. LegBot: no sessions touched.
- **usa** `usa-39e5d67046ed` + `usa-c457458312c2` 03:00:00-03:17:03 **completed** (session 119). Scrape ok, found 15 and 7, 1 vote event. Archive `us-archive-0f80da8a3c6b` ok: checked 39,277, fetched 0, **blocked 575 (same as every US archive since 11-12)**. LegBot: no sessions touched; refresh 4 bills.
- **ne** `ne-55a3aa98e6f5` 02:00:00-03:10:53 **completed**. Scrape ok, **found 3,621** (3,623 s), **1,772 vote events**. Archive `ne-archive-c930edaad70e` ok: checked 1,847, fetched 0. LegBot: no sessions touched.
- **az** `az-b9125f0c203b` 02:00:00-03:26:22 **completed**. Scrape ok, found 899 (4,915 s), no vote-event line. Archive `az-archive-5082db93d02f` ok: checked 2,190, fetched 0, blocked 3. LegBot: no sessions touched.
- **al** `al-5e2e5c22bba3` 02:00:00-04:27:20 **completed**. Scrape **mode full**, ok, **found 4,061** (4,087 s), **2,550 vote events**. Archive `al-archive-3ae99cff54d0` ok: checked 1,507, **archived 456**, fetch_errors 0. LegBot: trigger_failed, empty error, 04:27:20.
- **la** `la-b4f6e4600c8b` 02:00:00-06:07:42 **completed**. Scrape ok, **found 5,792** (13,629 s), **3,172 vote events**. Archive `la-archive-fb6cf2120597` ok: checked 2,616, fetched 0. LegBot: no sessions touched.
- **pa**: FAILED, above. **ma**: next note.

## Your ask 3: vote events on revision 61
**Yes.** The scrape reports show vote events saved on 61 for **NC 781, NE 1,772, AL 2,550, LA 3,172, US 1** (MA's 511 are in its own note). Those are the scraper's own `vote_event:` object counts. **I cannot give you the RDS import's own saved/updated line:** ddp-sync logs the same `no usable filing figures for a successful run` warning for every state (expected marker file missing), so the load step prints no summary. The warning is not new.

## LegBot (the part to look at)
- The three states where the Mac was triggered (**mi, va, al**) all logged `trigger_failed` with an **empty error text** at exactly **3,600 s** after the archive finished (mi 02:23:11 to 03:23:11, va 02:23:40 to 03:23:40, al about 03:27:20 to 04:27:20). That is the old 3,600 s timeout, not the 503 or the disconnect of 11-14. **The work happened anyway:** on the broker side **2,011 POSTs** to the write routes between **02:23:26 and 05:16:14 UTC, all 2xx** (936 new artifacts 201, 1,008 updates 200, 67 concept sets 201). I did not split those by state.
- The other states touched no sessions, so no trigger was sent.

## Not done, per your note
No retry, restart, pull, schedule or env change. No rollback (not mine, and nothing points at the image).
