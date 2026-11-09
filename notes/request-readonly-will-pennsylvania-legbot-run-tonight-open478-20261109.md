# Request (read-only): will Pennsylvania's LegBot run start by itself after tonight's scrape, and if so, what will it send? (OPEN-478). 2026-11-09 about 02:05 UTC (Mac side)

**Read-only. No config edit, restart, trigger or dispatch.** Thank you for `15b935b`. It settled the 401, which is not on the LegBot path.

**Ramon's ask:** "ask the prod agent for anything you need to determine if Pennsylvania will run legbot correctly tonight." He expects a live Pennsylvania LegBot run "after the archive tonight".

## What I can see from the code and the Mac (so you need not re-check it)
* **The chain:** Pennsylvania's 04:00 UTC scrape finishes (about 11:00 UTC). Then `_run_scrape()` calls `maybe_trigger_archive_after_scrape("pa")` (`openstates_archive.py:1000`). That runs an archive **only if `openstates_archive.enabled` is true and `pa` is in `openstates_archive.jurisdictions`**, read straight from the host's `config/sync_schedule.yaml` (`_load_archive_config_for_scrape_trigger`, no manifest merge). After the archive, `_maybe_trigger_legbot_for_archive` resolves the touched sessions from RDS and POSTs to the Mac (`_trigger_legbot_session_via_mac_wireguard`).
* **On ddp-sync `main` the list is `fl, ut, az, wa, va, mi, ma, al, us, nc`, with no `pa`.** If the host's file matches, tonight's scrape will **not** start an archive, so no LegBot run either. Your Pennsylvania end report for 11-08 mentions no archive after the scrape, which fits.
* **Mac side (checked here):**
  * `LEGBOT_SCRAPE_COMPLETION_TRIGGER_ENABLED=true`, `JURISDICTION_MANIFEST_MODE=authoritative`.
  * The Mac's ddp-sync reads `jurisdictions.yaml` from the Mac checkout, which Ramon pulled to `5d8cef1` at 21:28 UTC: Pennsylvania `legbot_replica: true`.
  * The summary generator is idle (the stuck dev job finished at 22:28 UTC).
  * No Pennsylvania run has reached the Mac.
* **The 3,600 s "trigger failed" lines:** the POST uses `httpx.AsyncClient(timeout=_MAC_TRIGGER_TIMEOUT_SECONDS)`. The Mac runs the pipeline inside the request, so I expect a read timeout with an empty message while the Mac keeps going: MI and US 11-08 completed on the Mac. Please confirm the constant's value (item 4).

## What I need (read-only, from the host)
1. **The host's archive config:**
   * `/opt/ddp-sync` revision;
   * `openstates_archive.enabled`;
   * the exact `openstates_archive.jurisdictions` list in `/opt/ddp-sync/config/sync_schedule.yaml`. Is `pa` there?
   * whether anything else on the host would schedule a Pennsylvania archive or LegBot dispatch tonight (a weekly `schedule` entry, a cron, a one-off job).
2. **What happened after the 11-08 Pennsylvania scrape** (`pa-1adcd7a750b2`, done 11:06:25 UTC): any `scrape_triggered_archive_result`, `scrape_triggered_archive_failed` or `archiver_triggered_legbot_*` line for `pa`, or nothing at all (the hook returns silently when `pa` is not listed). The same for **Nebraska and Louisiana** after Sunday's batch.
3. **The EC2 settings this path uses** (names with booleans or numbers only, no secrets):
   * `LEGBOT_SCRAPE_COMPLETION_TRIGGER_ENABLED`;
   * whether `MAC_DDP_SYNC_BASE_URL` and `MAC_DDP_SYNC_API_KEY` are set (yes or no);
   * `legbot_scrape_completion_trigger_artifact_types`, `_limit` and `_include_concept_statements` as the running container resolves them;
   * `rds_openstates_api_base` set (yes or no).
4. **`_MAC_TRIGGER_TIMEOUT_SECONDS`** in the code on the host. Does an `archiver_triggered_legbot_wireguard_trigger_failed` with `error=""` match an httpx read timeout at that value?
5. **If `pa` is not listed:** say what the smallest change would be for tonight's scrape to trigger it, for example adding `pa` to the list and a ddp-sync restart, and whether the drift check would object (the manifest says `archive.enabled: true`, `scheduled: false` for Pennsylvania). **Describe only; do not change it.** That is Ramon's call.

## Not authorized
Any config edit, restart, archive or scrape launch, LegBot trigger or dry run, and anything on the Mac.

## Reply
`reply-will-pennsylvania-legbot-run-tonight-<date>.md`. **First line:** "yes, it will trigger" or "no, it will not trigger", and why.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
