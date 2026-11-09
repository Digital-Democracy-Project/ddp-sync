# Reply (read-only): **No, it will not trigger.** Pennsylvania is not in the host's archive list, so tonight's scrape ends without an archive and without a LegBot run (OPEN-478). 2026-11-09, 02:20 UTC

Reply to `297bafc`. **Read-only: no config edit, restart, trigger, dispatch or dry run, and nothing on the Mac.** The one experiment I ran was a throwaway local timeout test (item 4); the one test of a config change used a temporary copy, deleted afterwards (item 5).

**Why no:** `maybe_trigger_archive_after_scrape("pa")` returns silently unless `pa` is in the host's `openstates_archive.jurisdictions`, and **it is not**. Nothing else on the host would schedule a Pennsylvania archive or LegBot dispatch tonight.

## 1. The host's archive config
- **`/opt/ddp-sync` revision:** `11cd64e` (merge of #267), the one deployed on 11-06.
- **`openstates_archive.enabled`: `true`** (`use_fargate: true`).
- **The list the hook reads** (resolved by the running container, `_load_archive_config_for_scrape_trigger()`, which opens `config/sync_schedule.yaml` at call time): **`['fl', 'ut', 'az', 'wa', 'va', 'mi', 'ma', 'al', 'us', 'nc']`. `pa` is not in it** (nor are `ne` and `la`). (`openstates_archive.knowledge_base_embedding.jurisdictions` is a separate list: `fl, us, va, mi, wa, az, ut`.)
- **Anything else that would schedule Pennsylvania tonight: no.** ddp-sync's job list has `openstates_primary_scrape_pa` at 04:00 UTC daily, and the weekly archive jobs for the ten states above (Monday fl and ut, Tuesday az, Wednesday va and wa, Thursday mi and nc, Friday ma, Saturday al, Sunday us), **no Pennsylvania archive and no LegBot job**. `bitnami`'s crontab has no line naming Pennsylvania, LegBot or an archive; `/etc/cron.d` holds only certbot, e2scrub and php. I know of no one-off job.

## 2. What happened after the 11-08 scrapes
- **Nothing was logged for `pa`, `ne` or `la`**: no `scrape_triggered_archive_result`, no `scrape_triggered_archive_failed`, no `archiver_triggered_legbot_*` line, since the 11-06 20:02 restart (the logs I can read). That fits the hook returning silently when the state is not listed. For comparison, `scrape_triggered_archive_result` was logged once on 11-08 for each of az, fl, ma, mi, nc, ut, va and wa and twice for usa.

## 3. The EC2 settings this path uses (as the running container resolves them; no secrets)
- `LEGBOT_SCRAPE_COMPLETION_TRIGGER_ENABLED`: **true**.
- `MAC_DDP_SYNC_BASE_URL` set: **yes**; `MAC_DDP_SYNC_API_KEY` set: **yes**.
- `legbot_scrape_completion_trigger_artifact_types`: **`bill_summary`, `bill_pros_cons`, `bill_vote_yes_frame`, `bill_vote_no_frame`, `bill_supporting_orgs`, `bill_opposing_orgs`, `bill_impact_analysis`, `bill_topics`, `bill_changelog`** (nine types).
- `legbot_scrape_completion_trigger_limit`: **`None`, which means no limit: every bill in a touched session.**
- `legbot_scrape_completion_trigger_include_concept_statements`: **true**. (The call itself always sends `include_org_research: false`, `retry_failed: false`, `dry_run: false`.)
- `rds_openstates_api_base` set: **yes**.
So **if the trigger did fire for Pennsylvania, the Mac would be asked to generate all nine artifact types plus concept statements for every touched Pennsylvania session with no limit** (the 2025-2026 session has 5,277 bills). Worth knowing before anyone decides to enable it.

## 4. `_MAC_TRIGGER_TIMEOUT_SECONDS` and the empty error
- The constant on the host is **`3600.0`** (`openstates_archive.py:503`).
- **Yes, an empty `error=""` matches an httpx read timeout.** In the running container (httpx 0.28.1) I ran a throwaway loopback server that accepts the request and never answers, with a 1-second timeout: the exception is `ReadTimeout` and **`str(e)` is `''`**. And every one of the 8 logged failures falls **3,600 or 3,601 seconds after its archive finished** (US 11-04 04:18:07 after 03:18:06; US 11-05 04:15:19 after 03:15:18; US 11-06 04:15:56 after 03:15:56; MA 11-06 06:03:32 after 05:03:31; US 11-07 04:17:38 after 03:17:38; MI 11-08 03:11:40 after 02:11:40; VA 11-08 03:23:41 after 02:23:41; US 11-08 04:13:41 after 03:13:40). So they are timeouts at exactly that value (reported in `15b935b9`).

## 5. If `pa` is not listed: the smallest change, **described only, nothing changed**
- **To make tonight's scrape start the chain:** add `pa` to `openstates_archive.jurisdictions` in `/opt/ddp-sync/config/sync_schedule.yaml` (line 909 onward). **The hook opens that file at call time and the config folder is mounted from the host, so the edit is seen without a restart** (a restart is needed only to register a weekly archive cron for the state). It must be in place before the scrape ends, about 11:00 UTC.
- **The drift check would object.** I ran `check-jurisdiction-drift.py` against a **temporary copy** of the schedule with `pa` added (deleted afterwards; the real file is unchanged): it reports `DRIFT archive.scheduled: ddp-sync openstates_archive.jurisdictions` with **"in ddp-sync openstates_archive.jurisdictions but not in the manifest: pa"**, because the manifest says `archive.scheduled: false` for Pennsylvania (and for Nebraska and Louisiana). (The same run also printed two `ERROR cannot read .../src/...` lines; those come from my temporary copy having only the schedule file, not from the change.) To keep the check clean, the manifest's `pa` entry would need `archive.scheduled: true`, **which would also register a weekly Pennsylvania archive job at the next restart.** The daily drift check at 06:20 UTC would otherwise report it.
- **What the chain would then do:** after the scrape, an archive of Pennsylvania on Fargate (the earlier trial saved 6,188 documents), then a trigger to the Mac with the settings in item 3. **Ramon's call.**

## Not done
No file edit, restart, trigger, archive or scrape, and nothing on the Mac.
