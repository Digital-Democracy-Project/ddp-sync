# Request: Stage 4 timed archive trial for NEBRASKA only (OPEN-479), one Fargate archive task, 2026-11-04

**Ramon asked for this request (this session, 2026-11-03/04), Nebraska first; Louisiana and Pennsylvania are NOT in this note**: I will read Nebraska's result and ask separately, one state at a time. **Nothing public depends on it:** the archive saves each bill version's document and its extracted text; LegBot, embedding, search refresh and routing stay off for these states.

## Why
Runbook Stage 4 (playbook section 5.3): the three pilot states have 0 archived version documents (replica, 11-03: PA, NE, LA all 0, against NC's 6,207), so the text quality, the extractor, the version-note stages and the format cannot be checked until the text is saved. Nebraska is the smallest (1,847 bills, 3,383 versions; one chamber; all PDF, read with `extract_sometimes_numbered_pdf`). For scale, NC's trial took about 110 minutes for 2,338 bills and 6,192 documents.

## The path (please read why before choosing another)
* **Not the manual trigger.** `POST /ddp-sync/v1/trigger/openstates-archive/ne` answers **404**: its targets come from the shared list's `archive.scheduled`, which is deliberately false for `ne` (the recurring schedule waits for `live`, OPEN-418). **Do not set `archive.scheduled` or edit `config/sync_schedule.yaml`.**
* **Not `run_single_archive_job` / `_run_archive_with_hook`.** That wrapper runs the Fargate task and then the archive-completion hooks: the LegBot relay to the Mac, the bill-search refresh and the knowledge-base embedding. I do not want any of them for these states. (The Mac's LegBot gate would skip Nebraska anyway: I read it with the Mac's own settings, it allows only the nine live states. But there is no reason to send the relay.)
* **The path I want:** call **`_run_archive_fargate("ne", config=<the app's openstates_archive config block>)`** from `ddp_sync.pipelines.openstates_archive` once, inside the running ddp-sync container (the same way you called `people_check_refusal`): it launches `cloud_archiver.py` as one `ddp-scrapers` task (RUNNER_SCRIPT dispatch, RDS database URL resolved from Secrets Manager), waits for it to stop (`max_wait_seconds` 43,200) and returns `{success, duration_seconds, ...}`, **with no hooks**. If you know a safer way to launch only that task, say so and use it. If `_run_archive_fargate` refuses (`archive.enabled` is read by run-archive.sh, not by this path), report the refusal and stop.

## Before launching (read-only; UTC)
1. **Gates:** 0 ECS tasks, 0 in-flight records, no ddp-sync job in the last 10 minutes, **not between 01:50 and 04:00 UTC tonight** (WA 02:30, USA 03:00, and the people pull), and not close to Sunday 02:00. **Tomorrow's daytime is fine.** A free evening or day, your choice; tell Ramon the time you choose.
2. **Read:** the app's `openstates_archive.cloud_path.fargate` (cluster, task definition, container name, `max_wait_seconds`; present or empty), and that `ne` shows `archive.enabled: true` and `archive.scheduled: false` in the app's view of the shared list.
3. **Record:** the DB state before: `ddp_bill_version_document` rows for Nebraska (expect 0) and for every other state (so a second run can show nothing else changed), read-only (`SET TRANSACTION READ ONLY`).

## Run once
4. Launch **once**. **If it fails, do not launch again**; report. A re-run is idempotent (natural-key skip) but I want to see the first failure.
5. **Read the new task's `taskDefinitionArn` and image from `describe-tasks`** and report **the revision number and image tag**: it should be **`ddp-scrapers:45` and `...:v42`**. This is also the confirmation you could not read from the task definition (your reply `f18db1a`, item 1).

## Report (each with its value)
* Start and end (UTC), `duration_seconds`, exit code, the task id, and **the archive run summary line verbatim** (fetched / skipped / archived / fetch_errors / blocked / extract_errors / conflicts / concurrent_writes / s3_verified / s3_unverified / persist_errors).
* **The first 10 task log lines (redacted)** and counts of `no function for`, `429`, `403`, `Too Many`, `Barracuda`, a block page, `Traceback`, per media type if the log gives it; and the log level counts as you did for the scrapes.
* **After it, read-only on RDS:** `ddp_bill_version_document` rows for Nebraska: total, **by `media_type` and `is_error`**, and **how many have empty or whitespace-only `raw_text`**; the same counts for every other state (unchanged).
* **S3:** that the mirrored copies exist for a sample of ten Nebraska documents (the archive's own `s3_verified` figure is enough if the log gives it); disk headroom as you did for NC.
* **No LegBot, embedding, search refresh or relay line** in ddp-sync's log for `ne` since the launch: report the count (expect 0).

## Stop and tell me (do not run a second state or anything else)
`_run_archive_fargate` or the launch refuses; the task is not placed; **any 429, 403, "Too Many", Barracuda or a block page** in the log; a non-zero exit; `blocked` above 0; or a run still going at **4 hours** (the archive's own default limit is 14,400 s; Nebraska should take about an hour): report at 4 h even if it is still healthy. A WAF block on nebraskalegislature.gov is the one that matters: do not retry.

## What I will do with it
Read the saved text from the Mac's replica (read-only): extractor errors and empty text per media type; the share of versions with no stage using the new rules; the length of Nebraska's amendment documents (the excerpts that might need excluding); line numbers, headers and symbols in the text; then decide with Ramon the cleaner and exclusion entries, and only then ask for Louisiana.

## Not asked
No scrape, no trigger of any other state, no people load, no LegBot, embedding, search refresh or relay, no change to `archive.scheduled`, any allowlist, `.env`, Secrets Manager or `config/sync_schedule.yaml`, no restart of ddp-sync, no deletion of any row or object.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
