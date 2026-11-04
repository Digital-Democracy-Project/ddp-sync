# Request: Stage 4 timed archive trial for PENNSYLVANIA and LOUISIANA together (OPEN-478, OPEN-480), two Fargate archive tasks, 2026-11-04

**Ramon's go (this session, 2026-11-04, /ask-me; he chose "Both now" over my recommendation of Pennsylvania first, alone).** Nebraska ran clean with this exact method (your report `2df6b64`: 58 min, 3,382 of 3,383 saved, no blocks). Same path, same limits, two states at once. **Because they run together, the early read in step 5 is how we protect against Pennsylvania's one real unknown.**

## The path (unchanged from the Nebraska request `32a108b`; read its "path" section)
Call **`_run_archive_fargate("<st>", config=<the app's openstates_archive block>)`** from `ddp_sync.pipelines.openstates_archive`, inside the running ddp-sync container, **once per state, no hooks** (not `run_single_archive_job`, not the manual trigger: it 404s for these states because `archive.scheduled` is false). Do not set `archive.scheduled`, do not edit `config/sync_schedule.yaml`. Two launches, `pa` and `la`, one task each; start them together or a minute apart, your choice.

## Why Pennsylvania is the risk
Pennsylvania's scraper emits PDF, HTML and Word links; only the PDF is read, by `extract_line_numbered_pdf`, which keeps **only lines that start with a number**. That is right for numbered PDFs and would return little or nothing for an unnumbered one. Nobody has opened a Pennsylvania document yet. Louisiana is PDF only, read by `extract_sometimes_numbered_pdf` (the reader Nebraska used), so I expect it to behave like Nebraska. Scale: Pennsylvania 6,194 versions (about 1.7 hours at Nebraska's rate of about 58 documents a minute), Louisiana 10,468 (about 3 hours).

## Before launching (read-only; UTC)
1. **Gates:** 0 ECS tasks, 0 in-flight records, no ddp-sync job in the last 10 minutes; away from 02:00, 02:30 and 03:00 (tonight's Washington and USA jobs; Nebraska overlapped them without a problem you could see, but I would still avoid it) and **not near Sunday 11-08 02:00 UTC** (the secondary batch, which now includes `pa`, `ne`, `la` and runs for hours). You choose the time; tell Ramon.
2. **Read:** `ddp_bill_version_document` rows for `pa` and `la` (expect 0), and the row count of every other state, read-only (`SET TRANSACTION READ ONLY`), saved for the comparison afterwards.

## Run
3. Launch each **once**; **if a launch fails or a task fails, do not launch it again**; report.
4. Read the first task's `taskDefinitionArn` and image (expect `ddp-scrapers:45` and `v42`, as for Nebraska).
5. **Early read of Pennsylvania, about 15 minutes after its launch, and again at about 30 minutes (read-only on RDS):** the number of Pennsylvania documents saved so far, how many have `is_error` true, **how many have `raw_text` empty or under 50 characters**, and the shortest five lengths. **If more than 20% of the first 100 or more documents are empty or under 50 characters, that is the reader failing: stop the Pennsylvania task if your role lets you (`ecs stop-task`; leave Louisiana running), keep what is saved (nothing is deleted), and tell Ramon at once, with the counts and three example document addresses.** If your role cannot stop it, say so and report; do not try another route.
6. At about 3 hours report Louisiana's progress if it is still running; at **4 hours** report either task still going (the archive's own limit is 14,400 s; Louisiana may need all of it, say so).

## Report when each finishes (each with its value)
* Start and end (UTC), `duration_seconds`, exit code, task id, and **the archive's summary line verbatim** (fetched / skipped / archived / fetch_errors / blocked / extract_errors / conflicts / concurrent_writes / s3_verified / s3_unverified / persist_errors).
* **Every `failed to persist` line**, with the address (OPEN-487 is the bug behind them; Nebraska lost one amendment to a too-long file name, and these two states' addresses may be longer).
* Counts of `no function for`, 429, 403, "Too Many", Barracuda, block page, `Traceback` in each task's log; the first 10 log lines (redacted); log level counts.
* After each: read-only on RDS, that state's `ddp_bill_version_document` rows **by `media_type` and `is_error`**, how many are empty or whitespace-only, and that every other state's count is unchanged.
* S3: object count under `s3://ddp-bill-archive/bills/raw/<st>/` and size; `s3_verified` from the summary is enough for the check.
* **No LegBot, embedding, search-refresh or relay line** in ddp-sync's log for `pa` or `la` since the launches: report the count (expect 0).

## Stop and tell me (do not start anything else)
A launch is refused or a task is not placed; **any 429, 403, "Too Many", Barracuda or block page** (do not retry: a WAF block on the legislature's site is the one that matters); a non-zero exit; `blocked` above 0; the Pennsylvania early read in step 5; a task still running at 4 hours.

## What I will do with it
Read the saved text from the Mac's replica (read-only), as for Nebraska (OPEN-479, comment 18969): extractor health per media type, empty text, the share of versions with no stage under the OPEN-482 rules, the length of any amendment-type families, line numbers, headers and symbols in the text, and the stored comparisons. Pennsylvania's reader is checked first.

## Not asked
No scrape, no other state, no people load, no LegBot, embedding, search refresh or relay, no change to `archive.scheduled`, any allowlist, `.env`, Secrets Manager or `config/sync_schedule.yaml`, no restart of ddp-sync, no deletion of any row or object.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
