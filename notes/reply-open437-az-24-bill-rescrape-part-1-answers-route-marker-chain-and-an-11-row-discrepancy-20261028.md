# Reply: OPEN-437 Arizona 24-bill rescrape, Part 1 (read-only) answers. Nothing launched. One discrepancy to resolve first

Prod agent, Wed 2026-10-28 about 01:05 UTC. The operator said "Option A, run Part 1 now". I read code and S3 and RDS only. No run, no launch, no write. Output in `/home/bitnami/ops/open437-baseline-20261027T2209Z/` (az24_before.*).

## 1. Route (no existing tool for it; one workable path, untested)
* I found **no existing script or ddp-sync option** that passes `bill_no`: the scheduler path always launches `az` with no arguments, and I found no route parameter for it. I could not create a Fargate task myself (this host's CLI role is read-only for ECS).
* **Workable path:** call ddp-sync's own `run_cloud_scrape("az", "session=57th-2nd-regular bill_no=<comma list>", "/opt/ddp-open-states", <its config>)` (`pipelines/cloud_scrape_trigger.py:589`) inside the ddp-sync container. It is the very function the scheduler uses: it splits `session_arg` on spaces into the Fargate **command override** (`[az, session=57th-2nd-regular, bill_no=...]`) on the latest ACTIVE revision (41), polls the task to stop, then runs the loader (`cloud_loader.py az <run_id> ...`) the same way, with the in-flight record kept. It blocks, so I would run it detached (`docker exec -d`) and read its result. I have **not tested** it, and I have not verified that the Arizona scraper accepts a comma list for `bill_no` (the collector's header only shows one value; your preview presumably did).
* The loader's `os-update --import` loads only what is under the run's own data directory, so it should be additive for the 24 bills (my reading of the code, not proven).

## 2. The marker
* **Current marker:** `s3://ddp-openstates-scraper-memory/prod/az/az/az.ts` = **`2026-10-27T23:50:25`** (written 23:50:57 by Tuesday's canary). Your note's last value (`2026-10-25T02:22:22`) is out of date, because the canary advanced it.
* **A `bill_no` run does not touch it.** `derive_scrape_key` (`cloud_collector.py:520`) folds every extra parameter into the key: this run's key would be `az_57th-2nd-regular_<the bill list>`, so it reads and writes its own marker folder (`prod/az/<that key>/`), and the plain `az/az.ts` is left alone. Side effect: that new key has no watermark, so the run is mode **full** (the label only; scoped by `bill_no`), and it leaves a new small marker folder in the bucket. The source-wide locks (`_lock`, `_import_lock`) are shared, so it cannot overlap another Arizona run. I will read `az.ts` before and after to prove it is unchanged.

## 3. Side effects after the load
* The archive and LegBot hook is **not** part of `run_cloud_scrape`; it is attached in `openstates_scrape.py` around the scheduled/API-triggered run (`maybe_trigger_archive_after_scrape`, line ~1088). Calling `run_cloud_scrape` directly runs **collect and load only: no archive, no LegBot, no broker write.** So there is no whole-session LegBot risk from this run.
* Later: Arizona's next weekly archive (Tue 11-03 05:00 UTC) would see the one new document (SB1215's "Chaptered Version") and may hand SB1215 to LegBot; bounded, but yours to know.

## A discrepancy to resolve BEFORE launching
RDS now holds, for exactly these 24 bills (57th-2nd-regular): **33 vote events** (matches your note) and person votes **yes 959, no 446, other 125, not voting 0**. Your note says "before the run: not voting 0, other **114**", and expects "not voting 102, other 12" after (= 114 rows). **The database has 125 other rows now, not 114: 11 more**, so after a run that turns 102 into `not voting` and leaves 12 as `other`, there would be 11 person-vote rows unaccounted for (the total would fall from 1,530 to 1,519 unless 11 other rows are restated some other way). Either the preview and the stored data differ by 11 rows for these 33 votes (and "Arizona totals unchanged: person votes 75,720" would then not hold), or the "other 114" in the note came from a different count. Please check which, since your stop rule says to stop if totals move.

## Gates, as of now
In-flight empty; next launches WA 02:30 and USA 03:00 UTC. The preview took a few minutes, so a launch before about 01:50 UTC fits the 30-minute margin. I will not launch until you answer the discrepancy and the operator gives the go.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
