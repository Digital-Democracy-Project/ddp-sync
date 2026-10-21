# REPORT: NC `2025` LegBot full-session run FINISHED clean at 03:02 UTC 2026-10-21 (23.0 h); 62% of `bill_opposing_orgs` rows failed with `insufficient_information`

Run (this host's call to the Mac's ddp-sync, started 04:03:02 UTC 2026-10-20, **HTTP 200 at 03:02:38 UTC 2026-10-21, 82,776 s**): `run_id` `44814cf36a4c4dc7a01815db392eefc4`, `success: true`, **2,338 bills considered, 2,338 processed, not truncated, 0 bill-level errors, 0 concept-statement failures**, peak memory 774.7 MB. Parameters as in the started report: all nine types, concept statements on, org research **off**, `limit` null, `retry_failed` **false**. The ddp-sync log shows no restart during the run; I did not restart anything and the Mac was not reloaded to my knowledge.

## What the broker holds for NC now (read-only count, 03:03 UTC)
* **2,343 NC bills** (the 2,338 plus the 5 special-session bills from 10-19), **19,667 NC artifacts**: **17,711 `complete`, 1,956 `failed`** (every row `review_status` approved, as production auto-approves); none pending or processing.
* Per type, complete / failed: `bill_summary` 2,288 / **55**; `bill_pros_cons` 2,079 / **264**; `bill_vote_yes_frame` 2,314 / 29; `bill_vote_no_frame` 2,335 / 8; `bill_supporting_orgs` 2,246 / **97**; **`bill_opposing_orgs` 878 / 1,465 (62.5% failed)**; `bill_impact_analysis` 2,306 / 37; `bill_topics` 2,342 / 1; `bill_changelog` 923 complete (the run reported 1,864 bills "not applicable" for the changelog, i.e. single-version bills).
* **Failure reason, read from the rows:** every one of the 1,465 failed `bill_opposing_orgs` rows and every one of the 55 failed `bill_summary` rows carries `failure_reason = insufficient_information`. That is the stored label, not a crash; I did not read the LegBot logic or sample the bills to confirm what it means. It matches the Washington pattern (46.1% `bill_opposing_orgs`) but is higher here.

## Observations, not conclusions
* The 5 special-session bills failed `bill_opposing_orgs` on 10-19 too; nothing here suggests a NC-specific pipeline fault, but the rate (62%) is the highest I have seen.
* `bill_changelog`: 923 complete rows vs your earlier estimate of about 3,550 NC versions without a changelog (OPEN-390). I did not reconcile the two; the changelog dispatch is one per bill (latest pair?), not one per version, which would explain the gap, but that is my guess.
* I did not read any NC summary for quality, and I made no rejection, retry or regeneration. `retry_failed` stays false until someone decides.

## Not done / needs a decision
* Quality read of NC summaries (suggest a sample against diffs, as for Michigan and Virginia); whether to retry the failed rows; the `bill_opposing_orgs` question (top LegBot data-quality issue). Virginia's archive runs at 05:00 UTC today; nothing from NC is running now.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
