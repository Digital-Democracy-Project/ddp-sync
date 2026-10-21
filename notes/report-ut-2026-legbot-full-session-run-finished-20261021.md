# REPORT: Utah `2026` LegBot full-session run FINISHED clean at 19:35:39 UTC 2026-10-21; 55% of `bill_opposing_orgs` rows failed (`insufficient_information`); the 680 hidden changelogs untouched

Run (this host's call to the Mac's ddp-sync, sent 04:47:41 UTC by the operator's choice, **HTTP 200 at 19:35:39 UTC, 53,277 s = 14.8 h**; the Mac started Utah work only at 06:00:51 UTC after the US/119 scan ended, so about 13.6 h of actual work): `run_id` `ae327322b2f64c509fb8f56ab37f00a6`, `success: true`, **1,016 bills considered, 1,016 processed, not truncated, 0 bill-level errors, 0 concept-statement failures (693 dispatched)**, peak memory 1,483.9 MB. Parameters: all nine artifact types, concept statements on, org research **off**, `limit` null, `retry_failed` **false**. I did not restart anything and the Mac was not reloaded to my knowledge.

## What the run reported (per bill, summed)
* Generated: summary 706, vote_yes_frame 705, vote_no_frame 702, supporting_orgs 685, impact_analysis 702, topics 713, pros_cons 614, opposing_orgs 303.
* **Failed:** opposing_orgs **410**, pros_cons 99, supporting_orgs 28, vote_no_frame 11, impact_analysis 11, vote_yes_frame 8, summary 7, **changelog 6**.
* Skipped because already present: about 290 to 303 of each type (the earlier partial run's work); changelog not applicable: 721 bills (single-version).

## What the broker holds for Utah `2026` now (read-only count, 23:17 UTC)
* **1,017 bills, 10,840 artifacts:** 9,413 `complete`+`approved`, 745 `failed`+`approved`, **680 `complete`+`rejected` (the hidden changelogs, unchanged)**, 1 `failed`+`rejected`, 1 `failed`+`pending_review`.
* Per type, complete / failed: `bill_summary` 1,010 / **7**; `bill_pros_cons` 906 / 111; `bill_vote_yes_frame` 1,009 / 8; `bill_vote_no_frame` 1,006 / 11; `bill_supporting_orgs` 983 / 34; **`bill_opposing_orgs` 455 / 562 (55% failed)**; `bill_impact_analysis` 1,004 / 13; `bill_topics` 1,017 / 0; `bill_changelog` 2,703 complete (2,023 approved + 680 rejected) / 1 failed (the old rejected one).
* **Failure reason, read from the rows:** all 562 failed `bill_opposing_orgs` rows and all 7 failed `bill_summary` rows carry `failure_reason = insufficient_information` (a stored label; I did not read the LegBot logic or sample bills). Same pattern as NC (62.5%) and WA (46.1%).

## Observations, not conclusions
* **Unreconciled:** the run reports **6 changelog failures**, but the broker holds only the 1 old failed changelog row (the one rejected on 10-14); I do not know whether the 6 left no failed row or are counted differently.
* **Hidden changelogs:** none of the 680 rejected Utah changelogs was regenerated (the run skipped them as expected: no stored diff); 3 new approved changelogs appeared (2,020 to 2,023).
* I read no Utah summary for quality, retried nothing, and `retry_failed` stays false until someone decides. Nothing from Utah is running now.

Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
