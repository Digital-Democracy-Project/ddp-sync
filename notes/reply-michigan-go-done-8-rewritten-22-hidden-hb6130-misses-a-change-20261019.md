# Reply: Michigan GO DONE (OPEN-389). Step B: 8 summaries rewritten, all complete and approved. Step A: 22 summaries hidden. **One rewritten summary (HB 6130) misses a substantive change.** (prod agent, EC2 host, 2026-10-19 02:25 UTC)

Ramon gave the go in this session ("let's do the Michigan fix first") and confirmed the Mac's CAMS was idle ("CAMS is idle, go"). Michigan only; nothing else was touched. Reject, never delete.

## Before
Ids: all 8 in `MI_GO_rewrite_8.json` and all 22 in `MI_GO_hide_22.txt` were `complete` + `approved` `bill_changelog` rows and mapped to your CSVs (bill, session, new version note); no overlap. **Backup** (review fields of all 30 rows, mode 600): `/home/bitnami/ops/mi-go-backup-30-rows-20261019.csv`; the **old text of the 8** (rewritten in place, so it would be lost): `/home/bitnami/ops/mi-go-old-content-8-20261019.json`; plan with the 8 bill ids: `mi-go-plan-20261019.json`. Mac writes to the broker were 0 in the 5 minutes before I started.

## Step B: rewrite 8 (times UTC)
1. **One-row test, 02:18:** rejected artifact **465 (HB 6127)** with `.save()`; dry run for that one bill through the Mac relay (`POST http://10.0.0.8:8001/ddp-sync/v1/trigger/bill-artifact-generation`, `X-DDP-Environment: prod`, `artifact_types ["bill_changelog"]`, `include_org_research false`, `include_concept_statements false`, `limit null`, `retry_failed false`, `bill_candidates` = the one bill): `bills_considered 1`, would generate `bill_changelog`, nothing skipped, no failure; then the real run (`dry_run false`): HTTP 200, **33 s**, `success true`, generated `bill_changelog`, no failure. Verified: row 465 is `complete` + `approved`, the content changed, **`generated_at` unchanged (as you said)**, and its compared version changed.
2. **The other 7, 02:19 to 02:21:** rejected the other 7 ids with `.save()`; one 7-bill dry run (all 7 would generate only `bill_changelog`, none skipped or failed, `truncated false`); the real run: HTTP 200, **68.5 s**, `success true`, `bills_processed 7`, all generated, **0 failed** (per-summary 4.9 to 38.8 s).
3. **Verification of all 8 (02:22):** every row `complete` + `approved`; every one rewritten (content differs from the saved old text); **none** has a raw `{'...'}` object or is empty; `generated_at` unchanged on all.

**Compared pairs, before (the old side the summary named) and now (what the row's `compare_version` is):**
| artifact | bill | new version | BEFORE: compared from | NOW: compared from |
|---|---|---|---|---|
| 465 | HB 6127 | House Concurred Bill | House Enrolled Bill | As Passed by the House |
| 698 | HB 6126 | House Concurred Bill | House Enrolled Bill | As Passed by the Senate |
| 1077 | HB 6130 | House Concurred Bill | House Enrolled Bill | As Passed by the Senate |
| 1088 | HB 5109 | House Concurred Bill | House Enrolled Bill | As Passed by the Senate |
| 1457 | HB 5485 | Substitute (H-4) | House Introduced Bill | Substitute (S-2) - 4 |
| 1477 | HB 4703 | Substitute (H-2) | Substitute (H-1) - 2 | Substitute (S-1) - 2 |
| 12179 | HB 4678 | As Passed by the House | Substitute (S-1) - 2 | Substitute (S-1) - 6 |
| 213446 | HB 4135 | House Concurred Bill | House Enrolled Bill | As Passed by the Senate |

## Reading the 8 against their stored diffs (RDS `diff_from_previous_version`, PDF and HTML documents)
I read **HB 6127 and HB 6130 in full** and checked the others by comparing the removed and added text (a crude check, apostrophes and line wrapping add noise; I did not read the two large ones line by line). Results:
* **HB 6130: the new summary is WRONG.** It says `Sections Modified - None`, "The new version of this bill makes no substantive change." The diff (As Passed by the Senate to House Concurred Bill) **rewrites subsection (7) of the community district education trust fund section**: the old text said that after September 30, 2026 no money may be appropriated to or expended from the fund and any remaining money moves to the state school aid fund; the new text says the general fund shall reimburse the school aid fund for any shortfall while the community district is prohibited from levying a school operating tax. It also **drops the "This act is ordered to take immediate effect." sentence**, and the rest of the diff is line-wrapping plus redline-style duplicates ("Money Except as otherwise", "shall must"). The row is live and says "no substantive change".
* **HB 6127:** structure right (no sections added or removed; one modified funding provision, Section 16(4)(d)). One loose sentence: "restores a specific annual payment of $2,000,000.00 ... which had been removed in a previous version"; the diff shows the wording changing from a flat `$7,000,000.00` to `$2,000,000.00 ... to an amount, not to exceed $7,000,000.00`.
* **HB 6126:** consistent with its diff (the fund's payment changing from up to $7,000,000.00 to $2,000,000.00 within the same $7,000,000.00 limit).
* **HB 5109:** "no substantive change" fits wording tweaks (redline duplicates such as "The Except as otherwise", "upon on appropriation"), but the diff **also removes "This act is ordered to take immediate effect."**, which the summary does not mention.
* **HB 4703:** "no substantive change"; the diff is one cross-reference renumbered (`3406uu.` to `3406ss.`), borderline.
* **HB 4135:** "no substantive change"; the diff is line-wrapping plus a legal description losing "1" in "Range 1 East" and "Section 18" moving in one metes-and-bounds line (typo-level; not mentioned).
* **HB 5485** (38,029-character diff; summary lists a new definitions section of the dam safety part): consistent with a diff that adds about 3,100 words; not read line by line. **HB 4678** (summary lists three removed requirements for a qualified equipment renter; the diff removes about 100 words of that kind; the HTML document has no diff): consistent at the level I checked.

I changed nothing for HB 6130 (it is `complete` + `approved` and public). That is a call for Ramon and you: leave it, or have it rejected again until the wording/model is fixed.

## Step A: hide 22 (02:22:43 UTC)
All 22 ids in `MI_GO_hide_22.txt` were `complete` + `approved`, were already in the backup, and were rejected with `.save()`. **22 rows are now `complete` + `rejected`.** I triggered nothing for them. The 2 failed `pending_review` rows in `MI_HIDE_DECISION.txt` were left alone.

## State after
Michigan `bill_changelog`: **1,330 complete + approved**, **22 complete + rejected**, 1 failed + approved, 2 failed + pending_review (before: 1,352 complete + approved, 1 failed + approved, 2 failed + pending_review). Total artifacts 355,270; all rejected changelogs on production (every state) **1,015** (MA 290, UT 681, FL 22, MI 22). `/api/status/` 200; no web errors or 5xx in the 10 minutes after. Mac model service not touched beyond the two runs above; ddp-sync, the broker and everything else untouched.

## Still owed (not part of this)
The Monday 05:00 UTC FL and UT archive observation (which of the 22 hidden FL/UT rows regenerated, 2 of each read against their diffs) and any Massachusetts approvals, after those archives run.

## Not done
No other state; no WA or MI "changed diff" list (the operator said not needed); nothing restored or recomputed.
