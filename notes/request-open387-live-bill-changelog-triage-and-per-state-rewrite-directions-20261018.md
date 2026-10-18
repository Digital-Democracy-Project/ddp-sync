# Request (2026-10-18): OPEN-387 triage of the live bill_changelog summaries, and per-state directions for any rewrite. Read-only checks now; NO rejects, NO regeneration until the operator says go for that state

From: Claude (Mac side, ddp-agents work). To: the prod agent. Jira: OPEN-387 (the triage), OPEN-386 (the umbrella), per-state subtasks OPEN-383 and OPEN-388 to OPEN-396.

**The operator has NOT given a go for any change on the production broker.** This note hands you (1) the triage result, (2) the work lists, (3) the procedure to follow state by state once the operator says go, and (4) a small read-only check to do now. Everything below the line "Per-state directions" is conditional on a later go from the operator, per state.

## What the triage is, and what it is not
* **Source:** the dev ddp-broker, which the operator says is a snapshot of production from the evening of 2026-10-17. 12,889 `bill_changelog` rows. I excluded 90 rows the AGENTS-160 study created in dev (VA 62, NC 27, MI 1) and restored the old text of 74 rows the study overwrote in place, so the figures describe production as it was. **Production itself was not read.** Please verify (item "Do now" below) before trusting any id.
* **Method (read-only):** for every row, rebuild the pair ddp-sync would use today from the Mac's api-v3 (the same transition rule as ddp-sync's `get_archived_version_transitions`: the new version's `diff_from_previous_version` against its baseline version), then run the AGENTS-160 checks (`scripts/changelog_checks.py`, `changelog_missing_additions.py` in ddp-agents) on the stored summary text.
* **Precision of the checks is low.** In the AGENTS-160 study only about 3 of 42 flags on new summaries were real defects. So I do **not** recommend rewriting by flag alone. The lists below are tiered by how sure we are.
* **Not a quality judgment on unflagged rows.** I read a random 14 unflagged summaries across seven states: no clear error in 13, one mild.

## Result (live rows in the snapshot, after the exclusions)
| state | rows | REWRITE NOW (sure) | OPTIONAL (low harm) | HOLD (large diff) | HIDE-or-keep decision (no diff today) | CHECK (version missing in api-v3) |
| --- | --- | --- | --- | --- | --- | --- |
| AZ | 1,972 | 1,497 | 10 | 27 | 134 | 0 |
| US | 3,245 | 226 | 0 | 60 | 0 | 3 |
| UT | 2,701 | 11 (already hidden) | 121 | 153 | 670 (already hidden) | 0 |
| FL | 1,229 | 11 (already hidden) | 44 | 82 | 11 (already hidden) | 0 |
| MI | 1,282 | 8 | 23 | 78 | 11 | 13 |
| WA | 1,867 | 0 | 1 | 74 | 2 | 0 |
| VA | 213 | 0 | 0 | 7 | 0 | 0 |
| MA | 290 (all hidden) | 0 | 11 | 8 | 0 | 0 |
| **all** | **12,799** | **1,753** | **210** | **489** | **828** | **16** |

What the tiers mean:
* **REWRITE NOW:** (A) the stored summary compares a different pair of versions than today's stored diff (the summary header says "From X To Y" and today's baseline for Y is not X): AZ 1,477, MI 8, UT 9; or (B) the row never completed (status `failed` or `pending`) **and today's api-v3 has a diff for it** (so it can be generated): AZ 60 (40 of them are also wrong-pair), US 5, FL 11; or (C, US only) a self-contradicting summary on a cover-sheet-only version (lists invented section changes while its paragraph says "no substantive change"): US 221. A and C were read by hand: US 8 of 8 sampled were real; Arizona's wrong pairs were confirmed (for example SB 1064's summary compares "Introduced Version to Senate Engrossed Version" while today's pair is "SENATE - Appropriations, Transportation and Technology to Senate Engrossed Version").
* **OPTIONAL:** contradictions in UT, FL, MI, AZ and Utah footer-timestamp versions whose summary lists the timestamp as a change. Low harm (the paragraph still says "no change"). The operator decides; do not include them in a batch unless told.
* **HOLD (large diff):** the diff is 25,000 characters or more and the summary skips a phrase the diff repeats. This is a known weak spot (AGENTS-160: 55% of large diffs vs 3% of medium and 0% of small). The fix is being researched in AGENTS-162. **Do not rewrite these now**; a rewrite today would give the same result.
* **HIDE-or-keep:** the summary (or failed row) describes a version that has no diff in today's api-v3, so ddp-sync cannot regenerate it. Precedent: Utah (SYNC-111) rejected these. AZ 134 (129 approved plus 5 failed), MI 11 (9 approved plus 2 failed), WA 2 are live or pending and undecided; UT 670 and FL 11 are already rejected. **Do not touch.**
* **CHECK:** MI 13 summaries whose version note (for example "House Enrolled Bill") no longer exists in api-v3, and US 3 whose note is shared by two versions. Report what you see; do not change.

## Do now (read-only, no go needed)
1. **Verify the ids against production.** For 20 ids per state from `AZ_REWRITE_NOW.txt`, `US_REWRITE_NOW.txt`, `MI_REWRITE_NOW.txt` (and 10 from the others' CSVs), confirm on the production broker that the artifact id still maps to the same (state, session, gov_id, new version note) as the CSV columns, and report the production counts per state and (status, review_status) for `bill_changelog`, to compare with the snapshot: AZ 1,972 (1,907 approved + 65 failed), US 3,245, UT 2,701 (2,020 approved, 681 rejected), FL 1,229 (1,207 approved + 22 rejected), MI 1,283, WA 1,867, VA 213, MA 290 (all rejected), NC 0. If the ids differ, say so; the CSVs also carry (state, session, gov_id, new version note) so the lists can be rebuilt from those.
2. **Tell the operator** whether you can reach the Mac's ddp-sync (the path the archive hook uses, `mac_ddp_sync_base_url`, with `X-DDP-Environment: prod`) from the EC2 host, and with which credential, so the trigger step below can be run by you or by the operator from the Mac.
3. **MI and WA, read-only:** OPEN-388 and OPEN-389 expect about 40 and 34 summaries whose stored diff changed (OPEN-380/381 recomputes). The snapshot cannot see a diff that changed within the same pair. If you can, list the MI and WA bill versions whose archived diff was recomputed after their summary's `generated_at` (RDS or archive records), as (state, session, gov_id, new version note).

## Per-state directions (ONLY after the operator's go for that state)

### Rules for every state
* **Wording freeze first.** Nothing starts until the operator confirms the changelog prompt is final (AGENTS-159's three small fixes may still change it) and the Mac's CAMS runs it. I will add a short note with the commit when that is true.
* **One state at a time; one batch at a time.** Production auto-approves (`BILL_ARTIFACT_REQUIRE_REVIEW=false`) and a rejected row is hidden from the site until it is regenerated, so reject only the batch you are about to run and run it at once. Batches of 50 to 100 bills (about 50 to 150 rows).
* **Back up before changing anything:** write the review fields of the batch's rows (id, status, review_status, reviewed_by, reviewed_at) to a CSV on the host and keep it.
* **One-row test first on production:** reject one row from the batch, run a one-bill trigger (below), confirm the row is rewritten in place with new text (note: **`generated_at` does not change on an in-place rewrite**; compare content) and is `complete` and `approved`, then proceed.
* **Reject, never delete.** Reject with the admin action (`review_status = rejected`), as for Utah. ddp-sync treats a rejected row as missing (SYNC-115) and ddp-broker-py overwrites it in place on the rewrite. A row that is only `failed` counts as present, so those must be rejected too.
* **Trigger (the Mac runs LegBot):** `POST /ddp-sync/v1/trigger/bill-artifact-generation` on the Mac's ddp-sync with headers `Authorization: Bearer <ddp-sync key>`, `X-DDP-Environment: prod`, `Content-Type: application/json`, and a body with `jurisdiction_iso2`, `session_code`, `artifact_types: ["bill_changelog"]`, `include_org_research: false`, `include_concept_statements: false`, `limit: null`, `retry_failed: false`, `bill_candidates: [{"gov_id": ..., "bill_openstates_id": <bare UUID>}]` for the batch's bills, **`dry_run: true` first**, then `false`. Without `X-DDP-Environment` the Mac's ddp-sync falls back to its default broker (which is production), so always send it and read the dry run before the real run. A real run is a long synchronous call (about 20 to 45 seconds per summary).
* **Never** restart ddp-sync or run `cams reload` on the Mac while a run is in flight (it strands the tasks). The operator checks `cams status` is idle before each batch. Do not run a rewrite and NC's first-time batch at once.
* **Verify each batch:** every rejected row is `complete` and `approved` again; read about 10 regenerated summaries against their diffs (`GET /api/bill-versions/history/?jurisdiction=&session=&gov_id=` returns the changelog, raw text and diff per version). Summaries of cover-sheet-only or reflow-only versions should now say "no substantive change" with empty lists. Report ids still rejected or failed.
* **Stop conditions:** a row count that does not match the list, an id that maps to a different bill or version than the CSV, a regenerated row that is still `failed`, any summary that shows a raw `{'...'}` object or is empty, or anything unexpected. Stop, change nothing more, report.

### AZ (OPEN-383): REWRITE 1,497 (`AZ_REWRITE_NOW.txt`)
* 1,477 summaries describe the wrong pair (792 committee or floor-amendment versions, 681 Engrossed versions, 4 Chaptered), plus 20 more failed rows that have a diff today (65 failed rows in all: 40 are also wrong-pair, 5 have no diff and are in the hide list). Session `57th-2nd-regular`.
* **Precondition: Arizona's stored diffs must be final** (the ticket's own gate; confirm with the operator before the first batch). If a recompute is still coming, rewriting now writes them twice.
* Do the rewrite in about 15 to 25 batches. Expect roughly 10 to 19 hours of model time in total.
* **Leave alone:** `AZ_HIDE_DECISION.txt` (134: 129 approved plus 5 failed): no diff today; the operator decides. `AZ_HOLD_LARGE.txt` (27), `AZ_OPTIONAL.txt` (10).

### US (OPEN-394): REWRITE 226 (`US_REWRITE_NOW.txt`)
* 221 self-contradicting summaries on cover-sheet-only versions (Introduced to Reported, Reported to Engrossed: "Calendar No.", "Report No.", dates, "IB") that list invented section changes, and 5 failed rows that have a diff today (a sixth failed row is one of the 3 CHECK rows). Congress is session `119`. After the rewrite these should read "no substantive change".
* The other 3,000-odd US summaries were not flagged for a wrong pair or contradiction; 167 "quiet" flags and 38 "unsourced number" flags are noise (the summary correctly says no change on a cover-sheet-only diff, or a figure was rewritten as "$2.7 billion"). Do not rewrite them.
* `US_F_check_api_v3_version.csv` (3): two versions share a note ("Engrossed Amendment Senate", "Referred in Senate"); report which versions and whether both have a row; do not change.
* `US_HOLD_LARGE.txt` (60): hold.

### UT (OPEN-395): nothing to reject
* 681 rows are already rejected (hidden): 670 (669 complete plus 1 failed) have no diff today and stay hidden (Utah precedent); the other 11 are listed below. 11 rejected rows (`UT_REWRITE_NOW.txt`: 9 whose old side was a "Comparison to Sub #N" document, which is not today's baseline, plus 2 "Enrolled" rows on HB 68 and HB 475 with a valid pair) have a valid diff today and **will be regenerated automatically by the next Utah trigger and go live at once**. Do not trigger Utah (and tell the operator if the weekly archive hook would) until the operator has said go; there is no rejecting to do.
* `UT_OPTIONAL.txt` (121: 49 contradictions and 72 footer-timestamp-listed-as-change): optional, operator's call. `UT_HOLD_LARGE.txt` (153): hold.

### FL (OPEN-392): nothing to reject
* 22 rows are `pending` and already rejected: 11 have a diff today (`FL_REWRITE_NOW.txt`) and regenerate on the next Florida trigger (live at once); 11 have no diff (`FL_HIDE_DECISION.txt`) and stay as they are. The 1,207 approved Florida summaries had no wrong pair; the 44 contradictions (`FL_OPTIONAL.txt`, mostly "A bill to be entitled" lines or tiny items listed on format-only versions) are low harm and optional.
* `FL_HOLD_LARGE.txt` (82): hold.

### MI (OPEN-389)
* REWRITE 8 (`MI_REWRITE_NOW.txt`): wrong pair (for example HB 4135, HB 6127, HB 6126: the summary compares "House Enrolled Bill" to "House Concurred Bill", but today's baseline for "House Concurred Bill" is a different version). Small; batch them together. The 2 failed MI rows have no diff today (in the hide list).
* **CHECK (13):** `MI_F_check_api_v3_version.csv`: the NEW version note the summary is for ("House Enrolled Bill" or "Senate Enrolled Bill", for example on HB 4135, HB 5482, SB 569, HB 6127) no longer exists in api-v3. Report whether these bills' versions were renamed (for example to "Public Act") and whether a summary for the new name already exists; do not change.
* HIDE decision 11 (`MI_HIDE_DECISION.txt`): leave. OPTIONAL 23, HOLD 78: as above. Do item 3 of "Do now" for the "about 34 changed diffs".

### WA (OPEN-388)
* No wrong pair, no failed row, no contradiction: **nothing to rewrite on the evidence of the snapshot.** The "about 40 changed diffs" cannot be seen here: do item 3 of "Do now" and send the list; those become the WA batch. HIDE decision 2, HOLD 74: leave.

### VA (OPEN-393)
* Nothing to rewrite: 213 compared cleanly, no wrong pair, no failed row; 7 large-diff flags are HOLD. The "PDF dropped line" issue (OPEN-375) is separate and unchanged.

### MA (OPEN-396)
* All 290 rows are hidden (rejected, never deleted). Each has a valid diff today, so the next Massachusetts trigger would regenerate all of them and they would go live at once. **Do not trigger Massachusetts** until the operator decides (OPEN-396).

### NC (OPEN-390) and AL (OPEN-391)
* NC: no summaries exist yet (about 3,550 versions get one for the first time). No action now; never run it at the same time as a rewrite. AL: no rows in this snapshot; the Mac's ddp-sync skips Alabama (`LEGBOT_RDS_REPLICA_JURISDICTION_ALLOWLIST` does not include `AL`, seen as `jurisdiction_not_on_rds_replica_allowlist: AL`); adding it is the operator's decision. No action.

## Report back on this branch (one note)
The "Do now" results (production counts, the id spot-check outcome, the Mac relay answer, the MI/WA changed-diff lists). Nothing else until a go.

## Files
`notes/open387-triage-20261018/`: `summary.json`; per state `<STATE>_REWRITE_NOW.txt`, `_OPTIONAL.txt`, `_HOLD_LARGE.txt`, `_HIDE_DECISION.txt` (artifact ids, one per line); and `<STATE>_<A..G>_*.csv` with artifact_id, state, session, gov_id, new_version_note, summary_compares_from, review_status, status, diff_chars_today, flags. Ids come from the dev snapshot; verify before use.
