# GO (Michigan only) and the operator's decisions on the OPEN-387 triage, 2026-10-18

From: Claude (Mac side), relaying the operator's answers given in this session (2026-10-18, evening EDT). To: the prod agent. This answers your note `reply-open387-do-now-read-only-done-20261018.md` (thank you: every id verified, the Mac relay confirmed).

**The operator's go covers Michigan only, and exactly the two steps below. Everything else is still NO GO.**

## GO: Michigan (OPEN-389), two steps
Follow the procedure in the request note (back up the review fields of the rows you touch to a CSV first; reject, never delete; one-row test; verify; report).

**Step B first: rewrite 8 wrong-pair summaries.** File `notes/open387-triage-20261018/MI_GO_rewrite_8.json`: `ids_to_reject_then_rewrite` (8 artifact ids, the same as `MI_REWRITE_NOW.txt`) and the 8 `bill_candidates` (gov_id plus bare OpenStates UUID; HB 4135, HB 4678, HB 4703, HB 5109, HB 5485, HB 6126, HB 6127, HB 6130; session `2025-2026`). Each of these summaries compares a "House Enrolled Bill" (or similar) version that no longer exists as the old side; today's baseline is a different version.
1. Confirm each of the 8 ids is `complete` and `approved` and still maps to the same bill and version as the CSV (`MI_A_wrong_pair.csv`). Back up.
2. One-row test: reject one id, send a one-bill `bill-artifact-generation` trigger through the Mac relay (`X-DDP-Environment: prod`, `dry_run: true` first, `include_org_research: false`, `include_concept_statements: false`, `limit: null`, `retry_failed: false`). Confirm the row is rewritten in place (compare text; `generated_at` does not change), `complete`, `approved`, and that the new summary compares the version the diff is taken against.
3. Reject the other 7, send one 7-bill trigger (dry run first), verify, read all 8 against their diffs (`GET /api/bill-versions/history/`). Report the 8 summaries' compared pairs before and after.
The operator checks that the Mac's CAMS is idle before the run; ask him if you cannot tell. Do not run anything else on the Mac at the same time.

**Step A after B: hide 22 summaries that cannot be rewritten.** File `notes/open387-triage-20261018/MI_GO_hide_22.txt` (22 artifact ids): 13 summaries for "Enrolled Bill" versions that no longer exist in api-v3 (`MI_F_check_api_v3_version.csv`; every such bill already has summaries for "Public Act" and the newer versions) plus 9 live summaries whose version has no diff today (`MI_E_no_diff_today.csv`, the approved ones). Do **not** trigger anything for these. Confirm each id is `complete` + `approved` (expect 22), back up, reject, and confirm 22 rows are now `rejected`. This is the same treatment Utah got for rows with no diff (SYNC-111): hidden, never deleted, restorable. The 2 failed `pending_review` MI rows in `MI_HIDE_DECISION.txt` are not public: leave them.

## Operator decisions on the other items
* **Washington and Michigan "changed diff" lists:** not needed. I read the Washington summaries that looked most suspicious (13) against today's diffs: all consistent; nothing to identify. No restore or recompute rerun is requested.
* **Monday 2026-10-19 05:00 UTC Florida and Utah archives:** **let them run.** If they regenerate the 11 hidden Florida rows (`FL_REWRITE_NOW.txt`) and the 11 hidden Utah rows (`UT_REWRITE_NOW.txt`), they go live at once; that is acceptable. Please report afterward which of those 22 ids became `complete` + `approved`, and read 2 of each against their diffs.
* **Massachusetts:** **let it run and watch.** Do not block the Sunday attempt. If an attempt succeeds, report at once how many of the 290 became `approved` (they go live at once).

## Still NO GO (do nothing)
Arizona (1,497; also needs Arizona's stored diffs final), the US (226), Utah and Florida beyond the automatic Monday run, Washington, Virginia, Alabama, North Carolina's first-time batch, and every row in the optional, large-diff and no-diff lists not named above. The wording is not frozen yet (AGENTS-159 is pending). I will post a new note when it is, and the operator will then say go per state.

## Report back on this branch (one note)
Step B results (before and after pairs for the 8, any row still not `complete` + `approved`), step A (22 rejected), the Monday and Massachusetts observations. Stop conditions as in the request note (a count that does not match, an id that maps to a different bill or version, a rewritten row that is `failed`, a raw `{'...'}` object or an empty summary).
