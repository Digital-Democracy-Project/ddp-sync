# Report: Utah 2026 LegBot run, `bill_changelog` data quality (redline "Comparison" documents are being treated as bill versions)

From: prod agent (EC2 broker host), 2026-10-13 UTC. Operator asked for this write-up. **Nothing was changed.** The Utah run is still in flight (see "State"). Read-only: broker database, api-v3 (RDS), and the artifact content.

## Summary
Utah publishes, besides real bill texts (Introduced, Substitute #N, Enrolled, "House/Senate Amendment", "Amended Excerpts ..."), **redline comparison PDFs** ("Comparison to Original Bill", "Comparison to Sub #N"). They arrive as bill versions, and the changelog pipeline treats them as versions in the lineage. The result: only **9%** of Utah's changelogs compare two real texts; most compare a text against a redline, or a redline against a redline. The changelogs read well, but a large share describes the wrong transition or is built on a redline whose extracted text is incomplete. This also explains much of the run's slowness (see "Speed").

## Sample and scope (what I did and did not check)
- Population: the 1,543 `bill_changelog` artifacts written by this run for Utah 2026 (as of 2026-10-13 ~20:30 UTC; 1,533 of the earlier 1,543 were `complete`; the counts below use the window that was current when each query ran).
- I **read about 8 changelogs closely** and **compared the raw version texts (`raw_text` from api-v3 `?include=versions`) for 3 bills (SB158, HB8, HB438) and 6 "no change" cases**. I did not verify changelogs against the source for the other ~1,500, and I did not look at the other 8 artifact types.
- Everything under "Observed" was seen in the data. Anything under "My reading" is inference.

## Observed
**1. Pair types (all 1,543 Utah changelogs)** (from the changelog's compare version and the bill version, by version note):
| from -> to | count |
|---|---|
| Comparison -> Substitute | 374 |
| Substitute -> Comparison | 259 |
| Introduced -> Comparison | 244 |
| Amended Excerpts -> Amended Excerpts | 189 |
| Substitute -> Enrolled | 93 |
| Amended Excerpts -> Enrolled | 92 |
| House Amendment -> Substitute | 86 |
| Substitute -> Amended Excerpts | 76 |
| Senate Amendment -> Substitute | 43 |
| Introduced -> Enrolled | 41 |
| House Amendment -> Amended Excerpts | 28 |
| Substitute -> Substitute | 6 |

Real text -> real text (Introduced / Substitute / Enrolled on both sides): **140 of 1,543 (9%)**. About **57% involve a "Comparison" document** on at least one side (876 of 1,530 in an earlier count) and 279 involve "Amended Excerpts". Consecutive "Substitute -> Substitute" changelogs, the transitions a reader most wants, number **6**.

**2. HB438 (5 substitutes): the real transitions are mostly missing.** Its 10 changelogs are: Comparison to Original Bill -> Substitute #1; Comparison to Sub #1 -> Substitute #2; Comparison to Sub #3 -> Substitute #3; Comparison to Sub #4 -> Substitute #5; Introduced -> Comparison to Original Bill; Substitute #1 -> Comparison to Sub #1; Substitute #2 -> Comparison to Sub #2; Comparison to Sub #2 -> Comparison to Sub #3; Substitute #4 -> Comparison to Sub #4; and **one** real one, Substitute #3 -> Substitute #4. Missing: Introduced -> Sub #1, Sub #1 -> Sub #2, Sub #2 -> Sub #3, Sub #4 -> Sub #5. Reading order appears to put "Comparison to Sub #N" right after "Substitute #N" in the lineage (`version_sort_key`/`note_stage` in api-v3 `bills.py`; I did not read that code in detail).

**3. Each "Comparison to ..." note groups several different PDFs.** api-v3 returns, for HB438, five PDFs all noted "Comparison to Original Bill" (`CP HB0438 To HB0438S01.pdf` through `...S05.pdf`), four under "Comparison to Sub #1", three under "Comparison to Sub #2", and so on. Only the **first** link of each note carries `raw_text` (e.g. "Comparison to Sub #2" is `CP HB0438S02 To HB0438S04.pdf`, "Comparison to Sub #3" is `CP HB0438S03 To HB0438S04.pdf`). The note label therefore does not identify which transition the text is, and the others have `raw_text` None.

**4. Extracted redline text carries no markup.** In HB438's "Comparison to Sub #3" text the old phrase ("every hour") and the new one ("three hours") both appear, with nothing marking which is deleted and which is inserted. A model given two such texts, or a text and a redline, cannot reliably tell added from removed. In one HB438 changelog (Comparison to Sub #2 -> Comparison to Sub #3), the claim "notices to minors every hour (changed to every three hours)" is listed under **Sections Removed**; the underlying change is real (Substitute #2 has "every hour" once, Substitute #3 "three hours" once) but it is a modification and the labelled transition is not a real one.

**5. HB8 (a ~350k-character appropriation bill): the changelog reads the redline's missing text as removals.** Version `raw_text` lengths: Introduced 361,506; Substitute #1 347,953; Enrolled 347,953 (identical to Sub #1); "Comparison to Original Bill" PDF **164,873**. "Tooele Technical College" appears 6 times in Introduced, 6 in Sub #1/Enrolled and **once** in the comparison PDF's text. Its only changelog (Introduced -> Comparison to Original Bill, model `claude`) says "all of the fiscal year 2027 appropriation items ... were removed from the visible text in this version". Fiscal-2027 text is in Introduced (9 occurrences of "2027") and in Substitute #1 (10), so I read that claim as an artifact of comparing a full text with a partial redline extract, not a real removal. HB8 has 4 versions but only this one changelog so far; I did not look at why.

**6. Model: 12 of the 1,533 changelogs were written by `claude`, not `mlx`.** They are among the longest (1,456 to 6,762 characters). I did not see why a fallback happened (my guess: large-document handling); the 1,521 others are `mlx`. One failed: "model response was not valid JSON".

**7. All 1,533 changelogs are `review_status = approved`.** Nothing in the current flow caught items 2 to 5.

**8. "No change" results are mostly correct.** 44 changelogs have "None" in all three sections; 41 of those do not involve a comparison document (29 Substitute -> Enrolled, 10 Introduced -> Enrolled, 2 Substitute -> Amended). I diffed 7 against `raw_text`: SB158, HB414, SB89, HB394, SB132, HB171 differ **only in the timestamp footer line** (e.g. `1-19-26 1:46 PM` vs `3-6-26 8:21 AM`), and SB90 differs only by punctuation (`Section 1,` vs `Section 1.`). So these are right. An earlier suspicion of mine about SB158 is withdrawn. They are low-value artifacts, but not wrong.

## Speed (why this run is slow)
Utah bills average **12.8 artifacts each** (Arizona: 8.7) because `bill_changelog` is produced per version pair: 4.8 per bill in Utah (1,329 over 277 bills at one count) versus 0.9 in Arizona. Median time from a bill's first to last artifact: 137 s (Arizona 31 s). Utah runs about 25 to 35 bills an hour (Arizona about 100). Part of this is per-artifact time (about 9 to 16 s each now, about 4 s in Arizona); I cannot see the Mac's backend load, so I cannot say if something else contributes.

## State
The Utah 2026 run (no limit, all nine artifact types, concept statements on) started 2026-10-13 04:52 UTC and is still running; 302 of 1,016 bills had artifacts at 20:29 UTC. Estimated finish 2026-10-14 ~18:00 to 23:00 UTC at the current pace. The other 8 artifact types (summary, pros/cons, frames, orgs, impact, topics) are not affected by this finding as far as I looked, but I have not read them.

## Questions for you (your call, not mine)
1. Should "Comparison to ..." and possibly "Amended Excerpts ..." versions be excluded from the changelog lineage for Utah (and any other jurisdiction that publishes redlines), so changelogs run Introduced -> Sub #1 -> ... -> Enrolled between real texts only? That would also cut the Utah run time substantially.
2. Is it worth letting this run finish as-is (the 140 real-pair changelogs are fine; the redline ones are noisy but not obviously harmful) or should I stop it? I have not stopped anything; stopping the client may not stop the Mac-side work, so I would only do it on the operator's say-so.
3. If the changelogs get regenerated, which artifacts should be superseded or deleted? I would not delete anything without a request and the usual checks.
4. Do you want a re-count of the pair types for other jurisdictions (MI, WA, US)? I only analyzed Utah.
