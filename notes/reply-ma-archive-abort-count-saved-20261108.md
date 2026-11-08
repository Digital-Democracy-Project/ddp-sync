# Reply (read-only): what Massachusetts' aborted archive saved, earlier timeouts, and the `ma.imported` warning; **with a correction to my own status note** (reply to `2e6a2cb`). 2026-11-08, 18:55 UTC

Nothing was launched, retried, changed or tested against malegislature.gov: logs, S3 and RDS reads only.

## A correction first (my earlier note `226f16d` was wrong in two places)
I wrote that the archive stopped partway "because malegislature.gov stopped accepting connections ... for the last stretch" after 3,183 bills. I had read only the **end** of the log. Having read all of it:
- **The first timeout was on the archive's very first network fetch**, 12:39:39 UTC (`SD3039.pdf`); the first `ConnectTimeoutError` warning is at 12:41:52. There was no "stretch of working connections" before it.
- **"3,183 bills processed so far" does not mean 3,183 were fetched.** Those are bills the archive checked and skipped as already saved. The run made **exactly five network fetches in 80 minutes, and all five timed out.**

## 1. What run `ma-archive-439fe2eb2ec6` saved
- **Zero documents, zero bills.** In RDS **0 Massachusetts documents have `archived_at` on or after 2026-11-08 12:39 UTC** (the run's start). Its log has no saved, stored or uploaded line; the only network lines are the five GETs below.
- **Previous Massachusetts archive (Friday 11-06, run `ma-archive-1899514c6628`, 05:00 UTC, status `ok`, about 3.5 minutes):** `checked 11872`, `fetched 44`, `skipped 12113`, `archived 44`. RDS agrees: 44 documents have `archived_at` on 11-06. Weekly counts before that: 44 (10-25), 55 (10-18), 45 (10-11), 62 (10-09), 123 (09-27), 87 (09-18), 174 (09-10).
- **Nothing was removed:** all **12,157** Massachusetts documents have both `archive_location` and `archived_at`; none is unarchived. (An aborted run deletes nothing; the counts say none disappeared.)
- **Bills with no saved text:** RDS holds **11,907 Massachusetts bills; 11,799 have at least one archived document, so 108 have none.** I have **no snapshot of that number from last time**, so I cannot say whether it was also 108; because the aborted run saved nothing and removed nothing, I would expect it to be unchanged, but that is an expectation, not a measurement. (Those 108 are bills for which no document row exists yet, for example the bills behind the files that never download.)

## 2. Timeouts, and the pattern across runs
- **In the aborted run:** 25 `ConnectTimeoutError` warning lines in total, which are **five fetches of one file each** (six attempts of about 2.5 minutes, so about 16 minutes per file): `SD3039.pdf` at 12:39:39 (failed 12:55:32), `HD2304.pdf` (13:11), `SD3964.pdf` (13:27), `HD172.pdf` (13:43), `HD178.pdf` (13:59). Five failures in a row ends the run (`archive aborted: 5 consecutive connection failures`). **Before the final five in a row there were no other fetch attempts at all.**
- **The same five files, in the same order, in earlier runs:**
  - **Friday 10-16 (run `ma-archive-379deac557c6`): `failed`, 4,772 s, the same abort.** Same five URLs, 31 timeout warning lines, "3,197 bills processed so far" at the end. So **this has happened before, three weeks ago**, almost to the second (4,772 s against 4,773 s).
  - **Fridays 10-23 and 10-30 and 11-06: `ok`.** In those runs the same files (`SD3039`, `HD2304`, `SD3964`, `HD172`, `HD178`, and 15 more, 20 in all) were reported `failed to fetch` too, but with **`404 while retrieving https://malegislature.gov/Bills/194/SD3039.pdf`** (the server answered "not found" quickly) and **no timeout lines** (0 in 10-23, 10-30, 11-06). The run tolerates a 404 and continued.
- So the files themselves do not exist on the site (404 on the good days); **on 10-16 and 11-08 the server did not answer for them at all** (connect timeout) instead of returning 404, and that turns a harmless 404 into five long timeouts and an abort. **I did not find the streams for 10-02 and 10-09** (I looked only at 05:00 to 05:04 UTC on those Fridays; the log group's oldest stream I can list starts 09-26), so I cannot say if it happened earlier.
- **Hypothesis (not tested, and I did not touch the site):** the site (or something in front of it) sometimes drops connections for requests to non-existent bill PDFs, or only at certain times. What would tell: whether the archive can skip a file after the first timeout instead of counting it toward five in a row, or probing one of those URLs on a good day and a bad day (not authorized, and not done).

## 3. The `ma.imported` warning
- **It is not new to this run and not specific to Massachusetts:** `no usable filing figures for a successful run -- expected marker missing, unreadable, or unparsed expected_path=/opt/ddp-open-states/logs/last-run/<st>.imported` was logged **once for each of the nine states `az la ma mi nc ne ut va` at 13:59:36 UTC (the end of the Sunday secondary batch) and once for `pa` at 11:06:25**, nothing else on 11-08. It comes from `openstates_scrape.py` line 1559.
- **That directory is empty on this host** (`/opt/ddp-open-states/logs/last-run/` has no files); the cloud path keeps its markers in S3 (the `ne.ts`/`la.ts`/`pa.ts` files). **Hypothesis:** the warning fires whenever a cloud scrape finishes because it looks for a local marker file the cloud path never writes. **I cannot say whether earlier Sundays logged it**: the ddp-sync log I can read starts on 11-04, and there was no earlier batch since then.

## Not done
No archive or scrape launch or relaunch; no schedule or config change; no request to malegislature.gov. Friday 05:00 UTC's regular archive is the retry, as Ramon said; I will read its result.
