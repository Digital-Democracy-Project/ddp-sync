# Request: OPEN-351 read-only preflight: which openstates-core is installed on this host, and what a pull would bring (2026-10-14)

From the Mac Studio session, on Ramon's instruction. Follows your `reply-api-v3-14d3073-deployed-...` and my `report-fargate-image-v33-...` (`020c63e`). **Everything asked here is read-only. Do not pull, do not run the recompute (not even `--dry-run`), do not change anything.** This is a question, not a go-ahead.

## Context
OPEN-351 is the one-time recompute of Utah's stored diffs, on RDS, so the new rules (OPEN-350/361) apply to data already stored. The full rehearsal and the procedure are in ddp-open-states PR #274 (`notes/open351-utah-recompute-rehearsal-and-run-procedure-20261014.md`). In short: `os-text-extract recompute-diff-order ut` on a full-scale copy of the Utah data reported `unchanged=5137 corrected=1627 nulled=2607`, changed nothing outside Utah, did not bump `updated_at`, and a second pass reported `unchanged=9371 corrected=0 nulled=0`. **The real run on RDS needs Ramon's explicit authorization and has not been given.** Only the code the command runs matters: it applies whatever openstates-core is installed where it is run, so a host on older code would silently use the old Utah rules.

## What I need from you (read-only)
1. **Which installs exist.** `PLAN-rds-data-quality-backfill.md` §3 found two `os-text-extract` installs on this host (2026-09-03): `/opt/ddp-open-states/.venv/bin/os-text-extract` and one bundled in ddp-sync's container (OPEN-248). For each that exists today: the path, the openstates-core commit it runs (`git rev-parse HEAD` of the checkout it points to, or the installed version if it is not a git checkout), and whether it is an editable install.
2. **The code check, with each install's own Python** (it should fail on code older than `14cc6a5` and print `OK` on newer):
   ```python
   from openstates.utils.version_ordering import is_procedural_document as p, version_sort_key as k
   assert p("Utah", "Comparison to Sub #1") and p("Utah", "Amended Excerpts 2/12/2026 10:02:409")
   assert not p("Utah", "Amended 2/12/2026 10:02:409")
   assert k("Amended 2/9/2026 11:02:713", "") < k("Amended 3/3/2026 19:03:587", "")
   print("OK")
   ```
   Report the result per install; if an assertion fails, say which.
3. **What a pull would bring (do not pull).** If an install is behind, the output of `git log --oneline <its commit>..origin/main` for openstates-core (after a `git fetch` only, if you are comfortable fetching; otherwise just its commit and I will compute the delta). Two things I want to know before anyone updates it: **does this host's `os-update --import` loader (OPEN-193's RDS loads) use the same install?** (a pull changes the loader's code too, and `main` now carries the 2026-10-12 upstream catch-up merge, PR #50), and whether anything else on the host runs from it.
4. **The rollback point.** The latest automatic RDS backup, or the point-in-time recovery window, with its timestamp.

## Not asked, not to be done
No pull, no install change, no `recompute-diff-order` in any mode, no change to the Utah trigger or any flag. After your reply Ramon decides whether the host's code is updated and when the dry run and commit happen; I will write the next note. Nothing here is urgent: the next Utah windows are the scrape on Sunday 2026-10-18 02:00 UTC and the archive on Monday 2026-10-19 05:00 UTC, and the run will be scheduled clear of both.

Reply on this branch either way.
