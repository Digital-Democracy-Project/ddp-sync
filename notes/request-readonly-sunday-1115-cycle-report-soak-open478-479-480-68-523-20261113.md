# Request (read-only): one cycle report for Sunday 2026-11-15's runs, for the soak (OPEN-479, OPEN-480, OPEN-478, OPEN-68, OPEN-523), 2026-11-13 (Mac side)

## What I need from you
After Sunday's 02:00 UTC batch has finished (and Pennsylvania's 04:00 UTC nightly of 11-15), **one note** (`report-cycle-20261115.md`) answering, per state, read-only:

1. **Nebraska and Louisiana** (first weekly cycle after the 11-09 switch-on, so soak cycle 1 of 2): the job's `status`, `duration_seconds`, `failed`, `archived`; bills and vote events imported; whether the archive ran and LegBot was triggered (and what it logged); any ERROR or retry.
2. **Massachusetts:** the same, plus **which task-definition revision the task ran on** (I expect **rev 60 = image v57**, which carries the OPEN-474 vote-name fix) and whether it finished inside its 24 h limit.
3. **Alabama** (its first scrape since June; OPEN-523 step 4): the same, plus the revision (rev 60 expected) and the vote-event count it imported (it was 0 before; OPEN-524/527).
4. **Pennsylvania:** the nightlies of 11-13, 11-14 and 11-15 in one line each (status, duration, failed), so its first soak week is on record.
5. Anything else in Sunday's batch that failed or retried (any state), one line each.

**Clean** means: status completed, failed=0, archive on its attempt, no ERROR line for that state. Say "clean" or "not clean, because ..." per state; that is the soak note the plan (§5 Stage 6, v1 addition b) asks for.

## Why
Pennsylvania, Nebraska and Louisiana are switched on and in their two-clean-weekly-cycles soak; Sunday is the first full cycle for NE and LA. Massachusetts and Alabama need Sunday's data before their next checks (OPEN-68 step 6, OPEN-523 steps 4 to 8).

## Not authorized
Nothing that writes: no scrape, rerun, retry, archive, people load, recompute, restart, deploy, config, manifest or broker change, no `status: live`, no LegBot run. If a run failed, report it; do not re-run it.

## What I check on the Mac side afterwards
Stage 3 gates for MA and AL on the Mac replica (`quality_check.py --structural`), AL's bill stages (`validate_bill_stages --jurisdictions al`), MA's empty roll calls (OPEN-176), and the counts against your note.
