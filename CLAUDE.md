# CLAUDE.md

Instructions for Claude Code sessions working in this repository.

## ⚠️ WEBFLOW IS BEING DEPRECATED SOON ⚠️

**DO NOT design, build, or extend any new mechanism around Webflow** (the CMS sync
jobs, `webflow_id`-keyed flows, `bill_version.py`'s `check_and_reingest_version`/
Flow-2 write path, etc.). It is on its way out. Any new trigger, pipeline, or
feature — including bill-version-change detection, changelog generation, or
anything else that might naturally reach for the existing Webflow-driven poll job
as a hook point — must be built on the newer, Webflow-independent reach instead
(`/explore`'s own OpenStates-direct path, `session_pipeline_runner.py`,
`ensure_bill_exists`, etc.). Confirmed directly by Ramon, 2026-08-21 (see SYNC
session history around that date for the context this came up in — a discussion
of how to auto-trigger `bill_changelog` generation on a new scraped bill version).

## Dev/prod checkout discipline

`~/Developer/repos/ddp-sync` is **production** — the `com.ddp.ddp-sync` LaunchDaemon
(`/Library/LaunchDaemons/com.ddp.ddp-sync.plist`) runs `scripts/start-ddp-sync.sh` directly out
of that checkout, `RunAtLoad`+`KeepAlive`, with no separate dev instance. It can also carry real
in-progress uncommitted work on a feature branch at any time (confirmed 2026-08-02: it was
sitting on a feature branch with an uncommitted diff) — treat it the same way as
`ddp-open-states`'s production checkout: **do not edit files or switch branches there.**
Read-only operations (checking `git log`/`git status`, reading logs, checking which branch is
live) are fine.

**`~/Developer/repos/ddp-sync-dev`** is the isolated checkout for actual code changes — a plain
`git clone` of the same remote, created 2026-08-02 (OPEN-22) precisely because no such split
existed yet and the production checkout couldn't safely be used for development. Do all edits,
and run tests, there instead.

**Running tests:** this clone has no venv of its own. Reuse the production checkout's
(`~/Developer/repos/ddp-sync/.venv` — Python 3.12, pytest + ruff already installed) but override
`PYTHONPATH` to point at *this* clone's `src/`, e.g.:

```
cd ~/Developer/repos/ddp-sync-dev
PYTHONPATH="$PWD/src" ~/Developer/repos/ddp-sync/.venv/bin/python -m pytest tests/ -q
PYTHONPATH="$PWD/src" ~/Developer/repos/ddp-sync/.venv/bin/ruff check src/ tests/
```

`ddp_sync` is installed **editable**, pointing at the production checkout's `src/` — without the
`PYTHONPATH` override, you'd silently run tests against the live checkout's code instead of this
clone's.

**Work developed here must land in production via a pull request** — same discipline as
`ddp-open-states`: branch, commit, push, open a PR against this repo's `main` on GitHub. Never
push directly to `main` or fast-forward-merge locally and push. After a PR merges, updating the
live checkout (`git checkout main && git pull origin main` in `~/Developer/repos/ddp-sync`) is a
separate, deliberate step — check `git status`/`git log` there first to confirm it's actually on
`main` with no uncommitted work before pulling.

## No CI

None of `ddp-open-states`, `openstates-core`, `openstates-scrapers`, or `ddp-sync` have CI
configured — a PR's "tests pass" claim (including CodeBot's own PRs) is self-reported only.
Before merging anything, check out the actual branch in `ddp-sync-dev` (with its real
cross-repo/cross-branch dependencies checked out too, if any) and run the test suite yourself
rather than trusting the PR description.
