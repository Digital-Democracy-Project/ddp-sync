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

## Recurring jobs are scheduled by ddp-sync, not by CAMS

Every recurring/scheduled pipeline in this stack is meant to be scheduled by **ddp-sync's own
scheduler** (`src/ddp_sync/scheduler.py` and its YAML config) and fired into the target service
via an on-demand API call — e.g. OpenStates scrapes/archives, bill-version checks, and votebot
evals all work this way, each with a matching `/trigger/*` endpoint in
`src/ddp_sync/api/routes/triggers.py` for manual/ad-hoc runs alongside the cron-driven call. CAMS
(`ddp-agents`) must never self-schedule a recurring job internally (e.g. via its own
`cams.scheduler.CronScheduler` registered inside `app.py`'s lifespan) — if a job needs to run on a
cron, that cron belongs in ddp-sync, calling CAMS's existing on-demand trigger endpoint for it.

If asked to change a schedule (frequency, timing) for a recurring job, check first whether it's
actually scheduled this way — if it turns out to be self-scheduled inside CAMS or another service
instead, that's an architecture violation to flag and fix (typically a linked pair of Jira tickets,
one in `AGENTS` and one in `SYNC`), not just a config edit. Confirmed directly by Ramon, 2026-08-23,
when GrantBot's weekly Notion-funder-scrape job (self-scheduled inside CAMS at `"0 7 * * 1"`) was
found and flagged this way instead of just editing its cron string in place — see AGENTS-54 and
SYNC-36.

## Dispatch poll cadence is load-bearing, not cosmetic

`legbot_client.py`'s poll interval (`LEGBOT_POLL_INTERVAL_SECONDS`, default 1s)
governs more than how promptly this service notices a task finished. It sets how
long a bill leaves its MLX worker idle between calls, and CAMS's
`MLXWorkerSupervisor` cannot tell an idle-but-still-needed worker from a free
one — so a competing bill takes it, and the bill pays a fresh prefill when it
comes back.

It was 5 seconds until SYNC-39. Lowering it to 1 took a 20-bill VA 2026S1 run
from 830s to 501s (**40%**), cut re-prefills from 27 to 2, and took
`concept_statements`' cache hit rate from 10% to 100%. Nothing about the MLX
work changed: real generation stayed at ~1.5s throughout. The gain was waiting
that stopped happening.

Two rules follow.

**Do not raise this interval without measuring.** It looks like a politeness
knob against a service on the same machine. It is not; it is roughly a 40%
throughput lever.

**Do not use this client's reported durations as work timings.** They include
poll latency. Under the old 5s interval every artifact duration in a run was a
multiple of five, and a figure derived that way ("a cold prefill costs 15-30s")
propagated into `ddp-agents`' own CLAUDE.md and misled analysis for a session —
the real prefill on that corpus was 0.80s. For work timing use CAMS's
`mlx_artifact_started`→`mlx_artifact_complete` audit lines, which are not
quantised by anything here.

Full write-up and data: `ddp-agents`' `bench/legbot-throughput-2026-08-27/`.

`scrapebot_client.py` has its own copy of the constant, still 5, deliberately —
its mint holds no per-bill cache, so cadence costs it nothing there.

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

## Writing BillArtifacts: two invariants that look like free improvements

Both of these were established by measurement against the dev broker
(2026-08-28, SYNC-42 and SYNC-43), and both look like obvious upgrades until
you check what they actually do.

**Leave `model_version` / `prompt_version` / `prompt_hash` unset.** `ddp-sync`
passes none of them at any call site today, and all 1,459 artifact rows in the
dev broker have them NULL. That is load-bearing, not an oversight. ddp-broker-py
resolves the row a write lands on with those fields in the key, so with them
NULL a regeneration or retry **updates the existing row**; populate any of them
and the same write **creates a parallel row** instead — leaving a `failed` row
and a `complete` row for the same version and artifact type with nothing to say
which is current. Demonstrated both ways inside a rolled-back transaction:
delta 0 rows with them NULL, +1 row and a duplicate group with
`prompt_version="v2"`. SYNC-42 (#85, in review as of 2026-08-28) adds a test
in `tests/test_broker_client.py` that scans `src/` and fails, naming file and
line, if anything starts populating them. Adding "proper provenance" is not a
free improvement — it silently
changes what a retry means, and retry semantics have to be decided first.

Worth knowing why the constraint does not save you: the
`unique_billartifact_generation` partial index includes both fields, and
Postgres treats NULLs as distinct, so **that index does not constrain these
rows at all**. What makes the NULL case work is an explicit both-NULL branch in
ddp-broker-py's `_existing_ai_row`. The safety is deliberate application code,
not the database.

**`review_status` is not a signal you can use, and `validation_notes` must not
be blanked.** `review_status` defaults to `pending_review` for every artifact
the broker stores (1,278 of 1,459 rows), and its write path deliberately
*resets* it to `pending_review` on every write, because an approval belongs to
specific text. So it does not discriminate — "list everything awaiting review"
returns nearly the whole table — and it is deliberately not caller-writable,
since a generating service must not be able to mark its own output approved.
Do not reach for it as a "this one needs a look" marker; that was SYNC-43's
original design and it could not have worked.

`validation_notes` is the discriminator instead (empty on all 1,459 rows), and
it is **editable by a human reviewer** in ddp-broker-py's admin Content
fieldset. So never write it unconditionally: sending `""` on every write lets
each regeneration silently erase a reviewer's own notes. Send the field only
when there is something to record. SYNC-43 (`ddp-sync` #86 + `ddp-broker-py`
#363, in review as of 2026-08-28) establishes the convention of a
`source_support=inferred: ...` prefix there, queried with
`bill_version__session_code` — note `session_code` lives on `BillVersion`, not
`BillArtifact`, and the obvious spelling raises `FieldError`.

## No CI

None of `ddp-open-states`, `openstates-core`, `openstates-scrapers`, or `ddp-sync` have CI
configured — a PR's "tests pass" claim (including CodeBot's own PRs) is self-reported only.
Before merging anything, check out the actual branch in `ddp-sync-dev` (with its real
cross-repo/cross-branch dependencies checked out too, if any) and run the test suite yourself
rather than trusting the PR description.
