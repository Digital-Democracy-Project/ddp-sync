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

## ddp-sync runs as two independent instances -- settings do not carry across them

There are two separate, independently-configured deployments of this same codebase, not
one: the **Mac Studio** instance (the only one with CAMS/LegBot/MLX access — it runs the
scheduler for most jurisdictions' scraping into local Postgres), and the **EC2-broker**
instance (OPEN-193, co-located with production `ddp-broker-py` — it owns Fargate-based
scraping + RDS loading for a specific, configured list of jurisdictions). Exactly one
instance owns any given jurisdiction (`_cloud_path_owns()`); a jurisdiction never runs on
both.

Because these are separate processes, **an env var/setting set on one has no effect on the
other** — there is no shared config store. This bit SYNC-59 directly: the EC2 instance now
reaches the Mac's `ddp-sync` over the existing WireGuard mesh (`MAC_DDP_SYNC_BASE_URL`,
same live pattern `ddp-api`'s proxy already uses to reach the Mac's local api-v3, API-6) to
trigger LegBot after a cloud-owned scrape+RDS-load finishes. The gate for this,
`LEGBOT_SCRAPE_COMPLETION_TRIGGER_ENABLED`, is checked independently on **both** sides —
enabling it only on the Mac (already the case for the pre-existing in-process scrape hook)
does **not** also enable the cloud path; both deployments need their own copy turned on.
If a cloud-owned jurisdiction's LegBot trigger looks silently inactive, check both
instances' own env, not just one.

**Known, deliberate gap as of 2026-09-11 (SYNC-59) — superseded, see the section below.**
This originally said cloud-owned jurisdictions with no single, explicitly-configured
session (VA/UT/MI/MA/AZ/NC) couldn't trigger LegBot at all, pending an RDS-facing read
replica. That replica shipped and went live 2026-09-13 (OPEN-269 epic) — the real
remaining gap by 2026-09-16 was narrower and different in kind (a trigger-path
endpoint-duplication bug, then a silent session-resolution failure), both fixed. Kept
here as a historical marker only; the section immediately below is current.

## Crash-survivable LegBot dispatch tracking, and the RDS-replica gates layered on top of it (SYNC-61/275/276)

`pipelines/bill_artifact_generation.py`'s `bill_changelog` dispatch records each in-flight
task in Redis (`ddp:legbot:task:*`, sibling to the scraper pipeline's own `ddp:sync:task:*`
convention) before polling starts, so a process crash mid-dispatch doesn't lose a real,
already-computed CAMS answer — `recover_stale_legbot_dispatches()`, wired into the existing
`_zombie_sync_watchdog`, sweeps stale records on the next cycle. `read_task_result()` is the
one place that validates a stored CAMS answer is actually a dict before any caller touches
it — two real production crashes (a corrupted stored answer hitting first the recovery
sweep, then a live dispatch's own completion log line) were both fixed here, at the source,
rather than patched at each call site individually. If you add a new caller of a CAMS task
result, it goes through this function; don't re-implement your own JSON read.

**A real mechanism for reading RDS-loaded data on the Mac is a *different shape* than the
`RDS_OPENSTATES_API_BASE` override above, and the two have not been fully reconciled.**
`ddp-infra`'s `PLAN-rds-local-postgres-replication.md` sets up Postgres logical replication
from RDS into the Mac's own local Postgres (a `ddp_local_replication` subscription,
publishing all tables for every jurisdiction RDS holds any data for) — the idea being that
the Mac's *existing* `local_openstates_api_base` read path becomes correct for cloud-owned
jurisdictions too, once replication has caught up, rather than reading through a second,
separate RDS-facing api-v3 instance. Two gates sit in `session_pipeline_runner.py`'s
`_process_bill_inner`, both from the `PLAN-rds-local-postgres-replication.md`/OPEN-269 epic:
- `REPLICA_FRESHNESS_CHECK_ENABLED` (OPEN-275) — before dispatching a bill, compares an
  `md5(raw_text)` content hash between RDS's current version-document row and the Mac's
  local replica, failing closed (skip, no `BillArtifact` write) on any mismatch or error.
  Independently overridable via `REPLICA_FRESHNESS_CONTENT_CHECK_ENABLED` (OPEN-289
  follow-up) — set `False` in production for now, since OPEN-274's own health-check script
  also needs a live RDS credential that isn't configured, so "trust the health check
  instead" doesn't avoid a live-RDS dependency, it just moves it. Revert to `True` once
  OPEN-274 is properly built out (real `RDS_MONITORING_DATABASE_URL` + a scheduled run).
- `LEGBOT_RDS_REPLICA_JURISDICTION_ALLOWLIST` (OPEN-276) — checked *before* the freshness
  check (cheap local test, no reason to pay an RDS round-trip for a jurisdiction dispatch
  won't trust regardless), gates which jurisdictions may dispatch from the RDS-fed replica
  at all.

**As of 2026-09-13, both are live in production**, not just built: real logical replication
from production RDS is up (OPEN-271/272/273/274 all Done, independently verified), and
`LEGBOT_RDS_REPLICA_JURISDICTION_ALLOWLIST` covers 9 of the 10 tracked jurisdictions
(`mi,ut,fl,va,wa,us,ma,az,nc` — only `al` excluded, since it never migrated to the RDS/cloud
path at all). If asked to make a cloud-owned jurisdiction trigger LegBot end-to-end and it's
not already working: **check `PLAN-rds-local-postgres-replication.md` and this epic's own
Jira tickets for current status** before assuming either this mechanism or SYNC-59's own
`RDS_OPENSTATES_API_BASE` needs building — most of this epic's build work is done; what's
most likely still open is the specific per-jurisdiction session-resolution gap SYNC-59's own
section above describes.

**OPEN-290 (2026-09-14): the two HTTP entry points into this dispatch machinery
(`/trigger/scraper-session-legbot`, automated-only; `/trigger/bill-artifact-generation`,
manual) were consolidated into one.** They called the identical underlying function
(`run_legbot_pipeline`) but only one of them held SYNC-48's Redis overlap lock — a real
incident (two full FL/2026E runs dispatched 20 seconds apart, invisible to each other)
proved this. `/trigger/scraper-session-legbot` is deleted; `/trigger/bill-artifact-generation`
now routes through `trigger_scraper_session_pipeline` (the lock wrapper) for every caller,
manual or automated. An `X-DDP-Automated-Trigger` header on the request is what still lets
the automated (WireGuard-relayed, archive-completion-hook) caller be gated independently by
`LEGBOT_SCRAPE_COMPLETION_TRIGGER_ENABLED` — a plain manual call omits that header and is
never subject to that flag. The lock key is case-normalized (`.upper()` on both jurisdiction
and session) specifically because the two callers didn't agree on casing before this.

**OPEN-291 (2026-09-14): a jurisdiction's archive job now fires automatically off that
jurisdiction's own scrape completion**, not a fixed weekly cron alone — `_run_scrape()`
(every scrape path funnels through it: FL, WA, USA, the whole secondary batch) calls
`maybe_trigger_archive_after_scrape()` on success. Config-driven opt-in only (a
jurisdiction just needs to be in both its own scrape config and
`openstates_archive.jurisdictions` — no per-jurisdiction code). A Redis debounce inside
`_run_archive_with_hook` itself (not just at the trigger call site) prevents FL/USA's
multi-session-per-run shape, or the still-registered weekly cron landing close to a
hook-triggered run, from launching two overlapping archive Fargate tasks for the same
jurisdiction — it fails *open* (proceeds) if Redis is down, deliberately, since this
feature must never make archiving less reliable than it was before it existed.

**OPEN-292 (2026-09-15): the SYNC-48 overlap lock is now a renewed lease, not a flat
TTL.** A real full-session sweep (`limit=5000`, no `bill_candidates` cap) has no natural
duration bound — MI's own first full run took 31.4 hours (see `PLAN-legbot.md` §34),
which blew straight through the old flat 4-hour TTL, leaving most of that run with zero
overlap protection. The TTL is now short (`legbot_scrape_completion_trigger_lock_ttl_seconds`,
default 600s) and renewed on a background heartbeat every
`legbot_scrape_completion_trigger_lock_renewal_seconds` (default 120s) for as long as the
pipeline call is actually still running — each renewal re-checks ownership (GET-and-compare)
before extending the expiry, so it can never resurrect a lock a newer trigger has already
legitimately acquired. Real side benefit: a hard-crashed process (no clean shutdown) now
loses its lock within about one TTL window instead of up to 4 hours.

**SYNC-66 (2026-09-16, caught live in production): `resolve_touched_sessions()` used to
return `[]` both when nothing was genuinely touched AND when it couldn't check at all**
(api-v3 unreachable, rejected the request, or returned a malformed body) — logged at the
same INFO level either way. A real transient connection blip during the `us`
archive→LegBot hook (46 documents genuinely archived that run) was silently indistinguishable
from a clean no-op, and an entire archive run's worth of new content never reached LegBot.
Fixed: the function now returns `None` (not `[]`) when resolution genuinely fails and no
session was already found on an earlier page — `[]` is reserved for a real "nothing touched"
result. The caller (`_maybe_trigger_legbot_for_archive`) logs `None` at ERROR, not the
"nothing to do" INFO path. A small fixed retry (3 attempts, 2s apart) rides out the
specific connection-failure mode that was confirmed transient in production. **If you touch
this function, remember `None` and `[]` mean different things to every caller — don't
special-case "falsy" without checking which one you actually got.**

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

## Scrape cadence can now escalate itself (OPEN-140 `dynamic_cadence`, rolled out 2026-09-16)

`sync_schedule.yaml`'s per-jurisdiction `sync_day`/cron entries used to be the
whole story: a human decided how often a jurisdiction got scraped, and that
stayed fixed until someone edited the file. `dynamic_cadence` adds a second,
automatic layer on top, controlled by `config/sync_schedule.yaml`'s
`dynamic_cadence` block (`enabled`, `jurisdictions_excluded`).

**Redis holds the runtime truth; the YAML value is only a floor.** When a
jurisdiction's own signals (real recent legislative activity — new bills,
votes, status changes since last scrape) justify scraping more often than its
configured cadence, ddp-sync escalates it by writing a shorter effective
interval to Redis; the scheduler reads Redis first and falls back to the YAML
value only when nothing is there. **Escalation is automatic. Demotion back
down is advisory-only** — nothing here silently reduces a jurisdiction to a
sleepier cadence than its configured floor; a human decides that.

**MI was excluded from this mechanism from OPEN-140's initial rollout until
2026-09-16**, for WAF-safety reasons tracked under OPEN-53 — MI's legislature
site had a history of rate-limit/WAF trouble, and letting a self-escalating
system scrape it more aggressively without a human in the loop was judged too
risky until that history was reviewed. The exclusion was removed, and the
mechanism enabled system-wide for FL plus all 6 `secondary` jurisdictions, only
after two separate explicit confirmations from Ramon in the same session (one
for lifting MI's exclusion specifically, one for enabling the mechanism itself
beyond MI) — **treat either half of that as requiring its own sign-off if it
ever needs touching again; a decision to relax one does not imply the other.**
FL's `sync_day` comment, which had drifted into a stale manual-reminder TODO
predating this mechanism, was cleaned up to describe the current automatic
behavior instead.

## Running a targeted LegBot backfill: use `bill_candidates`, not a loop over the on-demand endpoint

A 947-bill `bill_changelog` backfill (2026-09-10/11) was run by calling
`/trigger/legbot-analyze-bill` (the on-demand, single-bill endpoint, with zero
concurrency awareness) once per bill from an external script, paced only by a
0.3s delay between *sending* each request. That pacing controlled nothing
about how many dispatches were actually in flight at once — each call's own
poll loop runs for potentially minutes, so hundreds ended up genuinely
concurrent regardless of the send-side delay. That overloaded CAMS's
status-check endpoint (618 real `500`s in ~6 minutes) and triggered a chain of
fixes: SYNC-56 (a stuck-placeholder bug this surfaced), SYNC-60 (retry past a
brief connection failure/5xx instead of hard-failing), SYNC-61 (track
in-flight dispatches in Redis and recover a genuinely-completed one instead of
losing it), SYNC-62 (id-targeted BillArtifact writes the recovery path needed),
and SYNC-63 (this section).

**Don't reach for an uncoordinated script again.** For a large targeted
backfill against a specific, known list of bills (not "every bill in a
session"), use `/trigger/bill-artifact-generation`'s `bill_candidates` field
(SYNC-63) instead of scripting a loop over `/trigger/legbot-analyze-bill`. It
feeds the same `session_pipeline_concurrency`-bounded, coverage-aware pipeline
the session-wide scan already uses (`session_pipeline_runner.py`'s
`run_legbot_pipeline`) a caller-supplied list instead of a session scan — real
concurrency stays capped regardless of how large the list is, `retry_failed`/
`dry_run` work exactly the same way, and an already-covered bill is skipped
automatically rather than requiring a caller's own ad hoc checkpoint file:

```
POST /trigger/bill-artifact-generation
{
  "jurisdiction_iso2": "fl", "session_code": "2026F",
  "artifact_types": ["bill_changelog"],
  "include_org_research": false, "include_concept_statements": false,
  "retry_failed": false, "limit": 1,
  "bill_candidates": [
    {"gov_id": "SJR 2F", "bill_openstates_id": "a3afb726-0000-0000-0000-000000000001"},
    ...
  ]
}
```

`limit` is still a required, validated positive value, but it does not
select or truncate `bill_candidates` in this mode — every supplied entry is
processed regardless of what `limit` is set to; pass any positive integer.

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
`prompt_version="v2"`. SYNC-42 (#85, merged) added a test in
`tests/test_broker_client.py` that scans `src/` and fails, naming file and
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
when there is something to record. `ddp-broker-py` #363 (merged) is what makes
the field writable at all; SYNC-43 (`ddp-sync` #86, merged) establishes the
convention of a `source_support=inferred: ...` prefix there, queried with
`bill_version__session_code` — note `session_code` lives on `BillVersion`, not
`BillArtifact`, and the obvious spelling raises `FieldError`.

## No CI

None of `ddp-open-states`, `openstates-core`, `openstates-scrapers`, or `ddp-sync` have CI
configured — a PR's "tests pass" claim (including CodeBot's own PRs) is self-reported only.
Before merging anything, check out the actual branch in `ddp-sync-dev` (with its real
cross-repo/cross-branch dependencies checked out too, if any) and run the test suite yourself
rather than trusting the PR description.
