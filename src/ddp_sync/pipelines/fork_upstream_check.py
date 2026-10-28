"""Daily check of how far each DDP fork has fallen behind its public upstream.

DDP keeps four forks of public OpenStates projects (people, openstates-core, openstates-scrapers,
api-v3). Catching a fork up is a human-reviewed pull request (ddp-infra PLAN-fork-management.md
§8); what nobody did was *notice* the gap opening, and a small gap is cheap to review where a
two-month gap is the 59-commit scrapers catch-up. This job only measures and tells:

- fetches each fork's `main` and its upstream's `main` over public https into a cache the job
  owns (a bare repo under `<openstates_root>/logs/fork-upstream-cache/`), with no credentials;
- counts upstream commits the fork lacks, finds how old the oldest one is, lists their titles
  and whether they touch sensitive areas (workflows, migrations, dependency files);
- writes `<openstates_root>/logs/fork-upstream-status.json` for a reviewer (human or agent) to
  read, and a `fork_upstream_check` flow status for `/health`;
- alerts through `post_alert` (#automation-errors) when a fork lacks a commit older than
  `alert_after_days`, or when a fetch/compare fails.

It never merges, never opens a pull request and never pushes. It never touches the working
trees under `<openstates_root>` (people/, openstates-core/, ...): those are what live scrapes
read.

Alerts are de-duplicated against the previous status file so a fork that stays behind does not
repeat the same message every night: a behind-fork alerts when it first crosses the threshold,
again when it crosses each later tier, and then at most once per `repeat_days`. A failing fetch
alerts on its first night, on the third, and weekly after that. Everything resets once the
fork is current (or the fetch works).
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import UTC, datetime
from typing import Any

import structlog

from ddp_sync.pipelines.openstates_scrape import (
    _get_root,
    _run_with_group_kill,
    _write_flow_status,
)
from ddp_sync.slack_alerts import post_alert

logger = structlog.get_logger()

FLOW_KEY = "fork_upstream_check"
STATUS_FILENAME = "fork-upstream-status.json"
CACHE_DIRNAME = "fork-upstream-cache"

_GIT_TIMEOUT_SECONDS = 600
_TITLE_LIMIT = 10
# A bounded list in the status file and the alert; the count is always the full number.

# The first tier is the alert threshold (3 days, per SYNC-104). Later tiers re-alert when the
# oldest missing commit crosses them, so a gap that keeps growing is not silent after the first
# message. Past the last tier, `repeat_days` applies.
DEFAULT_TIERS_DAYS = (3, 7, 14)
DEFAULT_REPEAT_DAYS = 7
_FAILURE_ALERT_STREAKS = (1, 3)  # then every DEFAULT_REPEAT_DAYS-th night

# Where a change deserves a closer look before a catch-up is merged.
_SENSITIVE_PREFIXES = (".github/workflows/",)
_SENSITIVE_SUBSTRINGS = ("/migrations/",)
_SENSITIVE_NAMES = ("pyproject.toml", "poetry.lock", "uv.lock", "requirements.txt", "Dockerfile")

# Default repo list. `fork`/`upstream` are git URLs, overridable per entry in
# sync_schedule.yaml (tests point them at local bare repos).
_DEFAULT_REPOS = (
    ("people", "openstates/people"),
    ("openstates-core", "openstates/openstates-core"),
    ("openstates-scrapers", "openstates/openstates-scrapers"),
    ("api-v3", "openstates/api-v3"),
)
_FORK_ORG = "Digital-Democracy-Project"


def _default_repo_cfgs() -> list[dict[str, str]]:
    return [
        {
            "name": name,
            "fork": f"https://github.com/{_FORK_ORG}/{name}.git",
            "upstream": f"https://github.com/{upstream}.git",
            "branch": "main",
        }
        for name, upstream in _DEFAULT_REPOS
    ]


def _git(args: list[str], cwd: str | None = None) -> tuple[int, str, str, bool]:
    """Run git with no credential helper and no prompts; returns (rc, stdout, stderr, timed_out)."""
    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0", "GIT_ASKPASS": "true"}
    cmd = ["git", "-c", "credential.helper=", *args]
    rc, out, err, timed_out, _ = _run_with_group_kill(cmd, env, _GIT_TIMEOUT_SECONDS, cwd=cwd)
    return rc, (out or b"").decode(errors="replace"), (err or b"").decode(errors="replace"), timed_out


class _GitError(Exception):
    pass


def _git_ok(args: list[str], cwd: str | None = None) -> str:
    rc, out, err, timed_out = _git(args, cwd)
    if timed_out:
        raise _GitError(f"git {args[0]} timed out after {_GIT_TIMEOUT_SECONDS}s")
    if rc != 0:
        raise _GitError(f"git {args[0]} exit {rc}: {err.strip()[-200:]}")
    return out


def _is_sensitive(path: str) -> bool:
    return (
        path.startswith(_SENSITIVE_PREFIXES)
        or any(s in f"/{path}" for s in _SENSITIVE_SUBSTRINGS)
        or os.path.basename(path) in _SENSITIVE_NAMES
    )


def _area(path: str) -> str:
    parts = path.split("/")
    if parts[0] == ".github" and len(parts) > 1:
        return ".github/" + parts[1]
    if parts[0] == "data" and len(parts) > 1:
        return f"data/{parts[1]}"
    return parts[0]


def _compare_repo(cache_root: str, cfg: dict[str, str], now: datetime) -> dict[str, Any]:
    """Fetch both mains into this repo's cache and measure what the fork lacks. Raises _GitError."""
    name = cfg["name"]
    branch = cfg.get("branch", "main")
    cache = os.path.join(cache_root, f"{name}.git")
    if not os.path.isdir(cache):
        os.makedirs(cache_root, exist_ok=True)
        _git_ok(["init", "--bare", "--quiet", cache])
    # Treeless fetch: commits and trees (enough for log and name-only diff), no file contents.
    for ref, url in (("fork", cfg["fork"]), ("upstream", cfg["upstream"])):
        _git_ok(
            ["fetch", "--quiet", "--no-tags", "--filter=blob:none", url,
             f"+refs/heads/{branch}:refs/checks/{ref}"],
            cwd=cache,
        )
    # Commits on upstream's main that the fork's main does not contain, oldest first.
    log = _git_ok(
        ["log", "--first-parent", "--reverse", "--format=%cI\t%s", "refs/checks/fork..refs/checks/upstream"],
        cwd=cache,
    )
    commits = [line.split("\t", 1) for line in log.splitlines() if line.strip()]
    result: dict[str, Any] = {
        "name": name,
        "behind_count": len(commits),
        "oldest_age_days": None,
        "titles": [c[1] for c in commits[-_TITLE_LIMIT:]][::-1],  # newest first
        "areas_changed": [],
        "sensitive_paths_changed": [],
    }
    if not commits:
        return result
    oldest = datetime.fromisoformat(commits[0][0])
    result["oldest_age_days"] = round((now - oldest).total_seconds() / 86400, 1)
    # Three dots: what upstream changed since the two histories last met, not our own carried fixes.
    paths = _git_ok(
        ["diff", "--name-only", "refs/checks/fork...refs/checks/upstream"], cwd=cache
    ).splitlines()
    result["areas_changed"] = sorted({_area(p) for p in paths if p})
    result["sensitive_paths_changed"] = sorted(p for p in paths if p and _is_sensitive(p))[:20]
    return result


def _tier(age_days: float | None, tiers: tuple[int, ...]) -> int:
    if age_days is None:
        return 0
    return sum(1 for t in tiers if age_days >= t)


def _load_previous(status_path: str) -> dict[str, Any]:
    try:
        with open(status_path) as f:
            data = json.load(f)
        return data.get("repos", {}) if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _behind_alert_due(entry: dict, prev: dict, tiers: tuple[int, ...], repeat_days: int, now: datetime) -> bool:
    tier = _tier(entry["oldest_age_days"], tiers)
    if prev.get("alerted_tier", 0) < tier:
        return True
    last = prev.get("last_alert_at")
    if not last:
        return True
    return (now - datetime.fromisoformat(last)).days >= repeat_days


def _failure_alert_due(streak: int, prev: dict, repeat_days: int, now: datetime) -> bool:
    if streak in _FAILURE_ALERT_STREAKS:
        return True
    last = prev.get("last_alert_at")
    return streak > _FAILURE_ALERT_STREAKS[-1] and (
        not last or (now - datetime.fromisoformat(last)).days >= repeat_days
    )


def _write_status_file(status_path: str, record: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(status_path), exist_ok=True)
    tmp = f"{status_path}.tmp"
    with open(tmp, "w") as f:
        json.dump(record, f, indent=2, sort_keys=True)
    os.replace(tmp, status_path)  # a reader never sees a half-written file


def _behind_text(entry: dict) -> str:
    sens = entry["sensitive_paths_changed"]
    lines = [
        f":warning: *{entry['name']} fork is {entry['behind_count']} upstream commit(s) behind* — "
        f"the oldest is {entry['oldest_age_days']:.0f} days old. A small catch-up is a quick "
        f"review; a big one is days of work. Recent upstream titles: "
        + "; ".join(entry["titles"][:3]),
    ]
    if sens:
        lines.append(f"Touches files worth a closer look: {', '.join(sens[:4])}")
    lines.append("Status: `logs/fork-upstream-status.json`. Nothing was merged.")
    return " ".join(lines)


async def run_fork_upstream_check_job(config: dict | None = None) -> dict[str, Any]:
    """Measure every fork's gap to upstream, record it, and alert per the de-dup rules above."""
    config = config or {}
    cfg = config.get("fork_upstream_check", {}) or {}
    root = _get_root(config)
    status_path = os.path.join(root, "logs", STATUS_FILENAME)
    cache_root = os.path.join(root, "logs", CACHE_DIRNAME)
    repo_cfgs = cfg.get("repos") or _default_repo_cfgs()
    tiers = tuple(cfg.get("alert_tiers_days") or DEFAULT_TIERS_DAYS)
    repeat_days = cfg.get("repeat_days", DEFAULT_REPEAT_DAYS)

    start = time.monotonic()
    now = datetime.now(UTC)
    logger.info("fork_upstream_check: starting", repos=[r["name"] for r in repo_cfgs])
    previous = _load_previous(status_path)
    repos: dict[str, Any] = {}
    alerts: list[str] = []

    for repo_cfg in repo_cfgs:
        name = repo_cfg["name"]
        prev = previous.get(name, {})
        try:
            entry = await asyncio.to_thread(_compare_repo, cache_root, repo_cfg, now)
            entry.update(status="ok", failure_streak=0)
            tier = _tier(entry["oldest_age_days"], tiers)
            if tier and _behind_alert_due(entry, prev, tiers, repeat_days, now):
                alerts.append(_behind_text(entry))
                entry.update(alerted_tier=tier, last_alert_at=now.isoformat())
            elif tier:
                # Still behind and already told: carry the memory forward unchanged.
                entry.update(alerted_tier=prev.get("alerted_tier", 0), last_alert_at=prev.get("last_alert_at"))
        except Exception as e:  # noqa: BLE001 -- a bad repo must not hide the other three
            streak = prev.get("failure_streak", 0) + 1
            entry = {
                "name": name, "status": "failed", "error": f"{type(e).__name__}: {e}",
                "failure_streak": streak,
                # A failed night measured nothing: keep the last good numbers visibly stale.
                "behind_count": prev.get("behind_count"), "oldest_age_days": prev.get("oldest_age_days"),
                "titles": prev.get("titles", []), "areas_changed": prev.get("areas_changed", []),
                "sensitive_paths_changed": prev.get("sensitive_paths_changed", []),
                "alerted_tier": prev.get("alerted_tier", 0), "last_alert_at": prev.get("last_alert_at"),
            }
            logger.error("fork_upstream_check: compare failed", repo=name, error=str(e), streak=streak)
            if _failure_alert_due(streak, prev, repeat_days, now):
                alerts.append(
                    f":x: *Could not check whether the {name} fork is behind upstream* "
                    f"(night {streak} in a row): {entry['error'][:200]}. "
                    f"The check measured nothing, so the last numbers in the status file are stale."
                )
                entry["last_alert_at"] = now.isoformat()
        repos[name] = entry

    record = {
        "flow": FLOW_KEY,
        "checked_at": now.isoformat(),
        "repos": repos,
    }
    try:
        await asyncio.to_thread(_write_status_file, status_path, record)
    except OSError as e:
        # The memory that de-duplicates alerts lives here; say so rather than alert blindly.
        logger.error("fork_upstream_check: could not write status file", path=status_path, error=str(e))
        alerts.append(f":x: *fork upstream check could not write its status file* ({e}); alerts may repeat.")

    for text in alerts:
        await asyncio.to_thread(post_alert, text, source="fork_upstream_check")

    failed = [n for n, e in repos.items() if e["status"] == "failed"]
    duration = round(time.monotonic() - start, 1)
    await _write_flow_status(FLOW_KEY, {
        "flow": FLOW_KEY,
        "started_at": now.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "status": "failed" if failed else "completed",
        "error": f"could not check: {', '.join(failed)}" if failed else None,
        "duration_seconds": duration,
        "behind": {n: e["behind_count"] for n, e in repos.items()},
    })
    logger.info("fork_upstream_check: done", failed=failed, alerts=len(alerts), duration_seconds=duration)
    return {"success": not failed, "repos": repos, "alerts_sent": len(alerts), "duration_seconds": duration}
