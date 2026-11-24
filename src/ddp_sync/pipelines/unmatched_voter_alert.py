"""OPEN-855: after each state's load, alert when a roll-call voter name matches no one.

Unmatched voter names (a printed name core's `resolve_person()` tied to no legislator, so the vote is stored with no
voter) used to be found by hand in onboarding's Stage 3 (Maine's 17, Kentucky's "Lewis C", New Mexico's "POPE, JR.").
Once a state was live nothing noticed a new one. `run_state_pipeline` now calls `maybe_alert_unmatched_voters` once per
run, after the broker signals and before the archive, when at least one session succeeded (the load into RDS is part of
the scrape step).

* **The count is not made here.** ddp-open-states' `unmatched-voters.py <state>` (with `qa_primitives.
  unresolved_voter_names`, Stage 3's Gate 1 rule: a person-vote with `voter_id` NULL) prints every session's vote and
  unmatched totals and every unmatched name with its count, read-only, as JSON. It runs with the openstates venv's
  Python (ddp-sync's own has no Postgres driver for it) and the live RDS login (`resolve_rds_database_url`), like
  `cloud_loader.py`.
* **Alert** (`post_alert`, #automation-errors) when the state has a name it did not have at its last run, in a session,
  or a session's unmatched share rose above its last run; it lists the top names with counts. Names the manifest
  accepts (`quality.accepted_unmatched_voters`, marked `accepted` by the script) are left out of both.
* **Memory:** the last run's names and shares in Redis (`ddp:unmatched_voters:<code>`, no expiry), written only after a
  run that needed no alert or whose alert was delivered, so an undelivered alert is tried again next run. With no
  memory (the first run after the release, or Redis down) every unmatched name counts as new.

Switch: `openstates_scrape.unmatched_voter_alert.enabled`. No per-state list: every scraped state is checked. Never
raises (the caller's `_logged` would catch it anyway) and never changes the run's outcome.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
from typing import Any

import structlog

from ddp_sync.services.rds_credentials import resolve_rds_database_url
from ddp_sync.slack_alerts import post_alert

logger = structlog.get_logger()

# The openstates toolchain's Python in ddp-sync's image (infrastructure/Dockerfile, OPEN-248): psycopg2 and PyYAML.
OPENSTATES_PYTHON = "/opt/venv-openstates/bin/python"
SCRIPT = "unmatched-voters.py"
TIMEOUT_S = 300
TOP_NAMES = 10
_KEY = "ddp:unmatched_voters:{code}"


def read_unmatched(code: str, openstates_root: str, runner=subprocess.run) -> dict[str, Any]:
    """Run `unmatched-voters.py <code>` against RDS and return its JSON. Raises RuntimeError (never with the connection
    string) when the login cannot be resolved or the script fails."""
    rds_url, error = resolve_rds_database_url()
    if error or not rds_url:
        raise RuntimeError(f"cannot resolve an RDS login: {error}")
    result = runner(
        [OPENSTATES_PYTHON, os.path.join(openstates_root, SCRIPT), code],
        cwd=openstates_root,
        env={**os.environ, "DATABASE_URL": rds_url},
        capture_output=True,
        text=True,
        timeout=TIMEOUT_S,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"{SCRIPT} exited {result.returncode}: {(result.stderr or '')[-300:].strip()}")
    return json.loads(result.stdout)


def snapshot(current: dict[str, Any]) -> dict[str, Any]:
    """What is remembered for the next run: the unaccepted (session, name) pairs, and each session's unaccepted
    unmatched count and total."""
    accepted_counts: dict[str, int] = {}
    names = []
    for entry in current.get("names") or []:
        if entry.get("accepted"):
            accepted_counts[entry["session"]] = accepted_counts.get(entry["session"], 0) + entry["count"]
        else:
            names.append([entry["session"], entry["name"]])
    shares = {
        session: [counts["unresolved"] - accepted_counts.get(session, 0), counts["total"]]
        for session, counts in (current.get("sessions") or {}).items()
    }
    return {"names": sorted(names), "shares": shares}


def _share(pair: list[int]) -> float:
    unmatched, total = pair
    return unmatched / total if total else 0.0


def compare(current: dict[str, Any], previous: dict[str, Any] | None) -> tuple[list[dict], list[tuple[str, float, float]]]:
    """(new names, sessions whose share rose). A new name is an unaccepted (session, name) not seen at the last run (all
    of them when there is no last run); a rise is a session whose unaccepted unmatched share is above its last run's."""
    now = snapshot(current)
    seen = {tuple(pair) for pair in (previous or {}).get("names") or []}
    new = [
        entry for entry in current.get("names") or []
        if not entry.get("accepted") and (entry["session"], entry["name"]) not in seen
    ]
    risen = []
    for session, pair in sorted(now["shares"].items()):
        before = ((previous or {}).get("shares") or {}).get(session)
        if before is not None and _share(pair) > _share(before):
            risen.append((session, _share(before), _share(pair)))
    return new, risen


def alert_text(code: str, current: dict[str, Any], new: list[dict], risen: list[tuple[str, float, float]],
               first_run: bool) -> str:
    """The alert: what changed, then the top unaccepted names, each with its votes summed over sessions."""
    new_names = {entry["name"] for entry in new}
    totals: dict[str, list] = {}
    for entry in current.get("names") or []:
        if not entry.get("accepted"):
            total = totals.setdefault(entry["name"], [0, []])
            total[0] += entry["count"]
            total[1].append(entry["session"])
    lines = [f":warning: {code.upper()} roll-call voter names that match no legislator after this load"
             + (" (first check: nothing earlier to compare with)" if first_run else "")]
    if new:
        lines.append(f"{len(new_names)} new name(s).")
    for session, before, after in risen:
        lines.append(f"Session {session}: unmatched share rose from {before:.2%} to {after:.2%}.")
    lines.append(f"Top names (of {len(totals)}):")
    for name, (count, sessions) in sorted(totals.items(), key=lambda item: (-item[1][0], item[0]))[:TOP_NAMES]:
        mark = " (new)" if name in new_names else ""
        lines.append(f"• \"{name}\": {count:,} votes in {', '.join(sorted(sessions))}{mark}")
    lines.append("Fix: add the printed name to the member's people file (other_names), or list it under the state's "
                 "quality.accepted_unmatched_voters in ddp-open-states' jurisdictions.yaml.")
    return "\n".join(lines)


async def _remembered(code: str) -> dict[str, Any] | None:
    from ddp_sync.services.redis_store import get_redis_store

    client = get_redis_store()._client
    if client is None:
        return None
    raw = await client.get(_KEY.format(code=code))
    return json.loads(raw) if raw else None


async def _remember(code: str, value: dict[str, Any]) -> None:
    from ddp_sync.services.redis_store import get_redis_store

    client = get_redis_store()._client
    if client is not None:
        await client.set(_KEY.format(code=code), json.dumps(value))


async def maybe_alert_unmatched_voters(jurisdiction: str, config: dict | None) -> None:
    """Check one state after its load and alert on a new unmatched voter name or a rising share (module docstring)."""
    try:
        block = (config or {}).get("unmatched_voter_alert") or {}
        if not block.get("enabled", False):
            return
        from ddp_sync.pipelines.openstates_archive import _archive_jurisdiction_code
        from ddp_sync.pipelines.openstates_scrape import _get_root

        code = _archive_jurisdiction_code(jurisdiction).lower()
        current = await asyncio.to_thread(read_unmatched, code, _get_root(config))
        try:
            previous = await _remembered(code)
        except Exception as e:  # noqa: BLE001 -- no memory means every name counts as new: louder, never quieter
            logger.warning("unmatched_voter_memory_unreadable", jurisdiction=code, error=str(e))
            previous = None
        new, risen = compare(current, previous)
        delivered = True
        if new or risen:
            text = alert_text(code, current, new, risen, first_run=previous is None)
            delivered = await asyncio.to_thread(post_alert, text, source="openstates_scrape.unmatched_voters")
        logger.info("unmatched_voter_check", jurisdiction=code, new_names=len(new), risen_sessions=len(risen),
                    alerted=bool(new or risen), delivered=delivered)
        if delivered:
            await _remember(code, snapshot(current))
    except Exception as e:  # noqa: BLE001 -- must never affect the scrape job's own result
        logger.warning("unmatched_voter_check_failed", jurisdiction=jurisdiction, error=str(e))
