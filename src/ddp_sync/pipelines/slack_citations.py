"""Nightly pull of organization-position citations from Slack #legislation (SYNC-127).

Staff (through the Zapier/LegBot reply) post each citation inside a bill's thread:

    I will update the records to show that `<org>` supports|opposes this legislation.
    Here is my citation: <url>

This job reads the channel's threads (thread replies included: the citations are replies, so channel
history alone misses them), turns each new citation into a verify_org_citations item, and either only
reports what it would send (`mode: report_only`, the default) or sends them through the same
verification-and-store path the manual trigger uses (`mode: write`). In write mode the cited position is
verified and written, then (with SYNC-127's other change, resolve_and_describe_organizations) matched to
an existing organization or described like any other new position.

Rules the ticket set, and where they live here:
  - The bill comes only from the thread's openstates.org link (jurisdiction, session and number). A
    thread without one is reported as `no_openstates_link`, never guessed from "submitted `Florida` bill
    `HB 7`": a number alone is ambiguous across sessions.
  - Each Slack message is recorded as handled once its citation is settled (Redis set of message
    timestamps, no expiry), not by a "last seen" time, because replies arrive late. If that record cannot
    be read, the run stops instead of resending everything.
  - Serial and capped per run (`max_citations_per_run`): every item is a metered verification call. What
    the cap leaves out stays unhandled and goes in the next run.
  - One host runs it: the per-host SLACK_CITATIONS_ENABLED flag is off unless a host's environment says
    true (the Mac, where CAMS is), on top of the shared yaml `enabled`.
  - The Slack token is the Agent Smith bot's SLACK_BOT_TOKEN (scope channels:history), the one ddp-sync
    already holds for alerts. It is read from the environment and never logged.

Parsing follows ddp-broker-py's notes/ops-handoff `notes/scripts/slack_citations.py`, which built the
manual batches (about 7,100 citations) before this job existed.
"""

from __future__ import annotations

import asyncio
import html
import os
import re
import time
from collections import Counter
from datetime import UTC, datetime

import httpx
import structlog

from ddp_sync.pipelines.verify_org_citations import (
    new_run_record,
    run_verify_org_citations_in_background,
)
from ddp_sync.services.local_openstates_client import find_bill
from ddp_sync.services.redis_store import get_redis_store
from ddp_sync.slack_alerts import post_alert

logger = structlog.get_logger()

DEFAULT_CHANNEL = "C04D5DWU5P1"  # #legislation
_SLACK_API = "https://slack.com/api/"
_SLACK_TIMEOUT_SECONDS = 30.0
_SLACK_MAX_ATTEMPTS = 6

CITATION_RE = re.compile(
    r"update the records to show that\s*`(?P<org>[^`]+)`\s*(?P<verb>supports|opposes)\s+this legislation"
    r"[\s\S]*?Here is my citation:\s*<(?P<url>[^>|\s]+)",
    re.IGNORECASE,
)
OPENSTATES_RE = re.compile(
    r"openstates\.org/(?P<jur>[a-z]{2})/bills/(?P<session>[^/\s>|]+)/(?P<num>[^/\s>|?]+)", re.IGNORECASE
)

# Outcomes that settle a citation for good, so its message is recorded as handled. Anything else
# (rate limited, the broker refused the write, the bill lookup could not answer) is tried again next run.
_SETTLED_OUTCOMES = {"written", "verification_failed", "no_current_version", "duplicate_in_request"}
_SETTLED_UNMATCHED = {"no_openstates_link", "bill_not_found"}

# Alert once a citation the cap left behind is this close to leaving the lookback window unsent.
_AGE_OUT_WARNING_DAYS = 7

# One run at a time per process (the nightly job and a manual trigger must not overlap).
_run_lock = asyncio.Lock()


def _norm_num(s: str) -> str:
    return re.sub(r"[\s.\-]", "", s).upper()


def parse_thread(messages: list[dict]) -> tuple[dict | None, list[dict]]:
    """One thread (root and replies, as Slack returns them) -> (its bill reference or None, its citations).

    The bill reference is {"jurisdiction" (upper ISO2), "session", "number"} from the first openstates.org
    link anywhere in the thread. Each citation is {"ts", "org_name", "position", "citation_url"}; the same
    (organization, stance, url) twice in one thread is kept once (the first message)."""
    text_all = "\n".join(html.unescape(m.get("text") or "") for m in messages)
    link = OPENSTATES_RE.search(text_all)
    bill = (
        {"jurisdiction": link["jur"].upper(), "session": link["session"], "number": _norm_num(link["num"])}
        if link else None
    )
    citations, seen = [], set()
    for m in messages:
        hit = CITATION_RE.search(html.unescape(m.get("text") or ""))
        if not hit:
            continue
        citation = {
            "ts": m["ts"],
            "org_name": hit["org"].strip(),
            "position": "support" if hit["verb"].lower() == "supports" else "oppose",
            "citation_url": hit["url"].strip(),
        }
        key = (citation["org_name"].lower(), citation["position"], citation["citation_url"])
        if key not in seen:
            seen.add(key)
            citations.append(citation)
    return bill, citations


class SlackReadError(RuntimeError):
    """Slack could not be read (bad token, missing scope, kept rate limiting)."""


async def _slack_pages(client: httpx.AsyncClient, token: str, method: str, **params):
    cursor = None
    while True:
        query = {"limit": 200, **params, **({"cursor": cursor} if cursor else {})}
        for _ in range(_SLACK_MAX_ATTEMPTS):
            resp = await client.get(
                _SLACK_API + method, params=query, headers={"Authorization": f"Bearer {token}"}
            )
            if resp.status_code == 429:
                await asyncio.sleep(int(resp.headers.get("Retry-After", "5")) + 1)
                continue
            data = resp.json()
            if data.get("ok"):
                break
            if data.get("error") == "ratelimited":
                await asyncio.sleep(5)
                continue
            raise SlackReadError(f"Slack {method} failed: {data.get('error')}")
        else:
            raise SlackReadError(f"Slack {method} kept rate limiting")
        for message in data.get("messages", []):
            yield message
        cursor = (data.get("response_metadata") or {}).get("next_cursor")
        if not cursor:
            return


async def fetch_threads(
    token: str, channel: str, *, roots_since: float, active_since: float = 0.0
) -> list[list[dict]]:
    """Every thread in `channel` whose root was posted since `roots_since` and whose latest reply is no
    older than `active_since` (Unix times), with all its replies.

    A citation is a reply, often posted long after its bill's thread was started, so roots are listed
    from far back (a cheap, paged history read) and replies are fetched only for threads with a reply
    since `active_since` (Slack gives each root its `latest_reply`)."""
    threads = []
    async with httpx.AsyncClient(timeout=_SLACK_TIMEOUT_SECONDS) as client:
        roots = [
            m async for m in _slack_pages(client, token, "conversations.history", channel=channel,
                                          oldest=f"{roots_since:.6f}")
            if m.get("reply_count") and float(m.get("latest_reply") or m["ts"]) >= active_since
        ]
        for root in roots:
            threads.append(
                [m async for m in _slack_pages(client, token, "conversations.replies", channel=channel,
                                               ts=root["ts"])]
            )
    return threads


def _results_match(ready: list[tuple[str, dict]], results: list[dict]) -> bool:
    """verify_org_citations returns one result per item in the order sent; check that before a result
    marks a Slack message handled, so a short or reordered list can never mark the wrong one."""
    fields = ("bill_openstates_id", "org_name", "position", "citation_url")
    return len(ready) == len(results) and all(
        all(item[f] == result.get(f) for f in fields) for (_, item), result in zip(ready, results)
    )


def _window(config: dict, now: float) -> tuple[float, float]:
    """(citations_since, roots_since): citations newer than the later of `start_date` and
    `lookback_days` ago, in threads started up to `thread_lookback_days` before that."""
    since = now - float(config.get("lookback_days", 30)) * 86400
    start_date = config.get("start_date")
    if start_date:
        since = max(since, datetime.fromisoformat(str(start_date)).replace(tzinfo=UTC).timestamp())
    return since, since - float(config.get("thread_lookback_days", 730)) * 86400


async def run_slack_citations_job(config: dict, *, trigger: str = "scheduled") -> dict:
    """One pass: read #legislation, report or send the new citations, record what is settled.

    Returns a summary dict (also logged as slack_citations_summary). Never raises; a failure that stops
    the run is logged and posted to the alerts channel."""
    if _run_lock.locked():
        logger.info("slack_citations: a run is already in progress -- skipping", trigger=trigger)
        return {"status": "skipped_already_running"}
    async with _run_lock:
        try:
            summary = await _run(config, trigger=trigger)
        except Exception as exc:
            logger.exception("slack_citations: run failed", trigger=trigger)
            post_alert(f"Slack citation pull failed: {type(exc).__name__}: {exc}"[:300], source="slack_citations")
            return {"status": "failed", "error": f"{type(exc).__name__}: {exc}"[:300]}
    logger.info("slack_citations_summary", trigger=trigger, **summary)
    return summary


async def _run(config: dict, *, trigger: str) -> dict:
    mode = config.get("mode", "report_only")
    if mode not in ("report_only", "write"):
        raise ValueError(f"slack_citations.mode must be report_only or write, not {mode!r}")
    token = os.getenv("SLACK_BOT_TOKEN", "")
    if not token:
        raise SlackReadError("SLACK_BOT_TOKEN is not set on this host")
    channel = config.get("channel_id", DEFAULT_CHANNEL)
    cap = int(config.get("max_citations_per_run", 200))
    source = config.get("source", "slack-legislation")

    citations_since, roots_since = _window(config, time.time())
    threads = await fetch_threads(token, channel, roots_since=roots_since, active_since=citations_since)

    found = []  # (citation, bill reference)
    for messages in threads:
        bill_ref, citations = parse_thread(messages)
        found += [(c, bill_ref) for c in citations if float(c["ts"]) >= citations_since]

    store = get_redis_store()
    done = await store.get_processed_slack_citations([c["ts"] for c, _ in found])
    if done is None:
        raise RuntimeError("the record of handled Slack messages could not be read (Redis); nothing sent")
    # Oldest first, so what the cap leaves for the next run is always the newest.
    new = sorted(((c, b) for c, b in found if c["ts"] not in done), key=lambda pair: float(pair[0]["ts"]))

    unmatched: Counter = Counter()
    settled_ts: list[str] = []
    ready: list[tuple[str, dict]] = []  # (message ts, verify item)
    bills: dict[tuple, tuple[dict | None, bool]] = {}
    for citation, bill_ref in new:
        if len(ready) >= cap:
            break
        if bill_ref is None:
            reason = "no_openstates_link"
        else:
            key = (bill_ref["jurisdiction"], bill_ref["session"], bill_ref["number"])
            if key not in bills:
                bills[key] = await find_bill(*key)
            bill, answered = bills[key]
            reason = None if bill else ("bill_not_found" if answered else "bill_lookup_failed")
        if reason:
            unmatched[reason] += 1
            logger.info("slack_citation_unmatched", ts=citation["ts"], reason=reason, org_name=citation["org_name"],
                        citation_url=citation["citation_url"], bill=bill_ref)
            if reason in _SETTLED_UNMATCHED:
                settled_ts.append(citation["ts"])
            continue
        ready.append((citation["ts"], {**bill, "org_name": citation["org_name"], "position": citation["position"],
                                       "citation_url": citation["citation_url"]}))

    summary = {
        "mode": mode, "threads": len(threads), "citations_in_window": len(found), "already_handled": len(found) - len(new),
        "ready": len(ready), "unmatched": dict(unmatched), "left_for_next_run": max(0, len(new) - len(ready) - sum(unmatched.values())),
    }
    if summary["left_for_next_run"]:
        # Left-overs are the newest (oldest first above); they are found again while inside lookback_days.
        logger.warning("slack_citations: cap reached, the rest wait for the next run",
                       left=summary["left_for_next_run"], cap=cap)
        handled = set(settled_ts) | {ts for ts, _ in ready}
        oldest_left = min(float(c["ts"]) for c, _ in new if c["ts"] not in handled)
        age_days = (time.time() - oldest_left) / 86400
        if age_days >= float(config.get("lookback_days", 30)) - _AGE_OUT_WARNING_DAYS:
            post_alert(
                f"Slack citation pull: {summary['left_for_next_run']} citations wait beyond the nightly cap of "
                f"{cap}; the oldest is {age_days:.0f} days old and drops out of the "
                f"{config.get('lookback_days', 30)}-day window unsent unless max_citations_per_run is raised.",
                source="slack_citations",
            )
    if mode == "report_only":
        for ts, item in ready:
            logger.info("slack_citation_would_send", ts=ts, **item)
        summary["status"] = "reported"
        return summary  # report-only writes nothing and records nothing

    run_id = f"slack-citations-{datetime.now(UTC):%Y%m%d-%H%M%S}"
    await store.set_verify_run(run_id, new_run_record(run_id, source=source, environment=None, total=len(ready)))
    result = await run_verify_org_citations_in_background(
        [item for _, item in ready], source=source, run_id=run_id, environment=None,
        broker_api_base=None, broker_api_token=None,
    )
    # A failed run marks none of its items: some may already be written, and sending them again costs a
    # verification call but cannot duplicate a row (the broker's write is an upsert on bill version,
    # organization, position and citation url).
    if result is not None and not _results_match(ready, result["results"]):
        logger.error("slack_citations: verification results do not line up with the items sent; none recorded",
                     sent=len(ready), results=len(result["results"]))
        result = None
    if result is not None:
        settled_ts += [ts for (ts, _), r in zip(ready, result["results"]) if r["outcome"] in _SETTLED_OUTCOMES]
        summary["verify_counts"] = result["counts"]
        summary["matcher"] = dict(Counter(r["resolution"]["outcome"] for r in result["results"] if "resolution" in r))
    if not await store.mark_slack_citations_processed(settled_ts):
        logger.error("slack_citations: could not record handled messages; they will be sent again", count=len(settled_ts))
    summary.update(status="finished" if result is not None else "failed", run_id=run_id, recorded_handled=len(settled_ts))
    return summary
