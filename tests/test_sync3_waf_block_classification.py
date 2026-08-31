"""SYNC-3: classify_failure_reason() must actually see a real WAF block.

The OPEN-22 escalation (window_size/threshold in sync_schedule.yaml) is wired but was dead
code: a real WafBlockDetected signal only ever reached scraper.log -- redirected there inside
run-scrape.sh's own scrape_attempt() tee pipeline -- never run-scrape.sh's own external
stdout/stderr, which is the only thing classify_failure_reason() could see. So a genuine block
always classified as generic nonzero_exit_other and the windowed escalation could never
accumulate the waf_block records it counts. Confirmed still true against current main (no
change to classify_failure_reason() or _run_scrape()'s stderr-only capture since 07fde76,
the merge the ticket names).

Covers the ticket's two evidence-bar items:
  1. A realistic blocked-run scraper.log tail, replayed through the classifier, now returns
     "waf_block" instead of "nonzero_exit_other".
  2. Seeding sync_schedule.yaml's window_size=4/threshold=3 worth of consecutive block
     records -- each built the real way, from a clean stderr plus a scraper.log-only WAF
     signature via _run_scrape() itself, not a synthetic failure_reason string -- fires the
     distinct sustained-block Slack alert (_alert_sustained_block).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from ddp_sync.pipelines.openstates_scrape import (
    _check_sustained_blocking,
    _read_scraper_log_tail,
    _run_scrape,
    classify_failure_reason,
)

# A realistic tail of run-scrape.sh's shared scraper.log for a real MI WAF block.
#
# Reconstructed from two real sources rather than invented text:
#   * the raised exception, openstates.utils.waf_circuit_breaker.raise_if_waf_block_threshold_
#     reached(): f"{scrape_label} aborted: {consecutive_blocks} consecutive WAF blocks detected
#     {fetch_description}" -- called from scrapers/mi/_waf_circuit_breaker.py's
#     _register_waf_block_or_abort(), which is what MI's bills.py calls once its own
#     MAX_CONSECUTIVE_WAF_BLOCKS threshold is reached.
#   * run-scrape.sh's own OPEN-53 classification log() line (run-scrape.sh ~line 1091):
#     "Failure for $STATE classified as a WAF block — terminal, will not be retried (OPEN-53)".
#
# Ordered the way run-scrape.sh actually writes them: scrape_attempt()'s
# `os-update ... 2>&1 | tee "$SCRAPE_OUT" >> scraper.log` appends the raw os-update traceback
# (including the ScrapeError line) DURING the scrape; run-scrape.sh's own log() lines (fastmode
# retry, then the failure + WAF classification lines) are appended AFTER scrape_attempt()
# returns, once both attempts have failed.
REALISTIC_BLOCKED_RUN_LOG_TAIL = """\
[2026-08-30 02:14:07] run-scrape.sh: checkout=/Users/agentsmith/Developer/repos/ddp-open-states log_dir=/Users/agentsmith/Developer/repos/ddp-open-states/logs
Traceback (most recent call last):
  File "/Users/agentsmith/Developer/repos/ddp-open-states/openstates-scrapers/scrapers/mi/bills.py", line 233, in scrape_bill
    self._register_waf_block_or_abort(exc, item_label=bill_id, scrape_label="MI bill scrape", fetch_description="fetching bill pages")
  File "/Users/agentsmith/Developer/repos/ddp-open-states/openstates-scrapers/scrapers/mi/_waf_circuit_breaker.py", line 58, in _register_waf_block_or_abort
    raise_if_waf_block_threshold_reached(
  File "/Users/agentsmith/Developer/repos/ddp-open-states/openstates-core/openstates/utils/waf_circuit_breaker.py", line 27, in raise_if_waf_block_threshold_reached
    raise ScrapeError(
openstates.exceptions.ScrapeError: MI bill scrape aborted: 3 consecutive WAF blocks detected fetching bill pages
[2026-08-30 02:14:07] Scrape failed, retrying with --fastmode (using local cache)...
[2026-08-30 02:19:41] ERROR: scrape/import failed for mi
[2026-08-30 02:19:41] Failure for mi classified as a WAF block — terminal, will not be retried (OPEN-53)
"""


# -- Evidence bar item 1: the classifier, handed a realistic scraper.log tail --


def test_realistic_blocked_run_log_tail_classifies_as_waf_block():
    assert classify_failure_reason("exit_code_90", REALISTIC_BLOCKED_RUN_LOG_TAIL) == "waf_block"


def test_clean_external_stderr_alone_still_misclassifies():
    """Pins the actual bug: run-scrape.sh's own external stderr (all classify_failure_reason()
    could see before this fix) is unrelated boilerplate for a WAF block -- the signature only
    ever lands in scraper.log. This must keep returning nonzero_exit_other; if it ever started
    returning waf_block, the fixture below would no longer be testing what SYNC-3 fixes."""
    clean_stderr = "exit status 1\n"
    assert classify_failure_reason("exit_code_90", clean_stderr) == "nonzero_exit_other"


def test_realistic_log_tail_concatenated_with_clean_stderr_still_classifies_as_waf_block():
    """The actual shape _run_scrape() now builds: clean stderr + scraper.log tail, concatenated."""
    combined = "exit status 1\n" + "\n" + REALISTIC_BLOCKED_RUN_LOG_TAIL
    assert classify_failure_reason("exit_code_90", combined) == "waf_block"


# -- _read_scraper_log_tail --


def test_read_scraper_log_tail_reads_the_realistic_fixture(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    (log_dir / "scraper.log").write_text(REALISTIC_BLOCKED_RUN_LOG_TAIL)

    tail = _read_scraper_log_tail(str(tmp_path))

    assert "consecutive waf blocks detected" in tail.lower()


def test_read_scraper_log_tail_only_reads_the_tail(tmp_path):
    """Bounded read: a shared, actively-appended, 50MB-rotated log must not be read in full."""
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    noise = "x" * 20000 + "\n"
    (log_dir / "scraper.log").write_text(noise + REALISTIC_BLOCKED_RUN_LOG_TAIL)

    tail = _read_scraper_log_tail(str(tmp_path), tail_bytes=4096)

    assert "consecutive waf blocks detected" in tail.lower()
    assert "x" * 20000 not in tail


def test_read_scraper_log_tail_missing_file_returns_empty(tmp_path):
    # No logs/ dir at all -- e.g. a jurisdiction that has never completed a run.
    assert _read_scraper_log_tail(str(tmp_path)) == ""


# -- End-to-end through _run_scrape() --


@pytest.mark.asyncio
async def test_run_scrape_classifies_waf_block_from_scraper_log_when_stderr_is_clean(tmp_path):
    """Before SYNC-3 this classified as nonzero_exit_other: _run_with_group_kill's stderr is
    ordinary run-scrape.sh boilerplate, and only the scraper.log this run wrote to (simulated
    here, same as a real run-scrape.sh invocation would leave behind) carries the WAF marker."""
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    (log_dir / "scraper.log").write_text(REALISTIC_BLOCKED_RUN_LOG_TAIL)

    with (
        patch(
            "ddp_sync.pipelines.openstates_scrape._run_with_group_kill",
            return_value=(90, b"", b"exit status 1\n", False, False),
        ),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure"),
    ):
        result = await _run_scrape("mi", None, str(tmp_path), timeout_s=10)

    assert result["success"] is False
    assert result["failure_reason"] == "waf_block"


# -- Evidence bar item 2: the windowed escalation actually fires --


@pytest.mark.asyncio
async def test_sustained_block_alert_fires_after_window_size_4_threshold_3(tmp_path):
    """Seed sync_schedule.yaml's window_size=4/threshold=3 worth of consecutive block records,
    each produced the real way (via _run_scrape(), from a clean stderr plus a scraper.log-only
    WAF signature -- not a hand-written failure_reason), and confirm the distinct
    sustained-block Slack alert (_alert_sustained_block) actually fires."""
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    (log_dir / "scraper.log").write_text(REALISTIC_BLOCKED_RUN_LOG_TAIL)

    results = []
    with (
        patch(
            "ddp_sync.pipelines.openstates_scrape._run_with_group_kill",
            return_value=(90, b"", b"exit status 1\n", False, False),
        ),
        patch("ddp_sync.pipelines.openstates_scrape._alert_scrape_failure"),
    ):
        for _ in range(4):
            results.append(await _run_scrape("mi", None, str(tmp_path), timeout_s=10))

    assert [r["failure_reason"] for r in results] == ["waf_block"] * 4

    # A minimal fake of get_run_history/append_run_history's real contract (append, then read
    # back the accumulated list) -- not a mock of the escalation logic itself, which stays real.
    fake_history: list[dict] = []

    async def _append(flow_key, jurisdiction, record, max_len):
        fake_history.append(record)

    async def _get(flow_key, jurisdiction):
        return fake_history

    mock_redis = AsyncMock()
    mock_redis.append_run_history.side_effect = _append
    mock_redis.get_run_history.side_effect = _get

    with (
        patch("ddp_sync.services.redis_store.get_redis_store", return_value=mock_redis),
        patch("ddp_sync.pipelines.openstates_scrape._alert_sustained_block") as mock_alert,
    ):
        for result in results:
            await _check_sustained_blocking(
                "openstates_secondary_scrapes",
                ["mi"],
                [result],
                {"secondary": {"escalation": {"window_size": 4, "threshold": 3}}},
            )

    assert mock_alert.call_count >= 1
    # Last call: all 4 of the last 4 runs blocked.
    assert mock_alert.call_args.args == ("mi", 4, 4)
