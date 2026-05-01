"""Unit tests for the votebot_eval pipeline (plan §3.2).

Covers the run-time logic: lock acquisition, subprocess invocation,
regression detection, Zapier alerting, metric string contracts.
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ddp_sync.pipelines import votebot_eval
from ddp_sync.pipelines.votebot_eval import (
    DEFAULT_CITATION_RATE_FLOOR,
    DEFAULT_PASS_RATE_FLOOR,
    LAST_RUN_KEY,
    LOCK_KEY,
    LOCK_SAFETY_MARGIN_S,
    METRIC_ALERT_FAILED,
    METRIC_ALERT_SENT,
    METRIC_ALERT_SKIPPED,
    METRIC_REGRESSION,
    METRIC_RUN_COMPLETED,
    METRIC_RUN_FAILED,
    METRIC_UNKNOWN_YAML_KEY,
    _compute_timeout,
    detect_regressions,
    push_eval_alert,
    run_votebot_eval,
)


# -----------------------------------------------------------------------------
# Pinned metric string contract (plan §3.6)
# -----------------------------------------------------------------------------

def test_metric_strings_are_pinned():
    """External log monitoring rules match these literal strings — typos here
    would silently disable alerts in production."""
    assert METRIC_RUN_COMPLETED == "votebot_eval.scheduled_run_completed"
    assert METRIC_REGRESSION == "votebot_eval.regression_detected"
    assert METRIC_RUN_FAILED == "votebot_eval.run_failed"
    assert METRIC_ALERT_SENT == "votebot_eval.alert_sent"
    assert METRIC_ALERT_SKIPPED == "votebot_eval.alert_skipped"
    assert METRIC_ALERT_FAILED == "votebot_eval.alert_failed"
    assert METRIC_UNKNOWN_YAML_KEY == "votebot_eval.unknown_yaml_key"


# -----------------------------------------------------------------------------
# Subprocess timeout scaling (plan §3.2 step 2)
# -----------------------------------------------------------------------------

def test_timeout_scales_with_days():
    """Timeout = max(600, 120 + days * 90). PM v5 Phase-3 build review
    widened the coefficient from 60 to 90 after empirical 30-day-window
    runtime data showed ~45-55 min on c6g.large.
    """
    assert _compute_timeout(1) == 600   # min floor wins (120+90=210 < 600)
    assert _compute_timeout(7) == 750   # 120 + 7*90 = 750
    assert _compute_timeout(10) == 1020  # 120 + 10*90 = 1020
    assert _compute_timeout(30) == 2820  # 120 + 30*90 = 2820


def test_lock_ttl_binding_is_timeout_plus_300():
    """Plan §3.4 (post PM v4) — lock TTL = subprocess timeout + 300s safety margin
    so the lock never expires mid-run. PM v3 caught a 30-min fixed TTL bug;
    PM v4 widened the margin from 120s to 300s for post-processing; PM v5
    Phase-3 build review widened the timeout coefficient from 60→90 s/day
    after empirical c6g.large 30-day-window data showed ~45-55 min runs.
    """
    assert LOCK_SAFETY_MARGIN_S == 300
    # For a 30-day window: timeout = 120 + 30*90 = 2820s; lock TTL = 3120s.
    assert _compute_timeout(30) == 2820
    assert _compute_timeout(30) + LOCK_SAFETY_MARGIN_S == 3120


# -----------------------------------------------------------------------------
# Regression detection (plan §3.2 step 5)
# -----------------------------------------------------------------------------

def test_regression_detection_clean_run_at_baseline():
    """First post-Deploy run with current production baseline (~0.27 citation,
    ~0.50 pass) MUST NOT fire any threshold-floor regression — the defaults
    (0.20/0.40) are deliberately set below baseline.
    """
    headline = {
        "citation_rate": 0.27,
        "pass_rate": 0.50,
        "bill_history_leak_count": 0,
    }
    regressions = detect_regressions(headline, last_run=None, config=None)
    assert regressions == [], f"unexpected regressions: {regressions}"


def test_regression_detection_fires_on_bill_history_leak():
    """Hard alert — any non-zero leak count fires regardless of other metrics."""
    headline = {
        "citation_rate": 0.99,
        "pass_rate": 0.99,
        "bill_history_leak_count": 1,
    }
    regressions = detect_regressions(headline, last_run=None, config=None)
    assert len(regressions) == 1
    assert regressions[0]["type"] == "leak_canary"
    assert regressions[0]["metric"] == "bill_history_leak_count"
    assert regressions[0]["value"] == 1


def test_regression_detection_fires_on_citation_floor():
    """Citation rate below the configured floor triggers a fixed_floor regression."""
    headline = {"citation_rate": 0.10, "pass_rate": 0.50, "bill_history_leak_count": 0}
    config = {"thresholds": {"citation_rate_floor": 0.30}}
    regressions = detect_regressions(headline, last_run=None, config=config)
    assert any(
        r["type"] == "fixed_floor" and r["metric"] == "citation_rate"
        for r in regressions
    )


def test_regression_detection_fires_on_pass_floor():
    headline = {"citation_rate": 0.99, "pass_rate": 0.30, "bill_history_leak_count": 0}
    regressions = detect_regressions(headline, last_run=None, config=None)
    assert any(
        r["type"] == "fixed_floor" and r["metric"] == "pass_rate"
        for r in regressions
    )


def test_regression_detection_fires_on_delta_drop():
    """Delta check — citation_rate dropping >10pp from last run fires
    even when both values are above the fixed floor."""
    headline = {"citation_rate": 0.40, "pass_rate": 0.99, "bill_history_leak_count": 0}
    last_run = {"citation_rate": 0.55, "pass_rate": 0.99}
    regressions = detect_regressions(headline, last_run=last_run, config=None)
    assert any(
        r["type"] == "delta_drop" and r["metric"] == "citation_rate"
        for r in regressions
    )


def test_regression_detection_threshold_defaults_below_baseline():
    """Plan §3.2 step 5 — defaults are 0.20/0.40, set below current production
    baseline (~0.27 citation, ~0.50 pass) so the first post-deploy run isn't
    a false-positive regression alert. PM v2 review concern #1.
    """
    assert DEFAULT_CITATION_RATE_FLOOR == 0.20
    assert DEFAULT_PASS_RATE_FLOOR == 0.40


# -----------------------------------------------------------------------------
# Zapier alerting (plan §3.7)
# -----------------------------------------------------------------------------

def test_push_eval_alert_skipped_when_webhook_empty():
    """Empty webhook URL must not raise — log and return False."""
    ok = push_eval_alert(
        webhook_url="",
        headline={"n_query_processed": 10, "citation_rate": 0.5},
        regression_details=[],
        report_path="/tmp/x.json",
        trigger="scheduled",
    )
    assert ok is False


def test_push_eval_alert_sets_routing_flags():
    """The three Zapier filter flags must be set correctly from inputs."""
    captured = {}

    def fake_post(url, json, timeout):
        captured["payload"] = json
        m = MagicMock()
        m.status_code = 200
        return m

    with patch("ddp_sync.pipelines.votebot_eval.requests.post", side_effect=fake_post):
        push_eval_alert(
            webhook_url="https://hooks.zapier.example/x",
            headline={
                "n_query_processed": 100,
                "citation_rate": 0.3,
                "pass_rate": 0.5,
                "cache_hit_rate": 0.2,
                "bill_history_leak_count": 5,  # triggers leak flag
                "window_days": 7,
                "window_start": "2026-04-23",
                "window_end": "2026-04-30",
                "p50_latency_ms_rag_only": 5000,
                "p95_latency_ms_rag_only": 15000,
            },
            regression_details=[
                {"type": "fixed_floor", "metric": "citation_rate", "value": 0.3, "threshold": 0.5}
            ],
            report_path="/tmp/x.json",
            trigger="scheduled",
        )

    p = captured["payload"]
    assert p["alert_type"] == "votebot_eval_complete"
    assert p["on_regression"] is True
    assert p["on_bill_history_leak"] is True
    assert p["on_failure"] is False  # no error passed
    # Pre-formatted warnings are non-empty when their flags fire.
    assert "regression_warning" in p
    assert p["regression_warning"]
    assert p["leak_warning"]
    assert p["failure_warning"] == ""


def test_push_eval_alert_failure_flag_set_when_error():
    """When called with error=..., on_failure must be True and warning populated."""
    captured = {}

    def fake_post(url, json, timeout):
        captured["payload"] = json
        m = MagicMock()
        m.status_code = 200
        return m

    with patch("ddp_sync.pipelines.votebot_eval.requests.post", side_effect=fake_post):
        push_eval_alert(
            webhook_url="https://hooks.zapier.example/x",
            headline={"n_query_processed": 0, "citation_rate": 0, "pass_rate": 0,
                      "cache_hit_rate": 0, "bill_history_leak_count": 0,
                      "p50_latency_ms_rag_only": 0, "p95_latency_ms_rag_only": 0},
            regression_details=[],
            report_path="/tmp/x.json",
            trigger="manual",
            error="subprocess_timeout",
        )

    p = captured["payload"]
    assert p["on_failure"] is True
    assert "subprocess_timeout" in p["failure_warning"]


def test_push_eval_alert_returns_false_on_non_2xx():
    """Non-2xx response must return False without raising."""
    def fake_post(*a, **kw):
        m = MagicMock()
        m.status_code = 500
        m.text = "Internal Server Error"
        return m

    with patch("ddp_sync.pipelines.votebot_eval.requests.post", side_effect=fake_post):
        ok = push_eval_alert(
            webhook_url="https://hooks.zapier.example/x",
            headline={"n_query_processed": 0, "citation_rate": 0, "pass_rate": 0,
                      "cache_hit_rate": 0, "bill_history_leak_count": 0},
            regression_details=[],
            report_path="/tmp/x.json",
            trigger="scheduled",
        )
    assert ok is False


def test_push_eval_alert_returns_false_on_network_exception():
    """Connection errors must be swallowed."""
    def fake_post(*a, **kw):
        raise ConnectionError("network down")

    with patch("ddp_sync.pipelines.votebot_eval.requests.post", side_effect=fake_post):
        ok = push_eval_alert(
            webhook_url="https://hooks.zapier.example/x",
            headline={"n_query_processed": 0, "citation_rate": 0, "pass_rate": 0,
                      "cache_hit_rate": 0, "bill_history_leak_count": 0},
            regression_details=[],
            report_path="/tmp/x.json",
            trigger="scheduled",
        )
    assert ok is False


# -----------------------------------------------------------------------------
# run_votebot_eval — concurrency lock + subprocess + headline parse
# -----------------------------------------------------------------------------

class FakeRedisClient:
    def __init__(self):
        self.store: dict[str, bytes] = {}
        self.set_ex_history: list[tuple[str, int]] = []

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.store:
            return False
        self.store[key] = value if isinstance(value, bytes) else str(value).encode()
        if ex is not None:
            self.set_ex_history.append((key, ex))
        return True

    async def get(self, key):
        return self.store.get(key)

    async def delete(self, key):
        return 1 if self.store.pop(key, None) is not None else 0


@pytest.fixture
def fake_redis_store():
    rs = MagicMock()
    rs._client = FakeRedisClient()
    rs.set_flow_status = AsyncMock()
    return rs


@pytest.mark.asyncio
async def test_run_votebot_eval_lock_contention_returns_already_running(
    fake_redis_store, monkeypatch, tmp_path
):
    """Plan §3.4 — second invocation while first holds the lock returns
    ``error="already_running"`` with the existing run_id."""
    monkeypatch.setattr(
        "ddp_sync.services.redis_store.get_redis_store", lambda: fake_redis_store
    )
    # Pre-seed the lock with a fake "in-flight" run.
    fake_redis_store._client.store[LOCK_KEY] = b"existing-run-id"

    # Path validation must also pass before lock check.
    monkeypatch.setattr(
        "ddp_sync.pipelines.votebot_eval.validate_votebot_path",
        lambda p: (True, None),
    )

    result = await run_votebot_eval(days=7, settings=MagicMock())
    assert result["success"] is False
    assert result["error"] == "already_running"
    assert result["current_run_id"] == "existing-run-id"


@pytest.mark.asyncio
async def test_run_votebot_eval_path_invalid_returns_503_marker(
    monkeypatch
):
    """Path validation is live — even after registration-time validation,
    the path can go stale and run_votebot_eval re-checks. Returns
    ``error="votebot_path_invalid"`` so the manual-trigger endpoint can
    translate to 503."""
    monkeypatch.setattr(
        "ddp_sync.pipelines.votebot_eval.validate_votebot_path",
        lambda p: (False, "directory does not exist"),
    )
    result = await run_votebot_eval(days=7, settings=MagicMock())
    assert result["success"] is False
    assert result["error"] == "votebot_path_invalid"


@pytest.mark.asyncio
async def test_run_votebot_eval_subprocess_timeout_writes_failure_status(
    fake_redis_store, monkeypatch, tmp_path
):
    """Subprocess timeout → flow_status records 'failed', metric event emitted,
    lock released, function returns instead of raising."""
    monkeypatch.setattr(
        "ddp_sync.services.redis_store.get_redis_store", lambda: fake_redis_store
    )
    monkeypatch.setattr(
        "ddp_sync.pipelines.votebot_eval.validate_votebot_path",
        lambda p: (True, None),
    )
    monkeypatch.setattr(
        "ddp_sync.pipelines.votebot_eval.resolve_votebot_path",
        lambda c: str(tmp_path),
    )
    # Make Path(path).expanduser() / .venv / bin / python work in the cmd build
    (tmp_path / ".venv" / "bin").mkdir(parents=True, exist_ok=True)
    (tmp_path / ".venv" / "bin" / "python").touch()
    (tmp_path / "scripts").mkdir(parents=True, exist_ok=True)
    (tmp_path / "scripts" / "evaluate_production.py").touch()

    import subprocess as _sp
    def fake_run(*a, **kw):
        raise _sp.TimeoutExpired(cmd=a[0] if a else [], timeout=kw.get("timeout"))
    monkeypatch.setattr("ddp_sync.pipelines.votebot_eval.subprocess.run", fake_run)

    result = await run_votebot_eval(days=7, settings=MagicMock())
    assert result["success"] is False
    assert result["error"] == "subprocess_timeout"
    fake_redis_store.set_flow_status.assert_awaited()
    # Lock should be released even on failure path.
    assert LOCK_KEY not in fake_redis_store._client.store


@pytest.mark.asyncio
async def test_run_votebot_eval_subprocess_uses_start_new_session(
    fake_redis_store, monkeypatch, tmp_path
):
    """PM v2 review — subprocess.run must be called with start_new_session=True
    so timeout/SIGKILL propagates to grandchildren."""
    monkeypatch.setattr(
        "ddp_sync.services.redis_store.get_redis_store", lambda: fake_redis_store
    )
    monkeypatch.setattr(
        "ddp_sync.pipelines.votebot_eval.validate_votebot_path",
        lambda p: (True, None),
    )
    monkeypatch.setattr(
        "ddp_sync.pipelines.votebot_eval.resolve_votebot_path",
        lambda c: str(tmp_path),
    )
    (tmp_path / ".venv" / "bin").mkdir(parents=True, exist_ok=True)
    (tmp_path / ".venv" / "bin" / "python").touch()
    (tmp_path / "scripts").mkdir(parents=True, exist_ok=True)
    (tmp_path / "scripts" / "evaluate_production.py").touch()

    captured_kwargs = {}
    def fake_run(*a, **kw):
        captured_kwargs.update(kw)
        m = MagicMock()
        m.returncode = 1  # non-zero so we exit before JSON parse
        m.stderr = b""
        m.stdout = b""
        return m
    monkeypatch.setattr("ddp_sync.pipelines.votebot_eval.subprocess.run", fake_run)

    await run_votebot_eval(days=7, settings=MagicMock())
    assert captured_kwargs.get("start_new_session") is True


@pytest.mark.asyncio
async def test_subprocess_runs_via_asyncio_to_thread(
    fake_redis_store, monkeypatch, tmp_path
):
    """PM v5 Phase-3 build review HIGH concern #1 — subprocess.run is
    blocking; calling it directly from an async function freezes the
    event loop for minutes, starving other ddp-sync API + cron tasks.
    Mirror legislator_bio's asyncio.to_thread pattern (line 704).

    This test verifies asyncio.to_thread is used by spying on it and
    asserting the wrapped subprocess.run is reached via that path.
    """
    monkeypatch.setattr(
        "ddp_sync.services.redis_store.get_redis_store", lambda: fake_redis_store
    )
    monkeypatch.setattr(
        "ddp_sync.pipelines.votebot_eval.validate_votebot_path",
        lambda p: (True, None),
    )
    monkeypatch.setattr(
        "ddp_sync.pipelines.votebot_eval.resolve_votebot_path",
        lambda c: str(tmp_path),
    )
    (tmp_path / ".venv" / "bin").mkdir(parents=True, exist_ok=True)
    (tmp_path / ".venv" / "bin" / "python").touch()
    (tmp_path / "scripts").mkdir(parents=True, exist_ok=True)
    (tmp_path / "scripts" / "evaluate_production.py").touch()

    to_thread_calls = []
    real_to_thread = asyncio.to_thread

    async def spying_to_thread(func, *args, **kwargs):
        to_thread_calls.append(func.__name__)
        return await real_to_thread(func, *args, **kwargs)

    monkeypatch.setattr(
        "ddp_sync.pipelines.votebot_eval.asyncio.to_thread", spying_to_thread
    )

    def fake_run(*a, **kw):
        m = MagicMock()
        m.returncode = 1  # quick exit before JSON parse
        m.stderr = b""
        return m
    monkeypatch.setattr("ddp_sync.pipelines.votebot_eval.subprocess.run", fake_run)

    await run_votebot_eval(days=7, settings=MagicMock())
    # The orchestrator must wrap blocking calls (subprocess.run +
    # push_eval_alert) in asyncio.to_thread. Both must show up in the
    # spy log; the subprocess shows as "fake_run" because of the
    # monkeypatch above. push_eval_alert is the most reliable signal
    # that the alerting wrap is in place too.
    assert "push_eval_alert" in to_thread_calls, (
        f"expected push_eval_alert to be invoked via asyncio.to_thread; "
        f"got calls: {to_thread_calls}"
    )
    # And there must be at least one subprocess wrap before that.
    assert len(to_thread_calls) >= 2, (
        f"expected at least 2 to_thread invocations (subprocess + alert); "
        f"got {to_thread_calls}"
    )


@pytest.mark.asyncio
async def test_subprocess_zero_exit_but_no_report_returns_parse_error(
    fake_redis_store, monkeypatch, tmp_path
):
    """PM v5 Phase-3 build review LOW concern #4 — subprocess can exit 0
    yet write nothing (OOM mid-write, disk full, etc.). The pipeline must
    surface this as a parse_error, not silently succeed."""
    monkeypatch.setattr(
        "ddp_sync.services.redis_store.get_redis_store", lambda: fake_redis_store
    )
    monkeypatch.setattr(
        "ddp_sync.pipelines.votebot_eval.validate_votebot_path",
        lambda p: (True, None),
    )
    monkeypatch.setattr(
        "ddp_sync.pipelines.votebot_eval.resolve_votebot_path",
        lambda c: str(tmp_path),
    )
    (tmp_path / ".venv" / "bin").mkdir(parents=True, exist_ok=True)
    (tmp_path / ".venv" / "bin" / "python").touch()
    (tmp_path / "scripts").mkdir(parents=True, exist_ok=True)
    (tmp_path / "scripts" / "evaluate_production.py").touch()

    def fake_run(*a, **kw):
        # Subprocess "succeeds" but writes no file.
        m = MagicMock()
        m.returncode = 0
        m.stderr = b""
        m.stdout = b""
        return m
    monkeypatch.setattr("ddp_sync.pipelines.votebot_eval.subprocess.run", fake_run)

    result = await run_votebot_eval(days=7, settings=MagicMock())
    assert result["success"] is False
    assert result["error"] == "parse_error"
    assert "report not written" in result["detail"].lower() or "missing" in result["detail"].lower()


@pytest.mark.asyncio
async def test_run_votebot_eval_lock_ttl_matches_timeout_plus_safety(
    fake_redis_store, monkeypatch, tmp_path
):
    """For a 30-day run (timeout 2820s after PM v5 widening), the lock TTL
    must be 2820 + 300 = 3120s. Catches the silent double-run bug PM v3 +
    v4 reviews surfaced.
    """
    monkeypatch.setattr(
        "ddp_sync.services.redis_store.get_redis_store", lambda: fake_redis_store
    )
    monkeypatch.setattr(
        "ddp_sync.pipelines.votebot_eval.validate_votebot_path",
        lambda p: (True, None),
    )
    monkeypatch.setattr(
        "ddp_sync.pipelines.votebot_eval.resolve_votebot_path",
        lambda c: str(tmp_path),
    )
    (tmp_path / ".venv" / "bin").mkdir(parents=True, exist_ok=True)
    (tmp_path / ".venv" / "bin" / "python").touch()
    (tmp_path / "scripts").mkdir(parents=True, exist_ok=True)
    (tmp_path / "scripts" / "evaluate_production.py").touch()

    def fake_run(*a, **kw):
        m = MagicMock()
        m.returncode = 1
        m.stderr = b""
        return m
    monkeypatch.setattr("ddp_sync.pipelines.votebot_eval.subprocess.run", fake_run)

    await run_votebot_eval(days=30, settings=MagicMock())
    # At least one ex= entry should be the lock-set call with TTL=3120.
    set_calls = fake_redis_store._client.set_ex_history
    lock_set_calls = [(k, ex) for (k, ex) in set_calls if k == LOCK_KEY]
    assert lock_set_calls, "lock was not set with an ex parameter"
    assert lock_set_calls[0][1] == 3120, (
        f"expected lock TTL 2820+300=3120, got {lock_set_calls[0][1]}"
    )
