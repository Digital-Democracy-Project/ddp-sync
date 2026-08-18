"""Tests for ddp_sync.config's SESSION_PIPELINE_CONCURRENCY env-var handling
(AGENTS-37). config.py has two separate literals for this setting -- the
SyncSettings dataclass default and _load_from_env()'s os.getenv fallback --
that could silently drift apart; these tests exercise the actual env-parsing
path directly, not just the dataclass default already covered in
test_session_pipeline_runner.py.
"""

from __future__ import annotations

from ddp_sync.config import _load_from_env


def test_load_from_env_defaults_session_pipeline_concurrency_to_one(monkeypatch):
    """AGENTS-37: the env-var fallback must also resolve to 1 when
    SESSION_PIPELINE_CONCURRENCY is unset -- this is the literal
    os.getenv("SESSION_PIPELINE_CONCURRENCY", "1") line PR AGENTS-37
    actually changed, not just the dataclass default."""
    monkeypatch.delenv("SESSION_PIPELINE_CONCURRENCY", raising=False)
    assert _load_from_env()["session_pipeline_concurrency"] == 1


def test_load_from_env_honors_explicit_session_pipeline_concurrency_override(monkeypatch):
    """AGENTS-37 lowers the DEFAULT only -- operators must still be able to
    opt into a higher value via the env var once real concurrent MLX-LM
    throughput on the target hardware is actually benchmarked and shown
    safe. This is not a removal of configurability."""
    monkeypatch.setenv("SESSION_PIPELINE_CONCURRENCY", "4")
    assert _load_from_env()["session_pipeline_concurrency"] == 4
