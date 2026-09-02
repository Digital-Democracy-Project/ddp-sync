"""Tests for ddp_sync.config's SESSION_PIPELINE_CONCURRENCY env-var handling
(AGENTS-37). config.py has two separate literals for this setting -- the
SyncSettings dataclass default and _load_from_env()'s os.getenv fallback --
that could silently drift apart; these tests exercise the actual env-parsing
path directly, not just the dataclass default already covered in
test_session_pipeline_runner.py.
"""

from __future__ import annotations

from unittest.mock import patch

from ddp_sync.config import _load_from_env, get_settings


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


def test_load_from_env_defaults_every_sync51_task_flag_to_true(monkeypatch):
    """SYNC-51: every per-task flag must default True -- merging this change alone must
    change nothing on any existing host (Mac, EC2-civic), since none of them will have
    these vars set in their .env yet."""
    flag_env_vars = [
        "BILL_SYNC_ENABLED",
        "LEGISLATOR_SYNC_ENABLED",
        "LEGISLATOR_BIO_SYNC_ENABLED",
        "ORGANIZATION_SYNC_ENABLED",
        "VOATZ_SYNC_ENABLED",
        "WEBFLOW_BATCH_ENABLED",
        "VOTEBOT_EVAL_ENABLED",
        "API_HEALTH_CHECK_ENABLED",
        "OPENSTATES_SCRAPE_ENABLED",
        "OPENSTATES_ARCHIVE_ENABLED",
        "MI_COOKIE_PUBLISH_ENABLED",
        "SESSION_PIPELINE_BATCH_ENABLED",
    ]
    for var in flag_env_vars:
        monkeypatch.delenv(var, raising=False)

    loaded = _load_from_env()
    for var in flag_env_vars:
        key = var.lower()
        assert loaded[key] is True, f"{key} must default True"


def test_load_from_env_honors_explicit_task_flag_opt_out(monkeypatch):
    """The actual SYNC-51 use case: a host's own .env opts a specific task out."""
    monkeypatch.setenv("VOATZ_SYNC_ENABLED", "false")
    monkeypatch.setenv("WEBFLOW_BATCH_ENABLED", "false")
    monkeypatch.delenv("OPENSTATES_SCRAPE_ENABLED", raising=False)

    loaded = _load_from_env()
    assert loaded["voatz_sync_enabled"] is False
    assert loaded["webflow_batch_enabled"] is False
    assert loaded["openstates_scrape_enabled"] is True


def test_task_flags_still_apply_when_secrets_manager_supplies_the_base_config(monkeypatch):
    """SYNC-51, found live on the EC2-broker host: get_settings() picks EITHER Secrets Manager
    OR .env for the whole config, never both -- so on any host where Secrets Manager succeeds
    (every EC2 host with an instance role that can reach it), _load_from_env() -- and every
    os.getenv() call inside it -- never ran, making the 12 per-task flags permanently inert
    exactly where they're needed most. Setting the env vars in the container's real
    environment had zero effect. This reproduces that exact scenario: a Secrets Manager
    payload that predates SYNC-51 (no flag keys in it at all, matching the real
    ddp-sync/credentials secret today) combined with real env vars set in the process."""
    monkeypatch.setenv("VOATZ_SYNC_ENABLED", "false")
    monkeypatch.setenv("OPENSTATES_SCRAPE_ENABLED", "true")
    get_settings.cache_clear()

    with patch(
        "ddp_sync.config._load_from_secrets_manager",
        return_value={"api_key": "from-secrets-manager"},  # no flag keys -- the real secret's shape
    ):
        settings = get_settings()

    try:
        assert settings.api_key == "from-secrets-manager"  # confirms Secrets Manager path was taken
        assert settings.voatz_sync_enabled is False
        assert settings.openstates_scrape_enabled is True
        # A flag with no env var set at all still falls back to the dataclass default (True),
        # not to whatever Secrets Manager omitted -- there's no flag key in that payload either.
        assert settings.webflow_batch_enabled is True
    finally:
        get_settings.cache_clear()
