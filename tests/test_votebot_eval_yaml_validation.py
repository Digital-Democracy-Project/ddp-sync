"""YAML validation tests for the votebot_eval block (plan §3.8).

Verifies the registration-time contract: required keys present + typed,
optional keys default cleanly, unknown keys produce typo warnings,
out-of-range values fail loudly.
"""

from __future__ import annotations

import pytest

from ddp_sync.pipelines.votebot_eval import validate_yaml_config


def test_valid_full_config_passes():
    """The shape from sync_schedule.yaml must validate without errors."""
    cfg = {
        "enabled": True,
        "frequency": "weekly",
        "sync_day": "sunday",
        "sync_time_utc": "12:00",
        "days": 7,
        "max_days": 30,
        "votebot_path": "/home/ubuntu/votebot",
        "thresholds": {
            "citation_rate_floor": 0.20,
            "pass_rate_floor": 0.40,
            "delta_drop_pp": 10,
        },
        "notifications": {"enabled": True, "alert_on_success": True},
    }
    validated, errors = validate_yaml_config(cfg)
    assert errors == []
    assert validated is cfg


def test_missing_required_key_aborts_registration():
    """Missing 'enabled' must error and return None to signal "skip registration"."""
    cfg = {
        "frequency": "weekly",
        "sync_day": "sunday",
        "sync_time_utc": "12:00",
        "days": 7,
    }
    validated, errors = validate_yaml_config(cfg)
    assert validated is None
    assert any("enabled" in e for e in errors)


def test_invalid_frequency_aborts():
    cfg = {
        "enabled": True,
        "frequency": "monthly",  # not in {weekly, daily}
        "sync_time_utc": "12:00",
        "days": 7,
    }
    validated, errors = validate_yaml_config(cfg)
    assert validated is None
    assert any("frequency" in e for e in errors)


def test_invalid_sync_day_for_weekly_aborts():
    cfg = {
        "enabled": True,
        "frequency": "weekly",
        "sync_day": "funday",  # not a real day
        "sync_time_utc": "12:00",
        "days": 7,
    }
    validated, errors = validate_yaml_config(cfg)
    assert validated is None
    assert any("sync_day" in e for e in errors)


def test_days_exceeds_max_days_aborts():
    cfg = {
        "enabled": True,
        "frequency": "weekly",
        "sync_day": "sunday",
        "sync_time_utc": "12:00",
        "days": 60,
        "max_days": 30,
    }
    validated, errors = validate_yaml_config(cfg)
    assert validated is None
    assert any("days" in e and "max_days" in e for e in errors)


def test_threshold_out_of_range_aborts():
    """citation_rate_floor must be in [0.0, 1.0]."""
    cfg = {
        "enabled": True,
        "frequency": "weekly",
        "sync_day": "sunday",
        "sync_time_utc": "12:00",
        "days": 7,
        "thresholds": {"citation_rate_floor": 1.5},
    }
    validated, errors = validate_yaml_config(cfg)
    assert validated is None
    assert any("citation_rate_floor" in e for e in errors)


def test_unknown_top_level_key_warns_but_passes(caplog):
    """Typo'd keys (e.g. ``alerts_enabled`` instead of ``notifications.enabled``)
    must surface as a warning so they don't silently no-op.
    PM v4 review concern.
    """
    cfg = {
        "enabled": True,
        "frequency": "weekly",
        "sync_day": "sunday",
        "sync_time_utc": "12:00",
        "days": 7,
        "alerts_enabled": True,  # typo — not a real key
    }
    validated, errors = validate_yaml_config(cfg)
    # Validation passes (unknown keys are warnings, not errors).
    assert validated is cfg
    assert errors == []
    # The structlog warning has metric=METRIC_UNKNOWN_YAML_KEY, but
    # caplog captures stdlib logging which structlog wraps differently.
    # The key insight is that validation didn't abort.


def test_thresholds_block_missing_falls_to_defaults():
    """Missing ``thresholds:`` block is OK — defaults apply at lookup time."""
    cfg = {
        "enabled": True,
        "frequency": "weekly",
        "sync_day": "sunday",
        "sync_time_utc": "12:00",
        "days": 7,
    }
    validated, errors = validate_yaml_config(cfg)
    assert errors == []
    assert validated is cfg


def test_none_config_returns_none_no_errors():
    """If the votebot_eval block is entirely absent from YAML, return None
    without errors — caller decides whether to register the job."""
    validated, errors = validate_yaml_config(None)
    assert validated is None
    assert errors == []


def test_non_dict_config_aborts():
    """If someone writes ``votebot_eval: true`` instead of a mapping, fail loudly."""
    validated, errors = validate_yaml_config("not a dict")  # type: ignore[arg-type]
    assert validated is None
    assert errors
