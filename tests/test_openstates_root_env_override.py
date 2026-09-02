"""Tests for OPEN-243: _get_root()'s OPENSTATES_ROOT env-var override.

openstates_root is inherently per-host, but sync_schedule.yaml is a single, checked-in
file every ddp-sync host pulls via git identically -- a value set there can only ever be
one host's real path at a time. Found live on the EC2-broker host: the checked-in value
was the Mac Studio's path, breaking cloud_loader.py's invocation there.
"""

from __future__ import annotations

from ddp_sync.pipelines.openstates_scrape import DEFAULT_OPENSTATES_ROOT, _get_root


def test_get_root_falls_back_to_config_value_when_env_unset(monkeypatch):
    monkeypatch.delenv("OPENSTATES_ROOT", raising=False)
    assert _get_root({"openstates_root": "/opt/ddp-open-states"}) == "/opt/ddp-open-states"


def test_get_root_falls_back_to_default_when_neither_env_nor_config_set(monkeypatch):
    monkeypatch.delenv("OPENSTATES_ROOT", raising=False)
    assert _get_root({}) == DEFAULT_OPENSTATES_ROOT
    assert _get_root(None) == DEFAULT_OPENSTATES_ROOT


def test_get_root_env_var_wins_even_when_config_sets_a_different_path(monkeypatch):
    """The actual OPEN-243 scenario: sync_schedule.yaml's checked-in openstates_root is
    the Mac Studio's path (shared across every host), but this host's real path is
    /opt/ddp-open-states -- the env var must win regardless of what the shared config says."""
    monkeypatch.setenv("OPENSTATES_ROOT", "/opt/ddp-open-states")
    assert _get_root({"openstates_root": "/Users/agentsmith/Developer/repos/ddp-open-states"}) == (
        "/opt/ddp-open-states"
    )


def test_get_root_empty_env_value_falls_back_rather_than_returning_empty(monkeypatch):
    """pm-review: an empty string is a falsy env value, not a real override -- a host with
    OPENSTATES_ROOT="" in its .env (e.g. an unset template variable) must still fall back to
    config/default rather than handing an empty path to cloud_loader.py."""
    monkeypatch.setenv("OPENSTATES_ROOT", "")
    assert _get_root({"openstates_root": "/opt/ddp-open-states"}) == "/opt/ddp-open-states"
