"""Path resolution + validation tests (plan §3.9).

Covers env > YAML > default precedence and the file-system existence checks
done at scheduler.start() time + manual-trigger time.
"""

from __future__ import annotations

import os

import pytest

from ddp_sync.pipelines.votebot_eval import (
    DEFAULT_VOTEBOT_PATH,
    resolve_votebot_path,
    validate_votebot_path,
)


def test_resolve_env_takes_precedence(monkeypatch):
    monkeypatch.setenv("VOTEBOT_PATH", "/env/path")
    assert resolve_votebot_path({"votebot_path": "/yaml/path"}) == "/env/path"


def test_resolve_yaml_when_no_env(monkeypatch):
    monkeypatch.delenv("VOTEBOT_PATH", raising=False)
    assert resolve_votebot_path({"votebot_path": "/yaml/path"}) == "/yaml/path"


def test_resolve_default_when_no_env_no_yaml(monkeypatch):
    monkeypatch.delenv("VOTEBOT_PATH", raising=False)
    assert resolve_votebot_path({}) == DEFAULT_VOTEBOT_PATH
    assert resolve_votebot_path(None) == DEFAULT_VOTEBOT_PATH


def test_validate_missing_directory_fails(tmp_path):
    bad = tmp_path / "doesnotexist"
    ok, err = validate_votebot_path(str(bad))
    assert ok is False
    assert "does not exist" in err or "not a directory" in err


def test_validate_missing_venv_fails(tmp_path):
    """Path exists but no .venv/bin/python — plan §3.9 must reject."""
    ok, err = validate_votebot_path(str(tmp_path))
    assert ok is False
    assert "venv" in err or "python" in err


def test_validate_missing_script_fails(tmp_path):
    """venv exists but no scripts/evaluate_production.py — must reject."""
    venv_bin = tmp_path / ".venv" / "bin"
    venv_bin.mkdir(parents=True)
    py = venv_bin / "python"
    py.write_text("")
    py.chmod(0o755)
    # No scripts/ dir at all.
    ok, err = validate_votebot_path(str(tmp_path))
    assert ok is False
    assert "evaluate_production" in err or "script" in err


def test_validate_happy_path(tmp_path):
    """All three required artifacts present → ok."""
    venv_bin = tmp_path / ".venv" / "bin"
    venv_bin.mkdir(parents=True)
    py = venv_bin / "python"
    py.write_text("")
    py.chmod(0o755)
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "evaluate_production.py").write_text("")
    ok, err = validate_votebot_path(str(tmp_path))
    assert ok is True
    assert err is None
