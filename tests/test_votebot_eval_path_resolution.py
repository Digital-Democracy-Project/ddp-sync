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
    ok, err, venv_python = validate_votebot_path(str(bad))
    assert ok is False
    assert venv_python is None
    assert "does not exist" in err or "not a directory" in err


def test_validate_missing_venv_fails(tmp_path):
    """Path exists but no venv at either .venv/ or venv/ — plan §3.9 must reject."""
    ok, err, venv_python = validate_votebot_path(str(tmp_path))
    assert ok is False
    assert venv_python is None
    assert "venv" in err or "python" in err


def test_validate_missing_script_fails(tmp_path):
    """venv exists but no scripts/evaluate_production.py — must reject."""
    venv_bin = tmp_path / ".venv" / "bin"
    venv_bin.mkdir(parents=True)
    py = venv_bin / "python"
    py.write_text("")
    py.chmod(0o755)
    # No scripts/ dir at all.
    ok, err, venv_python = validate_votebot_path(str(tmp_path))
    assert ok is False
    assert venv_python is None
    assert "evaluate_production" in err or "script" in err


def test_validate_happy_path_with_dot_venv(tmp_path):
    """Local-dev convention: ``.venv/bin/python`` (with leading dot)."""
    venv_bin = tmp_path / ".venv" / "bin"
    venv_bin.mkdir(parents=True)
    py = venv_bin / "python"
    py.write_text("")
    py.chmod(0o755)
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "evaluate_production.py").write_text("")
    ok, err, venv_python = validate_votebot_path(str(tmp_path))
    assert ok is True
    assert err is None
    assert venv_python == tmp_path / ".venv" / "bin" / "python"


def test_validate_happy_path_with_no_dot_venv(tmp_path):
    """VoteBot prod convention: ``venv/bin/python`` (no leading dot).

    Memory project_deployment.md documents this asymmetry — VoteBot's
    prod venv lives at ``~/votebot/venv/`` while ddp-sync uses ``.venv/``.
    A validator that only checked ``.venv`` would silently skip the cron
    job at registration time. Plan §3.9 must accept both conventions.
    """
    venv_bin = tmp_path / "venv" / "bin"  # NOTE: no leading dot
    venv_bin.mkdir(parents=True)
    py = venv_bin / "python"
    py.write_text("")
    py.chmod(0o755)
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "evaluate_production.py").write_text("")
    ok, err, venv_python = validate_votebot_path(str(tmp_path))
    assert ok is True
    assert err is None
    assert venv_python == tmp_path / "venv" / "bin" / "python"


def test_validate_prefers_dot_venv_when_both_exist(tmp_path):
    """If both ``.venv/`` and ``venv/`` exist (e.g. dev workstation that
    accumulated both), prefer ``.venv/`` for stability."""
    for venv_dir in (".venv", "venv"):
        venv_bin = tmp_path / venv_dir / "bin"
        venv_bin.mkdir(parents=True)
        py = venv_bin / "python"
        py.write_text("")
        py.chmod(0o755)
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "evaluate_production.py").write_text("")
    ok, err, venv_python = validate_votebot_path(str(tmp_path))
    assert ok is True
    assert venv_python == tmp_path / ".venv" / "bin" / "python"
