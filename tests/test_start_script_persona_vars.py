"""SYNC-99: scripts/start-ddp-sync.sh copies CODEBOT_SLACK_USERNAME / CODEBOT_SLACK_ICON_EMOJI from ddp-agents/.env.

The block between the `persona vars` markers is run in a real bash with the same options as the script
(`set -euo pipefail`) against a throwaway .env, because the icon emoji is configured only there and the alerts
would otherwise show CodeBot with the generic default icon."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).parent.parent / "scripts" / "start-ddp-sync.sh"
BASHES = [b for b in ("/bin/bash", shutil.which("bash")) if b]  # macOS ships bash 3.2; the script must work there


def _block() -> str:
    text = SCRIPT.read_text()
    start = text.index("# --- persona vars")
    return text[start:text.index("# --- end persona vars")]


def _run(bash: str, tmp_path: Path, env_file: str | None, preset: dict[str, str] | None = None) -> dict[str, str]:
    agents_env = tmp_path / "agents.env"
    if env_file is not None:
        agents_env.write_text(env_file)
    script = f"set -euo pipefail\n{_block()}\necho \"U=${{CODEBOT_SLACK_USERNAME-<unset>}}\"\necho \"I=${{CODEBOT_SLACK_ICON_EMOJI-<unset>}}\"\n"
    env = {"PATH": "/usr/bin:/bin", "AGENTS_ENV": str(agents_env), **(preset or {})}
    out = subprocess.run([bash, "-c", script], env=env, capture_output=True, text=True, check=True).stdout
    return dict(line.split("=", 1) for line in out.splitlines())


@pytest.mark.parametrize("bash", sorted(set(BASHES)))
def test_the_persona_vars_are_copied_from_the_agents_env_quotes_and_comments_stripped(bash, tmp_path):
    got = _run(bash, tmp_path, 'OTHER=x\nCODEBOT_SLACK_ICON_EMOJI=":codebot:"  # the bot\nCODEBOT_SLACK_USERNAME=\'Failure Bot\'\n')
    assert got["I"] == ":codebot:"
    assert got["U"] == "Failure Bot"  # a multi-word value survives
    got = _run(bash, tmp_path, "CODEBOT_SLACK_ICON_EMOJI=:codebot:\n")
    assert got == {"U": "<unset>", "I": ":codebot:"}  # the username is left to its default in code


@pytest.mark.parametrize("bash", sorted(set(BASHES)))
def test_a_value_already_in_the_environment_wins(bash, tmp_path):
    got = _run(bash, tmp_path, "CODEBOT_SLACK_ICON_EMOJI=:from-agents:\n", preset={"CODEBOT_SLACK_ICON_EMOJI": ":mine:"})
    assert got["I"] == ":mine:"


@pytest.mark.parametrize("bash", sorted(set(BASHES)))
def test_a_missing_file_or_an_empty_value_exports_nothing_and_never_fails_the_script(bash, tmp_path):
    assert _run(bash, tmp_path, None) == {"U": "<unset>", "I": "<unset>"}  # no such file under set -e
    assert _run(bash, tmp_path, "CODEBOT_SLACK_ICON_EMOJI=\nSOMETHING_ELSE=1\n") == {"U": "<unset>", "I": "<unset>"}


@pytest.mark.parametrize("bash", sorted(set(BASHES)))
def test_the_whole_script_parses_and_the_block_sits_between_the_env_load_and_the_exec(bash):
    """The block runs only if it comes after the service's own .env is sourced (so a value there wins) and
    before the `exec` that launches uvicorn (so the process inherits it)."""
    subprocess.run([bash, "-n", str(SCRIPT)], check=True)
    text = SCRIPT.read_text()
    assert text.index("source \"$PROJECT_DIR/.env\"") < text.index("# --- persona vars") < text.index("# --- end persona vars") < text.index("exec ")
