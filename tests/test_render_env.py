"""SYNC-131: infrastructure/render-env.sh writes the CodeBot persona variables on EC2.

EC2's ddp-sync gets its environment only from the .env this script renders from Secrets Manager (the Mac's
scripts/start-ddp-sync.sh copy from ddp-agents/.env does not run there), so a secret key missing from its
KEY_MAP never reaches the container. The script's embedded Python is run as is, against a fake secret, with
the AWS calls left out.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

SCRIPT = Path(__file__).parent.parent / "infrastructure" / "render-env.sh"
RDS_JSON = '{"username": "u", "password": "p"}'


def _render(tmp_path: Path, secret_json: str) -> dict[str, str]:
    text = SCRIPT.read_text()
    start = text.index("<<'PYEOF'\n") + len("<<'PYEOF'\n")
    program = text[start:text.index("\nPYEOF\n")]
    out = tmp_path / ".env"
    env = {"PATH": os.environ["PATH"], "DDP_SYNC_JSON": secret_json, "RDS_JSON": RDS_JSON}
    subprocess.run(
        [sys.executable, "-I", "-", str(out), "host", "5432", "openstates", "arn:fake"],
        input=program, env=env, text=True, capture_output=True, check=True,
    )
    lines = [line for line in out.read_text().splitlines() if line and not line.startswith("#")]
    return dict(line.split("=", 1) for line in lines)


def test_the_codebot_persona_is_written_when_the_secret_has_it(tmp_path):
    got = _render(tmp_path, '{"slack_bot_token": "xoxb-t", "codebot_slack_username": "CodeBot", '
                            '"codebot_slack_icon_emoji": ":codebot:"}')
    assert got["CODEBOT_SLACK_ICON_EMOJI"] == ":codebot:"
    assert got["CODEBOT_SLACK_USERNAME"] == "CodeBot"
    assert got["SLACK_BOT_TOKEN"] == "xoxb-t"


def test_absent_or_empty_keys_are_left_out_so_the_code_defaults_apply(tmp_path):
    got = _render(tmp_path, '{"slack_bot_token": "xoxb-t", "codebot_slack_icon_emoji": ""}')
    assert "CODEBOT_SLACK_ICON_EMOJI" not in got and "CODEBOT_SLACK_USERNAME" not in got


def test_the_cams_token_is_written_as_the_alert_only_variable(tmp_path):
    """SYNC-132: the token for the operator-alert route reaches EC2's container as OPERATOR_ALERT_TOKEN."""
    got = _render(tmp_path, '{"slack_bot_token": "xoxb-t", "operator_alert_token": "tok"}')
    assert got["OPERATOR_ALERT_TOKEN"] == "tok"
    assert "CAMS_API_TOKEN" not in got


def test_a_cams_api_token_secret_key_is_never_rendered_into_the_env(tmp_path):
    """`cams_api_token` set is the code's "this host is the Mac" signal (openstates_archive._mac_capable(), replica
    routing), so it must not reach EC2's environment even if the secret carries it. (The app also reads the secret
    directly through get_settings(); that is why the token's secret key is `operator_alert_token`, see render-env.sh.)"""
    got = _render(tmp_path, '{"slack_bot_token": "xoxb-t", "cams_api_token": "tok"}')
    assert "CAMS_API_TOKEN" not in got and "OPERATOR_ALERT_TOKEN" not in got
