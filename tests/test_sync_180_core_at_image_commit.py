"""SYNC-180: ddp-sync's cloud loader imports with the scrapers image's openstates-core, not "main at build time".

cloud_loader.py's import (`os-update`) runs inside this image, on the copy of openstates-core its build fetched. Built
at another moment than the scrapers image, it imported with an older core (found 2026-11-18: v62's 44c3b8dc while v65
was live, so newer state rules and the Oregon joint-committee lookup did not apply on load). The build now takes the
image's commit (DDP_CORE_SHA, no default, as api-v3's deploy/Dockerfile.ddp does) and records it where ddp-open-states'
check-core-pin-drift.py --sync-container reads it. These guards read the real files; the build itself was run by hand.
"""

from __future__ import annotations

import pathlib
import re
import subprocess

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
DOCKERFILE = (ROOT / "infrastructure" / "Dockerfile").read_text()
COMPOSE = (ROOT / "infrastructure" / "docker-compose.prod.yml").read_text()
RECORD = "/opt/openstates-core/DDP_CORE_COMMIT"  # the same path the scrapers image's build writes (OPEN-579)


def _core_step() -> str:
    """The builder-stage RUN that fetches openstates-core."""
    start = DOCKERFILE.index("ARG DDP_CORE_SHA")
    return DOCKERFILE[start:DOCKERFILE.index("\n\n", start)]


def test_core_is_fetched_at_the_given_commit_with_no_default():
    step = _core_step()
    assert re.search(r"^ARG DDP_CORE_SHA$", step, re.MULTILINE), "DDP_CORE_SHA must have no default"
    assert '"$DDP_CORE_SHA"' in step and "checkout -q FETCH_HEAD" in step
    assert "OPENSTATES_CORE_REF" not in DOCKERFILE, "core must not be taken from a branch at build time"
    core_fetch = step[:step.index('git clone --branch "$OPENSTATES_SCRAPERS_REF"')]
    assert "openstates-core.git" in core_fetch and "--branch" not in core_fetch


def test_the_record_is_written_and_checked_before_git_is_removed():
    step = _core_step()
    write = step.index(f"git -C /opt/openstates-core rev-parse HEAD > {RECORD}")
    check = step.index(f'[ "$(cat {RECORD})" = "$DDP_CORE_SHA" ]')
    assert write < check < step.index("rm -rf /opt/openstates-core/.git")
    # the final stage copies the whole core directory, so the record reaches the running container
    assert "COPY --from=openstates-builder /opt/openstates-core /opt/openstates-core" in DOCKERFILE


def test_compose_passes_the_commit_to_the_build():
    build = COMPOSE[COMPOSE.index("  ddp-sync:"):COMPOSE.index("    image: ddp-sync:prod")]
    assert re.search(r"^\s+DDP_CORE_SHA: \$\{DDP_CORE_SHA:-\}$", build, re.MULTILINE), build


def _guard() -> str:
    """The shell test at the head of the core step, as it runs in the build."""
    step = _core_step()
    body = step[step.index("RUN --mount=type=secret,id=github_token \\\n") + len("RUN --mount=type=secret,id=github_token \\\n"):]
    guard = body[:body.index(" && \\\n    TOKEN=")]
    return guard  # backslash-newlines kept: sh joins them as the build does


@pytest.mark.parametrize("value", ["", "main", "44c3b8dc", "5C1455E1679BBDAC8F00FE050D57C0152E17C648",
                                   "5c1455e1679bbdac8f00fe050d57c0152e17c648 ", "g" * 40])
def test_the_build_refuses_anything_but_a_full_commit(value):
    res = subprocess.run(["sh", "-c", _guard()], env={"DDP_CORE_SHA": value, "PATH": "/usr/bin:/bin"},
                         capture_output=True, text=True, check=False)
    assert res.returncode == 1 and "DDP_CORE_SHA must be" in res.stderr, (value, res.stderr)


def test_the_build_accepts_a_full_commit():
    res = subprocess.run(["sh", "-c", _guard()], env={"DDP_CORE_SHA": "5c1455e1679bbdac8f00fe050d57c0152e17c648",
                                                       "PATH": "/usr/bin:/bin"}, capture_output=True, text=True, check=False)
    assert res.returncode == 0, res.stderr
