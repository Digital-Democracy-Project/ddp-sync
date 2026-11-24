"""SYNC-188: ddp-sync's image takes openstates-scrapers at a named commit, and refuses a pair whose scrapers need a newer core.

The build used `git clone --branch "$OPENSTATES_SCRAPERS_REF"` (default main), so the scrapers in the image were whatever
main held at build time. On 2026-11-23 that put OPEN-803's scrapers (imports `openstates.utils.tls`, core 58205e01 or
later) next to the pinned core 9157b6b5, and every cloud load failed with ModuleNotFoundError. The build now fetches
DDP_SCRAPERS_SHA (no default, a full commit, as SYNC-180 does for core), records it in
/opt/openstates-scrapers/DDP_SCRAPERS_COMMIT, and runs ddp-open-states' check-scraper-imports.sh against the installed
pair. These guards read the real files; the builds themselves (that pair fails, 0f3c24c9 with the same scrapers passes,
and the bad pair builds when the check is removed) were run by hand, as SYNC-180's were.
"""

from __future__ import annotations

import pathlib
import re
import subprocess

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
DOCKERFILE = (ROOT / "infrastructure" / "Dockerfile").read_text()
COMPOSE = (ROOT / "infrastructure" / "docker-compose.prod.yml").read_text()
RECORD = "/opt/openstates-scrapers/DDP_SCRAPERS_COMMIT"
RUN_HEAD = "RUN --mount=type=secret,id=github_token \\\n"


def _fetch_step() -> str:
    """The builder-stage RUN that fetches openstates-core and openstates-scrapers."""
    start = DOCKERFILE.index("ARG DDP_CORE_SHA")
    return DOCKERFILE[start:DOCKERFILE.index("\n\n", start)]


def test_scrapers_are_fetched_at_the_given_commit_with_no_default():
    step = _fetch_step()
    assert re.search(r"^ARG DDP_SCRAPERS_SHA$", step, re.MULTILINE), "DDP_SCRAPERS_SHA must have no default"
    assert "OPENSTATES_SCRAPERS_REF" not in DOCKERFILE, "scrapers must not be taken from a branch at build time"
    assert "git clone" not in step and "--branch" not in step
    scrapers = step[step.index("git init -q /opt/openstates-scrapers"):]
    assert "openstates-scrapers.git" in scrapers and '"$DDP_SCRAPERS_SHA"' in scrapers
    assert "git -C /opt/openstates-scrapers checkout -q FETCH_HEAD" in scrapers


def test_the_record_is_written_and_checked_before_git_is_removed():
    step = _fetch_step()
    write = step.index(f"git -C /opt/openstates-scrapers rev-parse HEAD > {RECORD}")
    check = step.index(f'[ "$(cat {RECORD})" = "$DDP_SCRAPERS_SHA" ]')
    assert write < check < step.index("rm -rf /opt/openstates-core/.git /opt/openstates-scrapers/.git")
    # the final stage copies the whole scrapers directory, so the record reaches the running container
    assert "COPY --from=openstates-builder /opt/openstates-scrapers /opt/openstates-scrapers" in DOCKERFILE


def test_the_import_check_runs_on_the_installed_pair_before_the_final_stage():
    builder = DOCKERFILE[:DOCKERFILE.index("FROM python:3.11-slim-bookworm AS app")]
    install = builder.index("pip install --no-cache-dir --no-deps -e /opt/openstates-core")
    copy = re.search(r"^COPY --from=openstates check-scraper-imports\.sh /build/check-scraper-imports\.sh$", builder,
                     re.MULTILINE)
    run = re.search(r"^RUN IMPORT_CHECK_VENV=/opt/venv-openstates IMPORT_CHECK_SCRAPERS_DIR=/opt/openstates-scrapers \\$",
                    builder, re.MULTILINE)
    assert copy and run, "the check must be a live instruction, not a comment"
    copy, run = copy.start(), run.start()
    assert install < copy < run
    # a plain RUN: no `|| true`, no `; exit 0`, so the check's exit 1 fails the build
    check = builder[run:]
    assert check.rstrip().endswith("bash /build/check-scraper-imports.sh"), check
    assert "||" not in check and "exit 0" not in check


def test_compose_passes_the_commit_to_the_build():
    build = COMPOSE[COMPOSE.index("  ddp-sync:"):COMPOSE.index("    image: ddp-sync:prod")]
    assert re.search(r"^\s+DDP_SCRAPERS_SHA: \$\{DDP_SCRAPERS_SHA:-\}$", build, re.MULTILINE), build
    assert "additional_contexts:" in build and "openstates=/opt/ddp-open-states" in build  # where the check comes from


def _guards() -> str:
    """Both shell tests at the head of the fetch step (core, then scrapers), as they run in the build."""
    step = _fetch_step()
    body = step[step.index(RUN_HEAD) + len(RUN_HEAD):]
    return body[:body.index(" && \\\n    TOKEN=")]


CORE_OK = "5c1455e1679bbdac8f00fe050d57c0152e17c648"


def _run_guards(scrapers_sha: str) -> subprocess.CompletedProcess:
    return subprocess.run(["sh", "-c", _guards()], capture_output=True, text=True, check=False,
                          env={"DDP_CORE_SHA": CORE_OK, "DDP_SCRAPERS_SHA": scrapers_sha, "PATH": "/usr/bin:/bin"})


@pytest.mark.parametrize("value", ["", "main", "a121b5dd", "A121B5DD62DED5975CA16F4B588691327D0DC074",
                                   "a121b5dd62ded5975ca16f4b588691327d0dc074 ", "g" * 40])
def test_the_build_refuses_anything_but_a_full_scrapers_commit(value):
    res = _run_guards(value)
    assert res.returncode == 1 and "DDP_SCRAPERS_SHA must be" in res.stderr, (value, res.stderr)


def test_the_build_accepts_full_commits_for_both():
    res = _run_guards("a121b5dd62ded5975ca16f4b588691327d0dc074")
    assert res.returncode == 0, res.stderr
